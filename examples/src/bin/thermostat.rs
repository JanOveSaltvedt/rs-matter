/*
 *
 *    Copyright (c) 2026 Project CHIP Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

//! An example Matter device implementing the heating-only Thermostat cluster
//! over Ethernet.
//!
//! Endpoint 1 is a Thermostat (`0x0301`) with the `HEAT` feature, backed by a
//! simulated room: the local temperature drifts towards the heating setpoint
//! while `SystemMode` is `Heat`, and back towards ambient otherwise.
//!
//! Drive it with `chip-tool` once commissioned, e.g.
//!
//! ```sh
//! chip-tool thermostat read occupied-heating-setpoint <node-id> 1
//! chip-tool thermostat write system-mode 4 <node-id> 1
//! chip-tool thermostat setpoint-raise-lower 0 10 <node-id> 1
//! chip-tool thermostat subscribe local-temperature 1 10 <node-id> 1
//! ```
#![allow(clippy::uninlined_format_args)]

use core::cell::Cell;
use core::pin::pin;

use std::fs;
use std::io::{Read, Write};
use std::net::UdpSocket;
use std::path::PathBuf;

use embassy_futures::select::select3;

use async_signal::{Signal, Signals};
use log::{error, info, trace};

use futures_lite::StreamExt;

use rand::Rng;
use rs_matter::crypto::{default_crypto, Crypto};
use rs_matter::dm::clusters::app::thermostat::{
    self, ControlSequenceOfOperationEnum, OutOfBandMessage, SystemModeEnum, ThermostatHooks,
};
use rs_matter::dm::clusters::decl::thermostat as thermostat_cluster;
use rs_matter::dm::clusters::desc::{self, ClusterHandler as _};
use rs_matter::dm::clusters::groups::{self, ClusterHandler as _};
use rs_matter::dm::clusters::identify::{self, IdentifyHandler};
use rs_matter::dm::devices::test::{DAC_PRIVKEY, TEST_DEV_ATT, TEST_DEV_COMM, TEST_DEV_DET};
use rs_matter::dm::devices::DEV_TYPE_THERMOSTAT;
use rs_matter::dm::endpoints;
use rs_matter::dm::networks::eth::EthNetwork;
use rs_matter::dm::networks::SysNetifs;
use rs_matter::dm::{Async, Cluster, DataModel, Dataver, Endpoint, Node};
use rs_matter::error::Error;
use rs_matter::im::{EthInteractionModelState, InteractionModel};
use rs_matter::pairing::qr::QrTextType;
use rs_matter::pairing::DiscoveryCapabilities;
use rs_matter::respond::DefaultResponder;
use rs_matter::sc::pase::MAX_COMM_WINDOW_TIMEOUT_SECS;
use rs_matter::tlv::Nullable;
use rs_matter::transport::exchange::MatterBuffers;
use rs_matter::transport::MATTER_SOCKET_BIND_ADDR;
use rs_matter::utils::select::Coalesce;
use rs_matter::{clusters, devices, root_endpoint, with, Matter, MATTER_PORT};

#[path = "../common/mdns.rs"]
mod mdns;

/// The endpoint hosting the thermostat.
const THERMOSTAT_ENDPOINT: u16 = 1;

fn main() -> Result<(), Error> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let matter = Matter::new(&TEST_DEV_DET, TEST_DEV_COMM, &TEST_DEV_ATT, MATTER_PORT);

    // Persistence
    let store = rs_matter::persist::DirKvBlobStore::new_default();

    // Create the transport buffers
    let buffers: MatterBuffers = MatterBuffers::new();

    // Create the data model state (subscriptions, events, network store).
    let state: EthInteractionModelState = EthInteractionModelState::new(EthNetwork::new_default());

    // Bind the KV access object (the KV scratch buffer lives in `Matter`).
    let kv = matter.kv(store);

    // Re-hydrate the `Matter` instance (fabrics, basic info, RTC).
    matter.startup(&kv)?;

    // Create the crypto instance
    let crypto = default_crypto(rand::rng(), DAC_PRIVKEY);

    let mut rand = crypto.rand()?;

    // Thermostat cluster setup. The Thermostat cluster is not coupled to any
    // other cluster, so there is no `init()` step: validation and the repair of
    // the persisted state happen on the `Startup` lifecycle op.
    let thermostat_handler = thermostat::ThermostatHandler::new(
        Dataver::new_rand(&mut rand),
        THERMOSTAT_ENDPOINT,
        ThermostatDeviceLogic::new(),
    );

    // Create the Data Model instance
    let im = InteractionModel::new(
        &matter,
        &crypto,
        &buffers,
        data_model(rand, &thermostat_handler),
        &kv,
        &state,
    );

    // Bring the Data Model to its operational state: re-hydrate its persisted
    // state and deliver the `Startup` lifecycle op to all cluster handlers.
    futures_lite::future::block_on(im.startup())?;

    // Create a default responder capable of handling up to 3 subscriptions
    // All other subscription requests will be turned down with "resource exhausted"
    let responder = DefaultResponder::new(&im);

    // Run the responder with up to 4 handlers (i.e. 4 exchanges can be handled simultaneously)
    // Clients trying to open more exchanges than the ones currently running will get "I'm busy, please try again later"
    let mut respond = pin!(responder.run::<4, 4>());

    // Run the background job of the data model
    let mut im_job = pin!(im.run());

    let socket = async_io::Async::<UdpSocket>::bind(MATTER_SOCKET_BIND_ADDR)?;

    // Run the Matter and mDNS transports
    let mut mdns = pin!(mdns::run_mdns(&matter, &crypto));
    let mut transport = pin!(matter.run(&crypto, &socket, &socket, &socket));

    // We need to always print the QR text, because the test runner expects it to be printed
    // even if the device is already commissioned
    matter.print_standard_qr_text(DiscoveryCapabilities::IP)?;

    if !matter.has_fabrics() {
        // If the device is not commissioned yet, print the QR code to the console
        // and enable basic commissioning

        matter.print_standard_qr_code(QrTextType::Unicode, DiscoveryCapabilities::IP)?;

        matter.open_basic_comm_window(MAX_COMM_WINDOW_TIMEOUT_SECS, &crypto, &())?;
    }

    // Listen to SIGTERM (or Ctrl-C on Windows, where SIGTERM is not
    // supported by `async-signal`) because at the end of the test we'll
    // receive it.
    #[cfg(not(windows))]
    let mut term_signal = Signals::new([Signal::Term])?;
    #[cfg(windows)]
    let mut term_signal = Signals::new([Signal::Int])?;
    let mut term = pin!(async {
        term_signal.next().await;
        Ok(())
    });

    // Combine all async tasks in a single one
    let all = select3(
        &mut transport,
        &mut mdns,
        select3(&mut respond, &mut im_job, &mut term).coalesce(),
    );

    // Run with a simple `block_on`. Any local executor would do.
    futures_lite::future::block_on(all.coalesce())
}

/// The Node meta-data describing our Matter device.
const NODE: Node<'static> = Node {
    endpoints: &[
        root_endpoint!(eth),
        Endpoint::new(
            THERMOSTAT_ENDPOINT,
            devices!(DEV_TYPE_THERMOSTAT),
            clusters!(
                desc::DescHandler::CLUSTER,
                identify::CLUSTER,
                groups::GroupsHandler::CLUSTER,
                ThermostatDeviceLogic::CLUSTER,
            ),
        ),
    ],
};

/// The Data Model handler + meta-data for our Matter device.
/// The handler is the root endpoint 0 handler plus the thermostat endpoint's clusters.
fn data_model<'a, H: ThermostatHooks>(
    mut rand: impl Rng + Copy,
    thermostat: &'a thermostat::ThermostatHandler<H>,
) -> impl DataModel + 'a {
    (
        NODE,
        endpoints::EthSysHandlerBuilder::new()
            .netif_diag(&SysNetifs)
            .build(rand)
            .chain(
                |e, c| e == THERMOSTAT_ENDPOINT && c == desc::DescHandler::CLUSTER.id,
                Async(desc::DescHandler::new(Dataver::new_rand(&mut rand)).adapt()),
            )
            .chain(
                |e, c| e == THERMOSTAT_ENDPOINT && c == identify::CLUSTER.id,
                Async(IdentifyHandler::new(Dataver::new_rand(&mut rand)).adapt()),
            )
            .chain(
                |e, c| e == THERMOSTAT_ENDPOINT && c == groups::GroupsHandler::CLUSTER.id,
                Async(groups::GroupsHandler::new(Dataver::new_rand(&mut rand)).adapt()),
            )
            .chain(
                |e, c| e == THERMOSTAT_ENDPOINT && c == ThermostatDeviceLogic::CLUSTER.id,
                thermostat::HandlerAsyncAdaptor(thermostat),
            ),
    )
}

// Implementing the Thermostat business logic

/// How often the simulated room temperature is recomputed.
const TICK: embassy_time::Duration = embassy_time::Duration::from_secs(5);

/// How fast the room warms towards the setpoint while heating, in 0.01degC per
/// [`TICK`].
const HEATING_RATE: i16 = 20;

/// How fast the room cools towards [`AMBIENT`] while idle, in 0.01degC per
/// [`TICK`].
const COOLING_RATE: i16 = 10;

/// The temperature the simulated room drifts to with the heating off, in
/// 0.01degC.
const AMBIENT: i16 = 1600;

/// A simulated heating thermostat with file-backed persistence of the four
/// non-volatile attributes.
pub struct ThermostatDeviceLogic {
    local_temperature: Cell<i16>,
    occupied_heating_setpoint: Cell<i16>,
    min_heat_setpoint_limit: Cell<i16>,
    max_heat_setpoint_limit: Cell<i16>,
    system_mode: Cell<SystemModeEnum>,
    /// Whether the simulated heater is calling for heat.
    heating: Cell<bool>,
}

impl ThermostatDeviceLogic {
    pub fn new() -> Self {
        let this = Self {
            local_temperature: Cell::new(AMBIENT),
            occupied_heating_setpoint: Cell::new(2000),
            min_heat_setpoint_limit: Cell::new(Self::ABS_MIN_HEAT_SETPOINT),
            max_heat_setpoint_limit: Cell::new(Self::ABS_MAX_HEAT_SETPOINT),
            system_mode: Cell::new(SystemModeEnum::Off),
            heating: Cell::new(false),
        };

        this.load_state();

        this
    }

    /// Where the non-volatile attributes are kept between runs. A real device
    /// would put them in its own KV store; see `tests/src/bin/light_tests.rs`
    /// for the `HandlerContext::kv` route.
    fn state_path() -> PathBuf {
        std::env::temp_dir().join("rs-matter-example-thermostat-state")
    }

    fn load_state(&self) {
        let mut buf = [0u8; 9];

        let Ok(mut file) = fs::File::open(Self::state_path()) else {
            return;
        };

        if file.read_exact(&mut buf).is_err() {
            trace!("Thermostat: no usable persisted state");
            return;
        }

        // Only the two modes a heating-only thermostat can be in; anything
        // else would be rejected by the handler's startup repair anyway.
        let system_mode = match buf[0] {
            m if m == SystemModeEnum::Off as u8 => SystemModeEnum::Off,
            m if m == SystemModeEnum::Heat as u8 => SystemModeEnum::Heat,
            _ => {
                trace!("Thermostat: persisted SystemMode is not a supported value");
                return;
            }
        };

        self.system_mode.set(system_mode);
        self.occupied_heating_setpoint
            .set(i16::from_le_bytes([buf[1], buf[2]]));
        self.min_heat_setpoint_limit
            .set(i16::from_le_bytes([buf[3], buf[4]]));
        self.max_heat_setpoint_limit
            .set(i16::from_le_bytes([buf[5], buf[6]]));
        self.local_temperature
            .set(i16::from_le_bytes([buf[7], buf[8]]));
    }

    fn save_state(&self) {
        let mut buf = [0u8; 9];

        buf[0] = self.system_mode.get() as u8;
        buf[1..3].copy_from_slice(&self.occupied_heating_setpoint.get().to_le_bytes());
        buf[3..5].copy_from_slice(&self.min_heat_setpoint_limit.get().to_le_bytes());
        buf[5..7].copy_from_slice(&self.max_heat_setpoint_limit.get().to_le_bytes());
        buf[7..9].copy_from_slice(&self.local_temperature.get().to_le_bytes());

        let saved = fs::File::create(Self::state_path())
            .and_then(|mut file| file.write_all(&buf))
            .is_ok();

        if !saved {
            error!("Thermostat: could not persist the cluster state");
        }
    }

    /// Advance the room simulation by one [`TICK`], returning `true` if the
    /// local temperature changed.
    fn tick(&self) -> bool {
        let previous = self.local_temperature.get();

        let temperature = if self.heating.get() {
            previous.saturating_add(HEATING_RATE)
        } else {
            previous.saturating_sub(COOLING_RATE).max(AMBIENT)
        };

        self.local_temperature.set(temperature);
        self.update_relay();

        temperature != previous
    }

    /// Re-evaluate the heat demand, with a one-notch hysteresis band around the
    /// setpoint so the simulated relay does not chatter every tick.
    fn update_relay(&self) {
        let setpoint = self.occupied_heating_setpoint.get();
        let temperature = self.local_temperature.get();

        let heating = matches!(self.system_mode.get(), SystemModeEnum::Heat)
            && if self.heating.get() {
                temperature < setpoint.saturating_add(HEATING_RATE)
            } else {
                temperature < setpoint.saturating_sub(HEATING_RATE)
            };

        if heating != self.heating.get() {
            info!("Emulation: heating {}", if heating { "ON" } else { "OFF" });
        }

        self.heating.set(heating);
    }
}

impl Default for ThermostatDeviceLogic {
    fn default() -> Self {
        Self::new()
    }
}

impl ThermostatHooks for ThermostatDeviceLogic {
    /// A heating-only thermostat: the `HEAT` feature alone, the four mandatory
    /// attributes plus the optional heat setpoint limits, and the one mandatory
    /// command. See the `rs_matter::dm::clusters::app::thermostat` module docs
    /// for why the limits come as a set of four.
    const CLUSTER: Cluster<'static> = thermostat_cluster::FULL_CLUSTER
        .with_revision(11)
        .with_features(thermostat_cluster::Feature::HEATING.bits())
        .with_attrs(with!(
            required;
            thermostat_cluster::AttributeId::AbsMinHeatSetpointLimit
                | thermostat_cluster::AttributeId::AbsMaxHeatSetpointLimit
                | thermostat_cluster::AttributeId::OccupiedHeatingSetpoint
                | thermostat_cluster::AttributeId::MinHeatSetpointLimit
                | thermostat_cluster::AttributeId::MaxHeatSetpointLimit
        ))
        .with_cmds(with!(thermostat_cluster::CommandId::SetpointRaiseLower))
        .with_events(with!());

    const CONTROL_SEQUENCE_OF_OPERATION: ControlSequenceOfOperationEnum =
        ControlSequenceOfOperationEnum::HeatingOnly;

    fn local_temperature(&self) -> Nullable<i16> {
        Nullable::some(self.local_temperature.get())
    }

    fn occupied_heating_setpoint(&self) -> i16 {
        self.occupied_heating_setpoint.get()
    }

    fn set_occupied_heating_setpoint(&self, value: i16) -> Result<(), Error> {
        self.occupied_heating_setpoint.set(value);
        self.save_state();

        Ok(())
    }

    fn min_heat_setpoint_limit(&self) -> i16 {
        self.min_heat_setpoint_limit.get()
    }

    fn set_min_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
        self.min_heat_setpoint_limit.set(value);
        self.save_state();

        Ok(())
    }

    fn max_heat_setpoint_limit(&self) -> i16 {
        self.max_heat_setpoint_limit.get()
    }

    fn set_max_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
        self.max_heat_setpoint_limit.set(value);
        self.save_state();

        Ok(())
    }

    fn system_mode(&self) -> SystemModeEnum {
        self.system_mode.get()
    }

    fn set_system_mode(&self, value: SystemModeEnum) -> Result<(), Error> {
        self.system_mode.set(value);
        self.save_state();

        Ok(())
    }

    fn apply(&self, system_mode: SystemModeEnum, heating_setpoint: i16) {
        info!(
            "Emulation: system mode {:?}, heating setpoint {}.{:02}C, room {}.{:02}C",
            system_mode,
            heating_setpoint / 100,
            (heating_setpoint % 100).abs(),
            self.local_temperature.get() / 100,
            (self.local_temperature.get() % 100).abs(),
        );

        // Re-evaluate the relay immediately rather than waiting a tick, so that
        // switching to `Heat` has a visible effect right away.
        self.update_relay();
    }

    async fn run<F: Fn(OutOfBandMessage)>(&self, notify: F) {
        loop {
            // In a real device we would wait on a temperature sensor.
            embassy_time::Timer::after(TICK).await;

            if self.tick() {
                self.save_state();
                notify(OutOfBandMessage::LocalTemperature);
            }
        }
    }
}
