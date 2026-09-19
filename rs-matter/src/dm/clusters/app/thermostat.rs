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

//! Implementation of the Matter Thermostat cluster (`0x0201`), Matter 1.6
//! Application Cluster spec section 4.3, `ClusterRevision` 11.
//!
//! This is a **heating-only** implementation: the FeatureMap it accepts is
//! `HEAT` (optionally plus `LTNE`), which is the smallest conformant slice of
//! the cluster that still yields a usable device. See [`ThermostatHooks`] for
//! the device-specific logic the consumer supplies.
//!
//! Key features:
//! - Provides hooks for device-specific logic and persistence via the
//!   [`ThermostatHooks`] trait.
//! - Validates the cluster configuration and feature selection at startup.
//! - Enforces the setpoint-limit constraint chain of spec section 4.3.6 across
//!   every mutation path, including the asymmetry the spec requires: the
//!   `SetpointRaiseLower` command clamps silently, while an attribute write
//!   out of range is a `CONSTRAINT_ERROR`.
//!
//! Attributes served (section 4.3.11):
//!
//! | ID       | Name                         | Conformance    |
//! | -------- | ---------------------------- | -------------- |
//! | `0x0000` | `LocalTemperature`           | M              |
//! | `0x0003` | `AbsMinHeatSetpointLimit`    | `[HEAT]`       |
//! | `0x0004` | `AbsMaxHeatSetpointLimit`    | `[HEAT]`       |
//! | `0x0012` | `OccupiedHeatingSetpoint`    | `HEAT`         |
//! | `0x0015` | `MinHeatSetpointLimit`       | `[HEAT]`       |
//! | `0x0016` | `MaxHeatSetpointLimit`       | `[HEAT]`       |
//! | `0x001B` | `ControlSequenceOfOperation` | M              |
//! | `0x001C` | `SystemMode`                 | M              |
//!
//! The four `*HeatSetpointLimit` attributes are optional, but the
//! `CONSTRAINT_ERROR` and clamping rules are written in terms of them, so they
//! are either all served or all omitted — see [`ThermostatHandler::validate`].
//!
//! The only command served is `SetpointRaiseLower` (`0x00`), the sole
//! unconditionally mandatory one (section 4.3.12.1).
//!
//! Unsupported features, all of them optional in the spec:
//! - `COOL` (cooling) and `AUTO` (auto mode). Without `AUTO` there is no
//!   `MinSetpointDeadBand` and none of the deadband clauses of section 4.3.6
//!   apply — they hold "if, and only if, the AUTO feature is supported".
//! - `OCC` (occupancy) and therefore the unoccupied setpoints.
//! - `MSCH` (schedules), `PRES` (presets) and `TSUGGEST` (thermostat
//!   suggestions), along with their commands and the global
//!   `AtomicRequest`/`AtomicResponse` pair — atomic writes are only needed for
//!   the `Presets`/`Schedules` attributes. These features additionally require
//!   the device to support time synchronization.
//! - `TEVT` (events), which is provisional in 1.6.
//! - `SB` (setback), deprecated in cluster revision 10.
//!
//! The legacy weekly-schedule feature was removed from the Matter 1.6 data
//! model altogether; its commands (`0x01`..=`0x03`) survive in the IDL we
//! generate from, but they are not served.

use core::future::{ready, Future};
use core::pin::pin;

use embassy_futures::select::{select, Either};

use crate::dm::types::EndptId;
use crate::dm::{
    AttrChangeNotifier, AttrId, Cluster, Dataver, HandlerContext, InvokeContext, LifecycleOp,
    ReadContext, WriteContext,
};
use crate::error::{Error, ErrorCode};
use crate::tlv::{Nullable, TLVBuilderParent};
use crate::utils::sync::Signal;

pub use crate::dm::clusters::decl::thermostat::*;

/// The `ClusterRevision` this handler implements (Matter 1.6).
const CLUSTER_REVISION: u16 = 11;

/// The features this handler knows how to serve. Anything else in a
/// [`ThermostatHooks::CLUSTER`] FeatureMap is rejected by
/// [`ThermostatHandler::validate`].
const SUPPORTED_FEATURES: u32 =
    Feature::HEATING.bits() | Feature::LOCAL_TEMPERATURE_NOT_EXPOSED.bits();

/// Messages passed to the `notify` closure of [`ThermostatHooks::run`].
///
/// They tell the handler that the device changed an attribute behind the
/// cluster's back — a new sensor reading, a turn of a knob on the device's own
/// front panel — so that the handler can re-report it to any subscriber.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum OutOfBandMessage {
    /// [`ThermostatHooks::local_temperature`] changed.
    LocalTemperature,
    /// [`ThermostatHooks::occupied_heating_setpoint`] changed.
    OccupiedHeatingSetpoint,
    /// [`ThermostatHooks::system_mode`] changed.
    SystemMode,
    /// [`ThermostatHooks::min_heat_setpoint_limit`] or
    /// [`ThermostatHooks::max_heat_setpoint_limit`] changed.
    SetpointLimits,
    /// Any or all of the above changed.
    Update,
}

impl OutOfBandMessage {
    /// The set of attributes this message marks as needing a re-report.
    const fn pending(&self) -> u8 {
        match self {
            Self::LocalTemperature => PENDING_LOCAL_TEMPERATURE,
            Self::OccupiedHeatingSetpoint => PENDING_OCCUPIED_HEATING_SETPOINT,
            Self::SystemMode => PENDING_SYSTEM_MODE,
            Self::SetpointLimits => PENDING_MIN_LIMIT | PENDING_MAX_LIMIT,
            Self::Update => PENDING_ALL,
        }
    }
}

const PENDING_LOCAL_TEMPERATURE: u8 = 1 << 0;
const PENDING_OCCUPIED_HEATING_SETPOINT: u8 = 1 << 1;
const PENDING_SYSTEM_MODE: u8 = 1 << 2;
const PENDING_MIN_LIMIT: u8 = 1 << 3;
const PENDING_MAX_LIMIT: u8 = 1 << 4;
const PENDING_ALL: u8 = PENDING_LOCAL_TEMPERATURE
    | PENDING_OCCUPIED_HEATING_SETPOINT
    | PENDING_SYSTEM_MODE
    | PENDING_MIN_LIMIT
    | PENDING_MAX_LIMIT;

/// The pending-notification bit to attribute ID mapping, in ascending
/// attribute order.
const PENDING_ATTRS: &[(u8, AttributeId)] = &[
    (PENDING_LOCAL_TEMPERATURE, AttributeId::LocalTemperature),
    (
        PENDING_OCCUPIED_HEATING_SETPOINT,
        AttributeId::OccupiedHeatingSetpoint,
    ),
    (PENDING_MIN_LIMIT, AttributeId::MinHeatSetpointLimit),
    (PENDING_MAX_LIMIT, AttributeId::MaxHeatSetpointLimit),
    (PENDING_SYSTEM_MODE, AttributeId::SystemMode),
];

/// A heating-only Thermostat cluster handler.
///
/// The Thermostat cluster is not coupled to any other cluster, so the handler
/// needs no wiring step: construct it with [`ThermostatHandler::new`] and chain
/// it. Configuration validation and the repair of persisted state both happen
/// on the `Startup` lifecycle operation.
pub struct ThermostatHandler<H: ThermostatHooks> {
    dataver: Dataver,
    /// Needed to address `notify_attr_changed` from [`Self::run`], which has
    /// only a [`HandlerContext`] and hence no notion of a "current" endpoint.
    endpoint_id: EndptId,
    hooks: H,
    /// Bitmask of attributes awaiting a subscription re-report, fed by
    /// [`Self::out_of_band_message`] and drained by [`Self::run`].
    ///
    /// A `Signal<Option<OutOfBandMessage>>` would be the more obvious choice,
    /// but it is a single slot that *replaces* on signal: two out-of-band
    /// changes landing back to back would lose the first. Accumulating into a
    /// mask instead makes the path lossless.
    pending: Signal<u8>,
}

impl<H: ThermostatHooks> ThermostatHandler<H> {
    /// Create a new `ThermostatHandler` with the given hooks.
    ///
    /// # Arguments
    /// - `dataver` - the cluster data version.
    /// - `endpoint_id` - the endpoint hosting this cluster instance.
    /// - `hooks` - the device-specific thermostat logic and persistence.
    pub const fn new(dataver: Dataver, endpoint_id: EndptId, hooks: H) -> Self {
        Self {
            dataver,
            endpoint_id,
            hooks,
            pending: Signal::new(0),
        }
    }

    /// Adapt the handler instance to the generic `rs-matter` `Handler` trait.
    pub const fn adapt(self) -> HandlerAsyncAdaptor<Self> {
        HandlerAsyncAdaptor(self)
    }

    /// Whether the configured FeatureMap contains all of `features`.
    fn supports_feature(features: u32) -> bool {
        H::CLUSTER.feature_map & features != 0
    }

    /// Whether the user-configurable setpoint limits are served. They are
    /// either all present or all absent - [`Self::validate`] enforces that.
    fn has_limits() -> bool {
        H::CLUSTER
            .attribute(AttributeId::MinHeatSetpointLimit as _)
            .is_some()
    }

    /// The effective lower bound on the heating setpoint: the user-configurable
    /// limit when served, else the manufacturer's absolute limit.
    fn min_setpoint(&self) -> i16 {
        if Self::has_limits() {
            self.hooks.min_heat_setpoint_limit()
        } else {
            H::ABS_MIN_HEAT_SETPOINT
        }
    }

    /// The effective upper bound on the heating setpoint.
    fn max_setpoint(&self) -> i16 {
        if Self::has_limits() {
            self.hooks.max_heat_setpoint_limit()
        } else {
            H::ABS_MAX_HEAT_SETPOINT
        }
    }

    /// Clamp a candidate heating setpoint into the effective limits.
    ///
    /// Takes an `i32` because the `SetpointRaiseLower` arithmetic can overflow
    /// `i16` before it is clamped.
    fn clamp_setpoint(&self, value: i32) -> i16 {
        value.clamp(self.min_setpoint() as i32, self.max_setpoint() as i32) as i16
    }

    /// Whether `mode` is a `SystemMode` this thermostat can be put into.
    ///
    /// Section 4.3.11.22: "Its value SHALL be limited by the
    /// ControlSequenceOfOperation attribute." With `HeatingOnly` /
    /// `HeatingWithReheat`, "Cool and precooling are not possible"; of the
    /// remaining values only `Off` and `Heat` are meaningful for a device that
    /// has neither a fan nor a dehumidifier nor second-stage emergency heat.
    fn is_supported_system_mode(mode: SystemModeEnum) -> bool {
        matches!(mode, SystemModeEnum::Off | SystemModeEnum::Heat)
    }

    /// Push the current control state onto the device.
    fn apply(&self) {
        self.hooks.apply(
            self.hooks.system_mode(),
            self.hooks.occupied_heating_setpoint(),
        );
    }

    /// Mark a set of attributes as needing a re-report and wake [`Self::run`].
    ///
    /// Public so that a consumer holding the handler can poke it directly,
    /// besides the `notify` closure handed to [`ThermostatHooks::run`].
    pub fn out_of_band_message(&self, message: OutOfBandMessage) {
        let bits = message.pending();

        self.pending.modify(|pending| {
            *pending |= bits;
            (true, ())
        });
    }

    /// Wait until at least one attribute is pending, then take the whole mask.
    async fn wait_pending(&self) -> u8 {
        self.pending
            .wait(|pending| (*pending != 0).then(|| core::mem::take(pending)))
            .await
    }

    /// Emit one `notify_attr_changed` per pending attribute.
    fn notify_pending(&self, ctx: impl HandlerContext, pending: u8) {
        for (bit, attr) in PENDING_ATTRS {
            if pending & bit != 0 {
                ctx.notify_attr_changed(self.endpoint_id, Self::CLUSTER.id, *attr as AttrId);
            }
        }
    }

    /// Re-report a single attribute of this cluster instance.
    fn notify(&self, notifier: &impl AttrChangeNotifier, attr: AttributeId) {
        notifier.notify_attr_changed(self.endpoint_id, Self::CLUSTER.id, attr as AttrId);
    }

    /// `OccupiedHeatingSetpoint` write, section 4.3.11.12: "If an attempt is
    /// made to set this attribute to a value greater than MaxHeatSetpointLimit
    /// or less than MinHeatSetpointLimit, a response with the status code
    /// CONSTRAINT_ERROR SHALL be returned."
    ///
    /// Note the contrast with `SetpointRaiseLower`, which clamps instead - see
    /// [`Self::raise_lower_setpoint`].
    fn write_occupied_heating_setpoint(
        &self,
        notifier: impl AttrChangeNotifier,
        value: i16,
    ) -> Result<(), Error> {
        if value < self.min_setpoint() || value > self.max_setpoint() {
            Err(ErrorCode::ConstraintError)?;
        }

        self.hooks.set_occupied_heating_setpoint(value)?;

        self.apply();
        self.notify(&notifier, AttributeId::OccupiedHeatingSetpoint);

        Ok(())
    }

    /// `MinHeatSetpointLimit` write, section 4.3.11.15: "If an attempt is made
    /// to set this attribute to a value which conflicts with setpoint values
    /// then those setpoints SHALL be adjusted by the minimum amount to permit
    /// this attribute to be set to the desired value. If an attempt is made to
    /// set this attribute to a value which is not consistent with the
    /// constraints and cannot be resolved by modifying setpoints then a
    /// response with the status code CONSTRAINT_ERROR SHALL be returned."
    ///
    /// Raising the floor above `MaxHeatSetpointLimit` or dropping it below
    /// `AbsMinHeatSetpointLimit` breaks the section 4.3.6 chain in a way no
    /// setpoint adjustment can fix, so those are the `CONSTRAINT_ERROR` cases.
    fn write_min_heat_setpoint_limit(
        &self,
        notifier: impl AttrChangeNotifier,
        value: i16,
    ) -> Result<(), Error> {
        if value < H::ABS_MIN_HEAT_SETPOINT || value > self.hooks.max_heat_setpoint_limit() {
            Err(ErrorCode::ConstraintError)?;
        }

        self.hooks.set_min_heat_setpoint_limit(value)?;
        self.notify(&notifier, AttributeId::MinHeatSetpointLimit);

        // Drag the setpoint up by the minimum amount, if it now sits below the
        // new floor.
        if self.hooks.occupied_heating_setpoint() < value {
            self.hooks.set_occupied_heating_setpoint(value)?;

            self.apply();
            self.notify(&notifier, AttributeId::OccupiedHeatingSetpoint);
        }

        Ok(())
    }

    /// `MaxHeatSetpointLimit` write, section 4.3.11.16 - the mirror image of
    /// [`Self::write_min_heat_setpoint_limit`].
    fn write_max_heat_setpoint_limit(
        &self,
        notifier: impl AttrChangeNotifier,
        value: i16,
    ) -> Result<(), Error> {
        if value > H::ABS_MAX_HEAT_SETPOINT || value < self.hooks.min_heat_setpoint_limit() {
            Err(ErrorCode::ConstraintError)?;
        }

        self.hooks.set_max_heat_setpoint_limit(value)?;
        self.notify(&notifier, AttributeId::MaxHeatSetpointLimit);

        if self.hooks.occupied_heating_setpoint() > value {
            self.hooks.set_occupied_heating_setpoint(value)?;

            self.apply();
            self.notify(&notifier, AttributeId::OccupiedHeatingSetpoint);
        }

        Ok(())
    }

    /// `SystemMode` write, section 4.3.11.22: the value "SHALL be limited by
    /// the ControlSequenceOfOperation attribute" - see
    /// [`Self::is_supported_system_mode`].
    fn write_system_mode(
        &self,
        notifier: impl AttrChangeNotifier,
        value: SystemModeEnum,
    ) -> Result<(), Error> {
        if !Self::is_supported_system_mode(value) {
            Err(ErrorCode::ConstraintError)?;
        }

        self.hooks.set_system_mode(value)?;

        self.apply();
        self.notify(&notifier, AttributeId::SystemMode);

        Ok(())
    }

    /// The `SetpointRaiseLower` command, section 4.3.12.1.
    ///
    /// - `Mode = Cool`: "If the server does not support the COOL feature then
    ///   it SHALL respond with INVALID_COMMAND."
    /// - `Mode = Both`: "The client MAY indicate Both regardless of the server
    ///   feature support. The server SHALL only adjust the setpoint that it
    ///   supports and not respond with an error."
    /// - `amount` is "the amount (possibly negative) that should be added to
    ///   the setpoint(s), in steps of 0.1degC", whereas the setpoint attributes
    ///   are in 0.01degC - hence the factor of ten.
    /// - "If the resulting value is outside the limits imposed by
    ///   MinCoolSetpointLimit, MaxCoolSetpointLimit, MinHeatSetpointLimit and
    ///   MaxHeatSetpointLimit, the value is clamped to those limits. This is
    ///   not considered an error condition."
    fn raise_lower_setpoint(
        &self,
        notifier: impl AttrChangeNotifier,
        mode: SetpointRaiseLowerModeEnum,
        amount: i8,
    ) -> Result<(), Error> {
        if matches!(mode, SetpointRaiseLowerModeEnum::Cool) {
            Err(ErrorCode::InvalidCommand)?;
        }

        // `Heat` and `Both` both adjust the heating setpoint; `Both` simply has
        // no cooling setpoint to adjust here.
        let previous = self.hooks.occupied_heating_setpoint();
        let target = self.clamp_setpoint(previous as i32 + amount as i32 * 10);

        if target != previous {
            self.hooks.set_occupied_heating_setpoint(target)?;

            self.apply();
            self.notify(&notifier, AttributeId::OccupiedHeatingSetpoint);
        }

        Ok(())
    }

    /// Check that the cluster is configured in a way this handler can serve.
    ///
    /// # Panics
    ///
    /// Panics with a descriptive message if [`ThermostatHooks::CLUSTER`] is
    /// misconfigured. This is a programming error caught once at startup, not
    /// a runtime condition, which is why it is a panic rather than an `Error`.
    fn validate(&self) {
        if H::CLUSTER.revision != CLUSTER_REVISION {
            panic!(
                "Thermostat validation: incorrect revision number: expected {} got {}",
                CLUSTER_REVISION,
                H::CLUSTER.revision
            );
        }

        if !Self::supports_feature(Feature::HEATING.bits()) {
            panic!("Thermostat validation: the HEAT feature must be enabled - this handler only implements a heating thermostat");
        }

        if H::CLUSTER.feature_map & !SUPPORTED_FEATURES != 0 {
            panic!(
                "Thermostat validation: unsupported features in the feature map: 0x{:08x}. Only HEAT and LTNE are implemented",
                H::CLUSTER.feature_map & !SUPPORTED_FEATURES
            );
        }

        // Mandatory attributes: section 4.3.11 marks `LocalTemperature`,
        // `ControlSequenceOfOperation` and `SystemMode` as `M`, and
        // `OccupiedHeatingSetpoint` as mandatory given `HEAT`.
        if H::CLUSTER
            .attribute(AttributeId::LocalTemperature as _)
            .is_none()
            || H::CLUSTER
                .attribute(AttributeId::OccupiedHeatingSetpoint as _)
                .is_none()
            || H::CLUSTER
                .attribute(AttributeId::ControlSequenceOfOperation as _)
                .is_none()
            || H::CLUSTER.attribute(AttributeId::SystemMode as _).is_none()
        {
            panic!("Thermostat validation: missing required attributes: LocalTemperature, OccupiedHeatingSetpoint, ControlSequenceOfOperation or SystemMode");
        }

        // The four setpoint limits are individually optional, but the
        // constraint chain of section 4.3.6 and the `CONSTRAINT_ERROR` rules of
        // sections 4.3.11.12/15/16 are written in terms of all four. Serving a
        // strict subset would leave one end of the chain unenforceable.
        let limits = [
            AttributeId::AbsMinHeatSetpointLimit,
            AttributeId::AbsMaxHeatSetpointLimit,
            AttributeId::MinHeatSetpointLimit,
            AttributeId::MaxHeatSetpointLimit,
        ];

        let served = limits
            .iter()
            .filter(|attr| H::CLUSTER.attribute(**attr as _).is_some())
            .count();

        if served != 0 && served != limits.len() {
            panic!("Thermostat validation: the AbsMin/AbsMax/Min/MaxHeatSetpointLimit attributes must either all be present or all be absent");
        }

        // Every event this cluster defines is gated on `TEVT` (and provisional
        // in 1.6 besides), and the feature check above has already rejected it.
        // A served event would be advertised in `EventList` and never emitted.
        if H::CLUSTER.events().next().is_some() {
            panic!("Thermostat validation: no event can be served without the TEVT feature - pass `.with_events(with!())`");
        }

        if H::CLUSTER
            .command(CommandId::SetpointRaiseLower as _)
            .is_none()
        {
            panic!("Thermostat validation: missing required command: SetpointRaiseLower");
        }

        if H::ABS_MIN_HEAT_SETPOINT > H::ABS_MAX_HEAT_SETPOINT {
            panic!(
                "Thermostat validation: ABS_MIN_HEAT_SETPOINT ({}) must not exceed ABS_MAX_HEAT_SETPOINT ({})",
                H::ABS_MIN_HEAT_SETPOINT,
                H::ABS_MAX_HEAT_SETPOINT
            );
        }

        // Section 4.3.10.16: only the two heating sequences are consistent with
        // a server that supports HEAT but not COOL.
        if !matches!(
            H::CONTROL_SEQUENCE_OF_OPERATION,
            ControlSequenceOfOperationEnum::HeatingOnly
                | ControlSequenceOfOperationEnum::HeatingWithReheat
        ) {
            panic!("Thermostat validation: CONTROL_SEQUENCE_OF_OPERATION must be HeatingOnly or HeatingWithReheat for a heating-only thermostat");
        }
    }

    /// Pull persisted state back into the section 4.3.6 constraint chain.
    ///
    /// The limits and the setpoint are non-volatile and restored by the
    /// consumer, while the absolute limits are compiled in. A firmware update
    /// that narrows `ABS_MIN_HEAT_SETPOINT`/`ABS_MAX_HEAT_SETPOINT`, or a
    /// corrupt restore, can therefore leave stored values out of bounds. Fix
    /// them up once at startup rather than leaving the cluster reporting values
    /// it would reject on a write.
    fn repair(&self) -> Result<(), Error> {
        if Self::has_limits() {
            // AbsMinHeatSetpointLimit <= MinHeatSetpointLimit <= MaxHeatSetpointLimit <= AbsMaxHeatSetpointLimit
            let min = self
                .hooks
                .min_heat_setpoint_limit()
                .clamp(H::ABS_MIN_HEAT_SETPOINT, H::ABS_MAX_HEAT_SETPOINT);
            if min != self.hooks.min_heat_setpoint_limit() {
                self.hooks.set_min_heat_setpoint_limit(min)?;
            }

            let max = self
                .hooks
                .max_heat_setpoint_limit()
                .clamp(min, H::ABS_MAX_HEAT_SETPOINT);
            if max != self.hooks.max_heat_setpoint_limit() {
                self.hooks.set_max_heat_setpoint_limit(max)?;
            }
        }

        // MinHeatSetpointLimit <= OccupiedHeatingSetpoint <= MaxHeatSetpointLimit
        let setpoint = self.clamp_setpoint(self.hooks.occupied_heating_setpoint() as i32);
        if setpoint != self.hooks.occupied_heating_setpoint() {
            self.hooks.set_occupied_heating_setpoint(setpoint)?;
        }

        if !Self::is_supported_system_mode(self.hooks.system_mode()) {
            warn!("Thermostat: persisted SystemMode is not supported; falling back to Off");
            self.hooks.set_system_mode(SystemModeEnum::Off)?;
        }

        self.apply();

        Ok(())
    }
}

impl<H: ThermostatHooks> ClusterAsyncHandler for ThermostatHandler<H> {
    #[doc = "The cluster-metadata corresponding to this handler trait."]
    const CLUSTER: Cluster<'static> = H::CLUSTER;

    fn dataver(&self) -> u32 {
        self.dataver.get()
    }

    fn dataver_changed(&self) {
        self.dataver.changed();
    }

    fn lifecycle(&self, _ctx: impl HandlerContext, op: LifecycleOp) -> Result<(), Error> {
        if matches!(op, LifecycleOp::Startup) {
            self.validate();
            self.repair()?;
        }

        Ok(())
    }

    async fn run(&self, ctx: impl HandlerContext) -> Result<(), Error> {
        let mut hooks_fut = pin!(self.hooks.run(|message| self.out_of_band_message(message)));

        loop {
            match select(&mut hooks_fut, self.wait_pending()).await {
                Either::First(_) => panic!("ThermostatHooks::run returned; implementers MUST not return. Implementations should loop forever or await core::future::pending::<()>()."),
                Either::Second(pending) => self.notify_pending(&ctx, pending),
            }
        }
    }

    // Attribute accessors

    /// Section 4.3.11.2: the Calculated Local Temperature, or null when it is
    /// unavailable. With the `LTNE` feature the attribute "SHALL always report
    /// null" - the equipment still controls off the calculated value, there is
    /// simply no feedback for it over Matter.
    async fn local_temperature(&self, _ctx: impl ReadContext) -> Result<Nullable<i16>, Error> {
        if Self::supports_feature(Feature::LOCAL_TEMPERATURE_NOT_EXPOSED.bits()) {
            Ok(Nullable::none())
        } else {
            Ok(self.hooks.local_temperature())
        }
    }

    fn abs_min_heat_setpoint_limit(
        &self,
        _ctx: impl ReadContext,
    ) -> impl Future<Output = Result<i16, Error>> {
        ready(Ok(H::ABS_MIN_HEAT_SETPOINT))
    }

    fn abs_max_heat_setpoint_limit(
        &self,
        _ctx: impl ReadContext,
    ) -> impl Future<Output = Result<i16, Error>> {
        ready(Ok(H::ABS_MAX_HEAT_SETPOINT))
    }

    fn occupied_heating_setpoint(
        &self,
        _ctx: impl ReadContext,
    ) -> impl Future<Output = Result<i16, Error>> {
        ready(Ok(self.hooks.occupied_heating_setpoint()))
    }

    fn min_heat_setpoint_limit(
        &self,
        _ctx: impl ReadContext,
    ) -> impl Future<Output = Result<i16, Error>> {
        ready(Ok(self.hooks.min_heat_setpoint_limit()))
    }

    fn max_heat_setpoint_limit(
        &self,
        _ctx: impl ReadContext,
    ) -> impl Future<Output = Result<i16, Error>> {
        ready(Ok(self.hooks.max_heat_setpoint_limit()))
    }

    async fn control_sequence_of_operation(
        &self,
        _ctx: impl ReadContext,
    ) -> Result<ControlSequenceOfOperationEnum, Error> {
        Ok(H::CONTROL_SEQUENCE_OF_OPERATION)
    }

    async fn system_mode(&self, _ctx: impl ReadContext) -> Result<SystemModeEnum, Error> {
        Ok(self.hooks.system_mode())
    }

    // Attribute writes

    /// See [`Self::write_occupied_heating_setpoint`].
    fn set_occupied_heating_setpoint(
        &self,
        ctx: impl WriteContext,
        value: i16,
    ) -> impl Future<Output = Result<(), Error>> {
        ready(self.write_occupied_heating_setpoint(&ctx, value))
    }

    /// See [`Self::write_min_heat_setpoint_limit`].
    fn set_min_heat_setpoint_limit(
        &self,
        ctx: impl WriteContext,
        value: i16,
    ) -> impl Future<Output = Result<(), Error>> {
        ready(self.write_min_heat_setpoint_limit(&ctx, value))
    }

    /// See [`Self::write_max_heat_setpoint_limit`].
    fn set_max_heat_setpoint_limit(
        &self,
        ctx: impl WriteContext,
        value: i16,
    ) -> impl Future<Output = Result<(), Error>> {
        ready(self.write_max_heat_setpoint_limit(&ctx, value))
    }

    /// Section 4.3.11.21: "If an attempt is made to write to this attribute,
    /// the server SHALL silently ignore the write and the value of this
    /// attribute SHALL remain unchanged. This behavior is in place for
    /// backwards compatibility with existing thermostats."
    ///
    /// "Silently" means `SUCCESS` with no state change - not
    /// `UNSUPPORTED_WRITE`, and no change notification either.
    async fn set_control_sequence_of_operation(
        &self,
        _ctx: impl WriteContext,
        _value: ControlSequenceOfOperationEnum,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// See [`Self::write_system_mode`].
    async fn set_system_mode(
        &self,
        ctx: impl WriteContext,
        value: SystemModeEnum,
    ) -> Result<(), Error> {
        self.write_system_mode(&ctx, value)
    }

    // Commands

    /// See [`Self::raise_lower_setpoint`].
    async fn handle_setpoint_raise_lower(
        &self,
        ctx: impl InvokeContext,
        request: SetpointRaiseLowerRequest<'_>,
    ) -> Result<(), Error> {
        self.raise_lower_setpoint(&ctx, request.mode()?, request.amount()?)
    }

    // Commands that belong to features this handler does not implement. They
    // are filtered out of `AcceptedCommandList` by `with_cmds`, so the adaptor
    // rejects them before they reach us; these impls only exist because the
    // generated trait has no defaults for command handlers.

    async fn handle_set_weekly_schedule(
        &self,
        _ctx: impl InvokeContext,
        _request: SetWeeklyScheduleRequest<'_>,
    ) -> Result<(), Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_get_weekly_schedule<P: TLVBuilderParent>(
        &self,
        _ctx: impl InvokeContext,
        _request: GetWeeklyScheduleRequest<'_>,
        _response: GetWeeklyScheduleResponseBuilder<P>,
    ) -> Result<P, Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_clear_weekly_schedule(&self, _ctx: impl InvokeContext) -> Result<(), Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_set_active_schedule_request(
        &self,
        _ctx: impl InvokeContext,
        _request: SetActiveScheduleRequestRequest<'_>,
    ) -> Result<(), Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_set_active_preset_request(
        &self,
        _ctx: impl InvokeContext,
        _request: SetActivePresetRequestRequest<'_>,
    ) -> Result<(), Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_add_thermostat_suggestion<P: TLVBuilderParent>(
        &self,
        _ctx: impl InvokeContext,
        _request: AddThermostatSuggestionRequest<'_>,
        _response: AddThermostatSuggestionResponseBuilder<P>,
    ) -> Result<P, Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_remove_thermostat_suggestion(
        &self,
        _ctx: impl InvokeContext,
        _request: RemoveThermostatSuggestionRequest<'_>,
    ) -> Result<(), Error> {
        Err(ErrorCode::CommandNotFound.into())
    }

    async fn handle_atomic_request<P: TLVBuilderParent>(
        &self,
        _ctx: impl InvokeContext,
        _request: AtomicRequestRequest<'_>,
        _response: AtomicResponseBuilder<P>,
    ) -> Result<P, Error> {
        Err(ErrorCode::CommandNotFound.into())
    }
}

/// The device-specific half of a heating thermostat.
///
/// The handler owns every spec rule; the hooks own the hardware and the
/// persistence. Each `set_*` method below backs a non-volatile attribute and
/// SHALL persist its value across reboots. None of them should validate or
/// clamp - the handler has already done that, and re-checking here would only
/// let the two disagree.
pub trait ThermostatHooks {
    /// The cluster metadata, which selects the features, attributes and
    /// commands this instance serves. See [`ThermostatHandler::validate`] for
    /// what a heating-only configuration has to look like.
    const CLUSTER: Cluster<'static>;

    /// `AbsMinHeatSetpointLimit` (section 4.3.11.5): "the absolute minimum
    /// level that the heating setpoint MAY be set to. This is a limitation
    /// imposed by the manufacturer." Its `fixed` quality is why it is a const.
    ///
    /// In 0.01°C; the spec default is 700 (7.00°C).
    const ABS_MIN_HEAT_SETPOINT: i16 = 700;

    /// `AbsMaxHeatSetpointLimit` (section 4.3.11.6), in 0.01°C; the spec
    /// default is 3000 (30.00°C).
    const ABS_MAX_HEAT_SETPOINT: i16 = 3000;

    /// `ControlSequenceOfOperation` (section 4.3.11.21). A const because
    /// writes to the attribute are silently ignored, so it never changes.
    const CONTROL_SEQUENCE_OF_OPERATION: ControlSequenceOfOperationEnum =
        ControlSequenceOfOperationEnum::HeatingOnly;

    /// The Calculated Local Temperature in 0.01°C, or null when the reading is
    /// unavailable (section 4.3.11.1).
    fn local_temperature(&self) -> Nullable<i16>;

    /// Raw `OccupiedHeatingSetpoint` getter, in 0.01°C.
    fn occupied_heating_setpoint(&self) -> i16;

    /// Raw `OccupiedHeatingSetpoint` setter. This value SHALL be persisted
    /// across reboots.
    fn set_occupied_heating_setpoint(&self, value: i16) -> Result<(), Error>;

    /// Raw `MinHeatSetpointLimit` getter, in 0.01°C.
    ///
    /// Only called when the setpoint-limit attributes are served; the default
    /// returns [`Self::ABS_MIN_HEAT_SETPOINT`] for devices that omit them.
    fn min_heat_setpoint_limit(&self) -> i16 {
        Self::ABS_MIN_HEAT_SETPOINT
    }

    /// Raw `MinHeatSetpointLimit` setter. This value SHALL be persisted across
    /// reboots.
    fn set_min_heat_setpoint_limit(&self, _value: i16) -> Result<(), Error> {
        Err(ErrorCode::AttributeNotFound.into())
    }

    /// Raw `MaxHeatSetpointLimit` getter, in 0.01°C.
    fn max_heat_setpoint_limit(&self) -> i16 {
        Self::ABS_MAX_HEAT_SETPOINT
    }

    /// Raw `MaxHeatSetpointLimit` setter. This value SHALL be persisted across
    /// reboots.
    fn set_max_heat_setpoint_limit(&self, _value: i16) -> Result<(), Error> {
        Err(ErrorCode::AttributeNotFound.into())
    }

    /// Raw `SystemMode` getter.
    fn system_mode(&self) -> SystemModeEnum;

    /// Raw `SystemMode` setter. This value SHALL be persisted across reboots.
    fn set_system_mode(&self, value: SystemModeEnum) -> Result<(), Error>;

    /// Push the resolved control state onto the equipment.
    ///
    /// Called after every change to `SystemMode` or `OccupiedHeatingSetpoint`,
    /// and once at startup. The Matter spec deliberately does not define the
    /// control algorithm, so turning `(mode, setpoint, local temperature)` into
    /// heat demand - with whatever hysteresis or PI loop the hardware needs -
    /// is the device's business, not the cluster's.
    fn apply(&self, system_mode: SystemModeEnum, heating_setpoint: i16);

    /// Background task for out-of-band notifications to the handler.
    ///
    /// This future MUST NOT return. Implementers should either loop forever or
    /// await `core::future::pending::<()>()`, so the SDK's task does not
    /// observe a completed future.
    ///
    /// # Panics
    /// The SDK will panic if this method returns.
    async fn run<F: Fn(OutOfBandMessage)>(&self, _notify: F) {
        core::future::pending::<()>().await
    }
}

impl<T> ThermostatHooks for &T
where
    T: ThermostatHooks,
{
    const CLUSTER: Cluster<'static> = T::CLUSTER;
    const ABS_MIN_HEAT_SETPOINT: i16 = T::ABS_MIN_HEAT_SETPOINT;
    const ABS_MAX_HEAT_SETPOINT: i16 = T::ABS_MAX_HEAT_SETPOINT;
    const CONTROL_SEQUENCE_OF_OPERATION: ControlSequenceOfOperationEnum =
        T::CONTROL_SEQUENCE_OF_OPERATION;

    fn local_temperature(&self) -> Nullable<i16> {
        (*self).local_temperature()
    }

    fn occupied_heating_setpoint(&self) -> i16 {
        (*self).occupied_heating_setpoint()
    }

    fn set_occupied_heating_setpoint(&self, value: i16) -> Result<(), Error> {
        (*self).set_occupied_heating_setpoint(value)
    }

    fn min_heat_setpoint_limit(&self) -> i16 {
        (*self).min_heat_setpoint_limit()
    }

    fn set_min_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
        (*self).set_min_heat_setpoint_limit(value)
    }

    fn max_heat_setpoint_limit(&self) -> i16 {
        (*self).max_heat_setpoint_limit()
    }

    fn set_max_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
        (*self).set_max_heat_setpoint_limit(value)
    }

    fn system_mode(&self) -> SystemModeEnum {
        (*self).system_mode()
    }

    fn set_system_mode(&self, value: SystemModeEnum) -> Result<(), Error> {
        (*self).set_system_mode(value)
    }

    fn apply(&self, system_mode: SystemModeEnum, heating_setpoint: i16) {
        (*self).apply(system_mode, heating_setpoint)
    }

    fn run<F: Fn(OutOfBandMessage)>(&self, notify: F) -> impl Future<Output = ()> {
        (*self).run(notify)
    }
}

/// A reference [`ThermostatHooks`] implementation, simulating a heated room.
///
/// Used by `examples/src/bin/thermostat.rs` and by the unit tests below. It is
/// always compiled (not `#[cfg(test)]`) so that downstream examples and test
/// binaries can reuse it, mirroring [`super::on_off::test`] and
/// [`super::level_control::test`].
pub mod test {
    use core::cell::Cell;

    use embassy_time::{Duration, Timer};

    use crate::dm::clusters::decl::thermostat as thermostat_cluster;
    use crate::dm::Cluster;
    use crate::error::Error;
    use crate::tlv::Nullable;
    use crate::with;

    use super::{OutOfBandMessage, SystemModeEnum, ThermostatHooks};

    /// How often the simulated room temperature is recomputed.
    const TICK: Duration = Duration::from_secs(5);

    /// How fast the room warms towards the setpoint while heating, in 0.01°C
    /// per [`TICK`].
    const HEATING_RATE: i16 = 20;

    /// How fast the room cools towards [`AMBIENT`] while idle, in 0.01°C per
    /// [`TICK`].
    const COOLING_RATE: i16 = 10;

    /// The temperature the simulated room drifts to with the heating off.
    const AMBIENT: i16 = 1600;

    /// A simulated heating thermostat.
    ///
    /// Note that a real device would persist the four non-volatile attributes;
    /// this one keeps them in RAM, so they reset on restart. See
    /// `examples/src/bin/dimmable_light.rs` and `tests/src/bin/light_tests.rs`
    /// for file- and KV-backed persistence of hook state.
    pub struct TestThermostatDeviceLogic {
        local_temperature: Cell<i16>,
        occupied_heating_setpoint: Cell<i16>,
        min_heat_setpoint_limit: Cell<i16>,
        max_heat_setpoint_limit: Cell<i16>,
        system_mode: Cell<SystemModeEnum>,
        heating: Cell<bool>,
    }

    impl TestThermostatDeviceLogic {
        /// Create a new simulated thermostat, idle at [`AMBIENT`] with the
        /// spec-default setpoint of 20.00°C.
        pub const fn new() -> Self {
            Self {
                local_temperature: Cell::new(AMBIENT),
                occupied_heating_setpoint: Cell::new(2000),
                min_heat_setpoint_limit: Cell::new(Self::ABS_MIN_HEAT_SETPOINT),
                max_heat_setpoint_limit: Cell::new(Self::ABS_MAX_HEAT_SETPOINT),
                system_mode: Cell::new(SystemModeEnum::Off),
                heating: Cell::new(false),
            }
        }

        /// Whether the simulated heater is currently calling for heat.
        pub fn heating(&self) -> bool {
            self.heating.get()
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

            // A one-notch hysteresis band around the setpoint, so the
            // simulated relay does not chatter every tick.
            let setpoint = self.occupied_heating_setpoint.get();
            self.heating.set(
                matches!(self.system_mode.get(), SystemModeEnum::Heat)
                    && if self.heating.get() {
                        temperature < setpoint.saturating_add(HEATING_RATE)
                    } else {
                        temperature < setpoint.saturating_sub(HEATING_RATE)
                    },
            );

            temperature != previous
        }
    }

    impl Default for TestThermostatDeviceLogic {
        fn default() -> Self {
            Self::new()
        }
    }

    impl ThermostatHooks for TestThermostatDeviceLogic {
        /// A heating-only thermostat: the `HEAT` feature alone, the four
        /// mandatory attributes plus the optional heat setpoint limits, and
        /// the one mandatory command.
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

        fn local_temperature(&self) -> Nullable<i16> {
            Nullable::some(self.local_temperature.get())
        }

        fn occupied_heating_setpoint(&self) -> i16 {
            self.occupied_heating_setpoint.get()
        }

        fn set_occupied_heating_setpoint(&self, value: i16) -> Result<(), Error> {
            self.occupied_heating_setpoint.set(value);
            Ok(())
        }

        fn min_heat_setpoint_limit(&self) -> i16 {
            self.min_heat_setpoint_limit.get()
        }

        fn set_min_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
            self.min_heat_setpoint_limit.set(value);
            Ok(())
        }

        fn max_heat_setpoint_limit(&self) -> i16 {
            self.max_heat_setpoint_limit.get()
        }

        fn set_max_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
            self.max_heat_setpoint_limit.set(value);
            Ok(())
        }

        fn system_mode(&self) -> SystemModeEnum {
            self.system_mode.get()
        }

        fn set_system_mode(&self, value: SystemModeEnum) -> Result<(), Error> {
            self.system_mode.set(value);
            Ok(())
        }

        fn apply(&self, system_mode: SystemModeEnum, heating_setpoint: i16) {
            info!(
                "Emulation: system mode {:?}, heating setpoint {}.{:02}C",
                system_mode,
                heating_setpoint / 100,
                (heating_setpoint % 100).abs()
            );

            // Re-evaluate the relay immediately rather than waiting a tick, so
            // that switching to `Heat` has a visible effect right away.
            self.heating.set(
                matches!(system_mode, SystemModeEnum::Heat)
                    && self.local_temperature.get() < heating_setpoint,
            );
        }

        async fn run<F: Fn(OutOfBandMessage)>(&self, notify: F) {
            loop {
                // In a real device we would wait on a temperature sensor.
                Timer::after(TICK).await;

                if self.tick() {
                    notify(OutOfBandMessage::LocalTemperature);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for the spec rules [`ThermostatHandler`] enforces.
    //!
    //! They drive the context-free helpers the `ClusterAsyncHandler` methods
    //! delegate to, rather than the methods themselves: a `ReadContext` /
    //! `WriteContext` can only be built around a live `Matter` instance,
    //! whereas the helpers need nothing but an [`AttrChangeNotifier`] - and
    //! `()` is a no-op one.

    use core::cell::Cell;

    use crate::dm::clusters::decl::thermostat as thermostat_cluster;
    use crate::dm::{AttrId, Cluster, CmdId, Dataver};
    use crate::error::{Error, ErrorCode};
    use crate::tlv::Nullable;
    use crate::with;

    use super::test::TestThermostatDeviceLogic;
    use super::{
        AttributeId, CommandId, ControlSequenceOfOperationEnum, SetpointRaiseLowerModeEnum,
        SystemModeEnum, ThermostatHandler, ThermostatHooks,
    };

    /// `()` is a no-op `AttrChangeNotifier`, which is all the helpers need.
    const NULL_CTX: &() = &();

    /// Hooks whose feature map is a const parameter, so each test can pick its
    /// own cluster configuration. Mirrors `color_control::tests::MockHooks`.
    struct MockHooks<const F: u32> {
        local_temperature: Cell<Option<i16>>,
        occupied_heating_setpoint: Cell<i16>,
        min_heat_setpoint_limit: Cell<i16>,
        max_heat_setpoint_limit: Cell<i16>,
        system_mode: Cell<SystemModeEnum>,
        applied: Cell<u32>,
    }

    impl<const F: u32> MockHooks<F> {
        const fn new() -> Self {
            Self {
                local_temperature: Cell::new(Some(1900)),
                occupied_heating_setpoint: Cell::new(2000),
                min_heat_setpoint_limit: Cell::new(Self::ABS_MIN_HEAT_SETPOINT),
                max_heat_setpoint_limit: Cell::new(Self::ABS_MAX_HEAT_SETPOINT),
                system_mode: Cell::new(SystemModeEnum::Off),
                applied: Cell::new(0),
            }
        }
    }

    impl<const F: u32> ThermostatHooks for MockHooks<F> {
        const CLUSTER: Cluster<'static> = thermostat_cluster::FULL_CLUSTER
            .with_revision(11)
            .with_features(F)
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

        fn local_temperature(&self) -> Nullable<i16> {
            Nullable::new(self.local_temperature.get())
        }

        fn occupied_heating_setpoint(&self) -> i16 {
            self.occupied_heating_setpoint.get()
        }

        fn set_occupied_heating_setpoint(&self, value: i16) -> Result<(), Error> {
            self.occupied_heating_setpoint.set(value);
            Ok(())
        }

        fn min_heat_setpoint_limit(&self) -> i16 {
            self.min_heat_setpoint_limit.get()
        }

        fn set_min_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
            self.min_heat_setpoint_limit.set(value);
            Ok(())
        }

        fn max_heat_setpoint_limit(&self) -> i16 {
            self.max_heat_setpoint_limit.get()
        }

        fn set_max_heat_setpoint_limit(&self, value: i16) -> Result<(), Error> {
            self.max_heat_setpoint_limit.set(value);
            Ok(())
        }

        fn system_mode(&self) -> SystemModeEnum {
            self.system_mode.get()
        }

        fn set_system_mode(&self, value: SystemModeEnum) -> Result<(), Error> {
            self.system_mode.set(value);
            Ok(())
        }

        fn apply(&self, _system_mode: SystemModeEnum, _heating_setpoint: i16) {
            self.applied.set(self.applied.get() + 1);
        }
    }

    const HEAT: u32 = thermostat_cluster::Feature::HEATING.bits();
    const HEAT_LTNE: u32 = HEAT | thermostat_cluster::Feature::LOCAL_TEMPERATURE_NOT_EXPOSED.bits();

    fn handler<const F: u32>() -> ThermostatHandler<MockHooks<F>> {
        ThermostatHandler::new(Dataver::new(1), 1, MockHooks::<F>::new())
    }

    fn code<T>(result: Result<T, Error>) -> Result<T, ErrorCode> {
        result.map_err(|e| e.code())
    }

    /// Catches drift between `TestThermostatDeviceLogic::CLUSTER` and
    /// `ThermostatHandler::validate()`.
    #[test]
    fn test_logic_passes_handler_validate() {
        let logic = TestThermostatDeviceLogic::new();
        let handler = ThermostatHandler::new(Dataver::new(1), 1, &logic);

        handler.validate();
        handler.repair().unwrap();
    }

    /// Section 4.3.11.2: with `LTNE` the attribute "SHALL always report null",
    /// whatever the sensor says.
    #[test]
    fn local_temperature_is_null_with_ltne() {
        assert!(!ThermostatHandler::<MockHooks<HEAT>>::supports_feature(
            super::Feature::LOCAL_TEMPERATURE_NOT_EXPOSED.bits()
        ));
        assert!(ThermostatHandler::<MockHooks<HEAT_LTNE>>::supports_feature(
            super::Feature::LOCAL_TEMPERATURE_NOT_EXPOSED.bits()
        ));

        assert_eq!(
            handler::<HEAT>().hooks.local_temperature(),
            Nullable::some(1900)
        );
    }

    /// Section 4.3.11.22: `SystemMode` is limited by
    /// `ControlSequenceOfOperation`; with `HeatingOnly` only `Off` and `Heat`
    /// remain.
    #[test]
    fn system_mode_write_rejects_unsupported_modes() {
        let handler = handler::<HEAT>();

        for mode in [
            SystemModeEnum::Cool,
            SystemModeEnum::Auto,
            SystemModeEnum::Precooling,
            SystemModeEnum::EmergencyHeat,
            SystemModeEnum::FanOnly,
        ] {
            assert_eq!(
                code(handler.write_system_mode(NULL_CTX, mode)),
                Err(ErrorCode::ConstraintError),
                "SystemMode {mode:?} should not be accepted"
            );
        }

        for mode in [SystemModeEnum::Off, SystemModeEnum::Heat] {
            handler.write_system_mode(NULL_CTX, mode).unwrap();
            assert_eq!(handler.hooks.system_mode(), mode);
        }
    }

    /// Section 4.3.11.12: out-of-range writes are a `CONSTRAINT_ERROR`, in
    /// contrast to `SetpointRaiseLower`, which clamps.
    #[test]
    fn occupied_heating_setpoint_write_out_of_range_is_constraint_error() {
        let handler = handler::<HEAT>();

        handler
            .write_min_heat_setpoint_limit(NULL_CTX, 1500)
            .unwrap();
        handler
            .write_max_heat_setpoint_limit(NULL_CTX, 2500)
            .unwrap();

        assert_eq!(
            code(handler.write_occupied_heating_setpoint(NULL_CTX, 2501)),
            Err(ErrorCode::ConstraintError)
        );
        assert_eq!(
            code(handler.write_occupied_heating_setpoint(NULL_CTX, 1499)),
            Err(ErrorCode::ConstraintError)
        );

        // The bounds themselves are in range.
        handler
            .write_occupied_heating_setpoint(NULL_CTX, 2500)
            .unwrap();
        handler
            .write_occupied_heating_setpoint(NULL_CTX, 1500)
            .unwrap();
        assert_eq!(handler.hooks.occupied_heating_setpoint(), 1500);
    }

    /// Section 4.3.11.15/16: a limit write that conflicts with the setpoint
    /// drags the setpoint along by the minimum amount.
    #[test]
    fn setpoint_limit_writes_adjust_the_setpoint() {
        let handler = handler::<HEAT>();

        // Floor above the setpoint pushes it up.
        handler
            .write_min_heat_setpoint_limit(NULL_CTX, 2200)
            .unwrap();
        assert_eq!(handler.hooks.occupied_heating_setpoint(), 2200);

        // Ceiling below the setpoint pulls it down.
        handler
            .write_min_heat_setpoint_limit(NULL_CTX, 700)
            .unwrap();
        handler
            .write_max_heat_setpoint_limit(NULL_CTX, 1800)
            .unwrap();
        assert_eq!(handler.hooks.occupied_heating_setpoint(), 1800);
    }

    /// Section 4.3.6: user-configurable limits stay inside the device limits
    /// and do not cross each other. Those writes cannot be resolved by moving a
    /// setpoint, so they are a `CONSTRAINT_ERROR`.
    #[test]
    fn setpoint_limit_writes_outside_the_constraint_chain_are_rejected() {
        let handler = handler::<HEAT>();

        assert_eq!(
            code(handler.write_min_heat_setpoint_limit(NULL_CTX, 699)),
            Err(ErrorCode::ConstraintError)
        );
        assert_eq!(
            code(handler.write_max_heat_setpoint_limit(NULL_CTX, 3001)),
            Err(ErrorCode::ConstraintError)
        );

        handler
            .write_max_heat_setpoint_limit(NULL_CTX, 2000)
            .unwrap();
        assert_eq!(
            code(handler.write_min_heat_setpoint_limit(NULL_CTX, 2001)),
            Err(ErrorCode::ConstraintError)
        );
        assert_eq!(
            code(handler.write_max_heat_setpoint_limit(NULL_CTX, 699)),
            Err(ErrorCode::ConstraintError)
        );
    }

    /// Section 4.3.12.1.1.2: a server without the `COOL` feature "SHALL respond
    /// with INVALID_COMMAND" to `Mode = Cool`.
    #[test]
    fn setpoint_raise_lower_rejects_cool() {
        let handler = handler::<HEAT>();

        assert_eq!(
            code(handler.raise_lower_setpoint(NULL_CTX, SetpointRaiseLowerModeEnum::Cool, 10)),
            Err(ErrorCode::InvalidCommand)
        );
        assert_eq!(handler.hooks.occupied_heating_setpoint(), 2000);
    }

    /// Section 4.3.12.1.2: `Amount` is in steps of 0.1degC while the setpoint
    /// attribute is in 0.01degC. Section 4.3.12.1.1.3: `Both` is accepted
    /// regardless of feature support and adjusts only what the server has.
    #[test]
    fn setpoint_raise_lower_applies_tenths_of_a_degree() {
        for mode in [
            SetpointRaiseLowerModeEnum::Heat,
            SetpointRaiseLowerModeEnum::Both,
        ] {
            let handler = handler::<HEAT>();

            handler.raise_lower_setpoint(NULL_CTX, mode, 10).unwrap();
            assert_eq!(handler.hooks.occupied_heating_setpoint(), 2100);

            handler.raise_lower_setpoint(NULL_CTX, mode, -25).unwrap();
            assert_eq!(handler.hooks.occupied_heating_setpoint(), 1850);
        }
    }

    /// Section 4.3.12.1.3: "If the resulting value is outside the limits [...]
    /// the value is clamped to those limits. This is not considered an error
    /// condition."
    #[test]
    fn setpoint_raise_lower_clamps_without_erroring() {
        let handler = handler::<HEAT>();

        handler
            .write_min_heat_setpoint_limit(NULL_CTX, 1500)
            .unwrap();
        handler
            .write_max_heat_setpoint_limit(NULL_CTX, 2500)
            .unwrap();

        handler
            .raise_lower_setpoint(NULL_CTX, SetpointRaiseLowerModeEnum::Heat, 127)
            .unwrap();
        assert_eq!(handler.hooks.occupied_heating_setpoint(), 2500);

        handler
            .raise_lower_setpoint(NULL_CTX, SetpointRaiseLowerModeEnum::Heat, -128)
            .unwrap();
        assert_eq!(handler.hooks.occupied_heating_setpoint(), 1500);
    }

    /// Section 4.3.11.21: the `ControlSequenceOfOperation` a heating-only
    /// thermostat reports is fixed, and writes to it are silently ignored -
    /// which is why it is a hooks const with no setter at all.
    #[test]
    fn control_sequence_of_operation_is_fixed() {
        assert_eq!(
            <MockHooks<HEAT> as ThermostatHooks>::CONTROL_SEQUENCE_OF_OPERATION,
            ControlSequenceOfOperationEnum::HeatingOnly
        );
    }

    /// Section 4.3.6: startup pulls persisted state back into the chain.
    #[test]
    fn repair_restores_the_constraint_chain() {
        let handler = handler::<HEAT>();

        // Values a narrowed firmware range or a bad restore could leave behind.
        handler.hooks.min_heat_setpoint_limit.set(100);
        handler.hooks.max_heat_setpoint_limit.set(9000);
        handler.hooks.occupied_heating_setpoint.set(8000);
        handler.hooks.system_mode.set(SystemModeEnum::Cool);

        handler.repair().unwrap();

        assert_eq!(handler.hooks.min_heat_setpoint_limit.get(), 700);
        assert_eq!(handler.hooks.max_heat_setpoint_limit.get(), 3000);
        assert_eq!(handler.hooks.occupied_heating_setpoint.get(), 3000);
        assert_eq!(handler.hooks.system_mode.get(), SystemModeEnum::Off);
    }

    /// Pin the wire-visible shape of the endpoint: the `AttributeList` and
    /// `AcceptedCommandList` a controller reads back, and the `FeatureMap` and
    /// `ClusterRevision` that go with them.
    #[test]
    fn serves_the_heating_only_element_set() {
        let cluster = <TestThermostatDeviceLogic as ThermostatHooks>::CLUSTER;

        assert_eq!(cluster.id, 0x0201);
        assert_eq!(cluster.revision, 11);
        assert_eq!(cluster.feature_map, HEAT);

        let attrs: heapless::Vec<_, 8> = cluster
            .attributes()
            .map(|attr| attr.id)
            .filter(|id| *id < 0xF000) // skip the global attributes
            .collect();

        assert_eq!(
            attrs,
            [
                AttributeId::LocalTemperature as AttrId,
                AttributeId::AbsMinHeatSetpointLimit as AttrId,
                AttributeId::AbsMaxHeatSetpointLimit as AttrId,
                AttributeId::OccupiedHeatingSetpoint as AttrId,
                AttributeId::MinHeatSetpointLimit as AttrId,
                AttributeId::MaxHeatSetpointLimit as AttrId,
                AttributeId::ControlSequenceOfOperation as AttrId,
                AttributeId::SystemMode as AttrId,
            ]
        );

        let cmds: heapless::Vec<_, 1> = cluster.commands().map(|cmd| cmd.id).collect();

        assert_eq!(cmds, [CommandId::SetpointRaiseLower as CmdId]);
    }

    /// A misconfigured `CLUSTER` is a programming error, caught once at startup.
    #[test]
    #[should_panic(expected = "the HEAT feature must be enabled")]
    fn validate_rejects_a_cluster_without_heat() {
        handler::<0>().validate();
    }

    #[test]
    #[should_panic(expected = "unsupported features in the feature map")]
    fn validate_rejects_unsupported_features() {
        const AUTO: u32 = HEAT
            | thermostat_cluster::Feature::COOLING.bits()
            | thermostat_cluster::Feature::AUTO_MODE.bits();

        handler::<AUTO>().validate();
    }
}
