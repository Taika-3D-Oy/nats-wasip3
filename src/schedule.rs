//! NATS 2.14 JetStream Message Scheduling support ([ADR-51]).
//!
//! Message scheduling is a server-side feature where a single **schedule
//! message**, stored on its own dedicated subject, causes the server to
//! repeatedly (or once) publish a derived message to a target subject on a
//! given schedule.
//!
//! ## Requirements
//!
//! - NATS server **2.14+**
//! - The stream must have [`StreamConfig::allow_msg_schedules`] set to `true`.
//!   (This implicitly enables `allow_rollup_hdrs`.)
//! - If `ttl` is used, the stream must also have `allow_msg_ttl: true`.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use nats_wasip3::jetstream::StreamConfig;
//! use nats_wasip3::schedule::{Schedule, ScheduleSpec, after_secs_rfc3339};
//!
//! // 1. Create (or update) a stream with scheduling enabled.
//! js.create_stream(&StreamConfig {
//!     name: "work".to_string(),
//!     subjects: vec!["schedules.>".to_string(), "jobs.>".to_string()],
//!     allow_msg_schedules: true,
//!     allow_msg_ttl: true,   // needed if you set ttl in the spec
//!     ..Default::default()
//! }).await?;
//!
//! // 2. Publish a repeating schedule — fires every 5 minutes.
//! let spec = ScheduleSpec {
//!     schedule: Some(Schedule::Every("5m".to_string())),
//!     target: "jobs.heartbeat".to_string(),
//!     ttl: Some("5m".to_string()),
//!     ..Default::default()
//! };
//! js.publish_with_headers("schedules.heartbeat.main", &spec.to_headers(), b"ping").await?;
//!
//! // 3. One-shot delayed publish — fire in 10 minutes.
//! let at = after_secs_rfc3339(600);
//! let spec = ScheduleSpec {
//!     schedule: Some(Schedule::At(at)),
//!     target: "jobs.delayed".to_string(),
//!     ..Default::default()
//! };
//! js.publish_with_headers("schedules.delayed.task1", &spec.to_headers(), b"run").await?;
//! ```
//!
//! ## Subject layout
//!
//! Every schedule must have **its own unique subject**.  Use a subject pattern
//! such as `schedules.>` for the stream and place each schedule on its own
//! leaf, e.g. `schedules.orders.retry` or `schedules.sensors.sample.uuid`.
//! The target subjects (e.g. `orders`, `jobs.run`) must also be covered by the
//! stream's subject list.
//!
//! ## Stopping a schedule
//!
//! Delete or purge the schedule subject to stop it:
//!
//! ```rust,ignore
//! js.purge_stream_subject("work", "schedules.heartbeat.main").await?;
//! ```
//!
//! [ADR-51]: https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md
//! [`StreamConfig::allow_msg_schedules`]: crate::jetstream::StreamConfig::allow_msg_schedules

use crate::proto::Headers;

// ── Header name constants ──────────────────────────────────────────

/// Set on the schedule message to define when it fires.
///
/// Values:
/// - `@at 2026-06-01T09:00:00Z` — one-shot at an RFC 3339 UTC timestamp
/// - `@every 5m` — interval (min `1s`, uses Go `time.ParseDuration` syntax)
/// - `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` — predefined
/// - `"0 30 9 * * 1-5"` — 6-field cron (s m h dom mon dow)
pub const HEADER_SCHEDULE: &str = "Nats-Schedule";

/// The subject the server publishes the generated message to.
/// Must be covered by the same stream's subject list.
pub const HEADER_SCHEDULE_TARGET: &str = "Nats-Schedule-Target";

/// When set, the server re-emits the **last message on this subject** as the
/// body (subject sampling). If no message exists the schedule's own body is
/// used as a fallback. Wildcards are not supported.
pub const HEADER_SCHEDULE_SOURCE: &str = "Nats-Schedule-Source";

/// Duration string (Go format, e.g. `"5m"`) stamped as `Nats-TTL` on every
/// generated message. Requires `StreamConfig::allow_msg_ttl` on the stream.
pub const HEADER_SCHEDULE_TTL: &str = "Nats-Schedule-TTL";

/// IANA time-zone name for cron-expression schedules
/// (e.g. `"Europe/Helsinki"`, `"America/New_York"`).
/// Must **not** be used with `@at` or `@every` — the server rejects it.
pub const HEADER_SCHEDULE_TIME_ZONE: &str = "Nats-Schedule-Time-Zone";

/// When set to `"sub"`, the server applies a per-subject rollup on each fire.
pub const HEADER_SCHEDULE_ROLLUP: &str = "Nats-Schedule-Rollup";

// ── Headers set by the server on generated messages ────────────────

/// Set by the server on every generated message: the subject that holds the
/// originating schedule. Useful when a consumer receives messages from
/// multiple schedules.
pub const HEADER_SCHEDULER: &str = "Nats-Scheduler";

/// Set by the server on generated messages:
/// - For cron / `@every` schedules: the RFC 3339 timestamp of the **next**
///   invocation.
/// - For one-shot `@at` schedules: the literal string `"purge"`, indicating
///   the schedule will be removed after this message is delivered.
pub const HEADER_SCHEDULE_NEXT: &str = "Nats-Schedule-Next";

// ── Schedule expression ────────────────────────────────────────────

/// A schedule expression describing when the server fires a schedule message.
///
/// Pass to [`ScheduleSpec::schedule`] and call [`ScheduleSpec::to_headers`]
/// to obtain the `Headers` map ready for publishing.
#[derive(Debug, Clone)]
pub enum Schedule {
    /// Fire **once** at the given UTC time (RFC 3339 string).
    ///
    /// Use [`now_rfc3339`] or [`after_secs_rfc3339`] to generate the value.
    /// If the time is in the past the server fires immediately.
    At(String),

    /// Fire at a fixed **interval**, e.g. `Every("5m".to_string())`.
    ///
    /// The string uses Go's `time.ParseDuration` format
    /// (`s`, `m`, `h` suffixes). Minimum interval is `1s`.
    Every(String),

    /// A **six-field cron expression**: `seconds minutes hours day-of-month month day-of-week`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// // Weekdays at 09:30 UTC:
    /// Schedule::Cron("0 30 9 * * 1-5".to_string())
    /// // Every 15 seconds:
    /// Schedule::Cron("*/15 * * * * *".to_string())
    /// ```
    ///
    /// Names for month/day-of-week (first 3 letters, case-insensitive) are
    /// accepted. Step values (`/`) and ranges (`-`) work as in standard cron.
    Cron(String),

    /// `@hourly` — fire at the start of every hour.
    Hourly,
    /// `@daily` (aka `@midnight`) — fire at midnight UTC every day.
    Daily,
    /// `@weekly` — fire at midnight UTC every Sunday.
    Weekly,
    /// `@monthly` — fire at midnight UTC on the first of every month.
    Monthly,
    /// `@yearly` (aka `@annually`) — fire at midnight UTC on January 1st.
    Yearly,
}

impl Schedule {
    /// Returns the value to set in the [`HEADER_SCHEDULE`] header.
    pub fn to_header_value(&self) -> String {
        match self {
            Schedule::At(ts) => format!("@at {ts}"),
            Schedule::Every(interval) => format!("@every {interval}"),
            Schedule::Cron(expr) => expr.clone(),
            Schedule::Hourly => "@hourly".to_string(),
            Schedule::Daily => "@daily".to_string(),
            Schedule::Weekly => "@weekly".to_string(),
            Schedule::Monthly => "@monthly".to_string(),
            Schedule::Yearly => "@yearly".to_string(),
        }
    }
}

// ── Schedule specification ─────────────────────────────────────────

/// Full specification for a schedule publish.
///
/// Build one, call [`to_headers`][ScheduleSpec::to_headers], and publish the
/// resulting `Headers` together with the message body:
///
/// ```rust,ignore
/// let spec = ScheduleSpec {
///     schedule: Some(Schedule::Every("10m".to_string())),
///     target: "jobs.poll".to_string(),
///     ..Default::default()
/// };
/// js.publish_with_headers("schedules.poll", &spec.to_headers(), b"").await?;
/// ```
#[derive(Debug, Clone, Default)]
pub struct ScheduleSpec {
    /// The schedule expression. **Required** — omitting it means no
    /// `Nats-Schedule` header is set and the server will not treat the
    /// message as a schedule.
    pub schedule: Option<Schedule>,

    /// Subject the server publishes the generated message to. **Required.**
    /// Must be covered by the stream's subject list.
    pub target: String,

    /// When set, the server re-emits the last message on this subject as the
    /// body (subject sampling). Wildcards are not supported.
    pub source: Option<String>,

    /// Duration string (e.g. `"5m"`) stamped as `Nats-TTL` on generated
    /// messages. Requires `StreamConfig::allow_msg_ttl` on the stream.
    pub ttl: Option<String>,

    /// IANA time-zone name for cron schedules (e.g. `"Europe/Helsinki"`).
    /// Must not be combined with `Schedule::At` or `Schedule::Every`.
    pub time_zone: Option<String>,

    /// When `true`, the server applies a per-subject rollup on each generated
    /// message (`Nats-Schedule-Rollup: sub`).
    pub rollup: bool,
}

impl ScheduleSpec {
    /// Build a [`Headers`] map containing all schedule headers defined in
    /// this spec. Pass the result to `Client::publish_with_headers` or
    /// `JetStream::publish_with_headers`.
    pub fn to_headers(&self) -> Headers {
        let mut h = Headers::new();
        if let Some(ref sched) = self.schedule {
            h.insert(HEADER_SCHEDULE, sched.to_header_value());
        }
        if !self.target.is_empty() {
            h.insert(HEADER_SCHEDULE_TARGET, self.target.clone());
        }
        if let Some(ref src) = self.source {
            h.insert(HEADER_SCHEDULE_SOURCE, src.clone());
        }
        if let Some(ref ttl) = self.ttl {
            h.insert(HEADER_SCHEDULE_TTL, ttl.clone());
        }
        if let Some(ref tz) = self.time_zone {
            h.insert(HEADER_SCHEDULE_TIME_ZONE, tz.clone());
        }
        if self.rollup {
            h.insert(HEADER_SCHEDULE_ROLLUP, "sub");
        }
        h
    }
}

// ── RFC 3339 helpers ───────────────────────────────────────────────

/// Returns the current wall-clock time as an RFC 3339 UTC string
/// (e.g. `"2026-05-04T10:30:00.000000000Z"`).
///
/// Suitable for use with [`Schedule::At`] for an "immediately" one-shot or
/// as a base to add a duration manually.
pub fn now_rfc3339() -> String {
    let dt = wasip3::clocks::system_clock::now();
    format_rfc3339(dt.seconds.max(0) as u64, dt.nanoseconds)
}

/// Returns the wall-clock time `delta_secs` seconds from now, formatted as
/// an RFC 3339 UTC string.  Useful for one-shot delayed publishes.
///
/// ```rust,ignore
/// // Fire in 10 minutes:
/// let at = after_secs_rfc3339(600);
/// let spec = ScheduleSpec {
///     schedule: Some(Schedule::At(at)),
///     target: "tasks.run".to_string(),
///     ..Default::default()
/// };
/// ```
pub fn after_secs_rfc3339(delta_secs: u64) -> String {
    let dt = wasip3::clocks::system_clock::now();
    // seconds is i64; saturate at 0 rather than panic on negative clock readings.
    let base = dt.seconds.max(0) as u64;
    format_rfc3339(base + delta_secs, dt.nanoseconds)
}

/// Format a Unix timestamp (seconds since epoch + subsecond nanoseconds) as
/// an RFC 3339 UTC string.
///
/// `unix_secs` is treated as a signed value (negative = pre-1970); values
/// that produce dates before year 0000 or after year 9999 are clamped to
/// the valid RFC 3339 range.
pub fn format_rfc3339(unix_secs: u64, nanoseconds: u32) -> String {
    let (year, month, day) = days_to_ymd((unix_secs / 86_400) as i64);
    let rem = unix_secs % 86_400;
    let hour = rem / 3_600;
    let min = (rem % 3_600) / 60;
    let sec = rem % 60;
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
        year, month, day, hour, min, sec, nanoseconds
    )
}

/// Convert a count of days since the Unix epoch (1970-01-01 = day 0) to
/// `(year, month, day)` using Howard Hinnant's civil-from-days algorithm.
fn days_to_ymd(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // day of era [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // year of era [0, 399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month part [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y = if m <= 2 { yoe + era * 400 + 1 } else { yoe + era * 400 };
    (y as i32, m as u32, d as u32)
}
