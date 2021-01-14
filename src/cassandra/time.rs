use crate::cassandra::util::{Protected, ProtectedInner};

use crate::cassandra_sys::cass_time_from_epoch;
use crate::cassandra_sys::cass_timestamp_gen_free;
use crate::cassandra_sys::cass_timestamp_gen_monotonic_new;
use crate::cassandra_sys::cass_timestamp_gen_server_side_new;
use crate::cassandra_sys::CassTimestampGen as _TimestampGen;
// use cassandra_sys::cass_date_from_epoch;
// use cassandra_sys::cass_date_time_to_epoch;
use time::Duration;

/// Generators of client-side, microsecond-precision timestamps.
/// <b>Note:</b> This generator is thread-safe and can be shared by multiple sessions.
#[derive(Debug)]
pub struct TimestampGen(*mut _TimestampGen);
unsafe impl Send for TimestampGen {}
unsafe impl Sync for TimestampGen {}

impl ProtectedInner<*mut _TimestampGen> for TimestampGen {
    fn inner(&self) -> *mut _TimestampGen {
        self.0
    }
}

impl Protected<*mut _TimestampGen> for TimestampGen {
    fn build(inner: *mut _TimestampGen) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        TimestampGen(inner)
    }
}

// ///Cassandra representation of the number of days since epoch
// pub struct Date(u32);

/// Converts a unix timestamp (in seconds) to the Cassandra "time" type. The "time" type
/// represents the number of nanoseconds since midnight (range 0 to 86399999999999).
#[derive(Debug)]
pub struct Time(i64);

impl TimestampGen {
    /// Converts a unix timestamp (in seconds) to the Cassandra "time" type. The "time" type
    /// represents the number of nanoseconds since midnight (range 0 to 86399999999999).
    pub fn time_from_epoch(epoch_seconds: Duration) -> Time {
        unsafe { Time(cass_time_from_epoch(epoch_seconds.whole_seconds())) }
    }

    /// Creates a new monotonically increasing timestamp generator. This generates
    /// microsecond timestamps with the sub-millisecond part generated using a counter.
    /// The implementation guarantees that no more than 1000 timestamps will be generated
    /// for a given clock tick even if shared by multiple session objects. If that rate is
    /// exceeded then a warning is logged and timestamps stop incrementing until the next
    /// clock tick.
    pub fn gen_monotonic_new() -> Self {
        unsafe { TimestampGen(cass_timestamp_gen_monotonic_new()) }
    }

    /// Creates a new server-side timestamp generator. This generator allows Cassandra
    /// to assign timestamps server-side.
    ///
    /// <b>Note:</b> This is the default timestamp generator.
    pub fn gen_server_side_new() -> Self {
        unsafe { TimestampGen(cass_timestamp_gen_server_side_new()) }
    }

    //    pub fn from_epoch() -> Self {
    //        unsafe { TimestampGen(cass_timestamp_gen_monotonic_new()) }
    //    }
}

// Converts a unix timestamp (in seconds) to the Cassandra "date" type. The "date" type
// represents the number of days since the Epoch (1970-01-01) with the Epoch centered at
// the value 2^31.
// fn date_from_epoch(epoch_secs: Duration) -> Date {
//    unsafe { Date(cass_date_from_epoch(epoch_secs.num_days())) }
// }

// Combines the Cassandra "date" and "time" types to Epoch time in seconds.
// fn date_time_to_epoch(date: Date, time: Time) -> Duration {
//   unsafe { Duration::seconds(cass_date_time_to_epoch(date.0, time.0)) }
// }

impl Drop for TimestampGen {
    fn drop(&mut self) {
        unsafe { cass_timestamp_gen_free(self.0) }
    }
}
