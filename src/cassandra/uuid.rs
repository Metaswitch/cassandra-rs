use crate::cassandra::error::*;
use crate::cassandra::util::{Protected, ProtectedInner};

use crate::cassandra_sys::cass_uuid_from_string_n;
use crate::cassandra_sys::cass_uuid_gen_free;
use crate::cassandra_sys::cass_uuid_gen_from_time;
use crate::cassandra_sys::cass_uuid_gen_new;
use crate::cassandra_sys::cass_uuid_gen_new_with_node;
use crate::cassandra_sys::cass_uuid_gen_random;
use crate::cassandra_sys::cass_uuid_gen_time;
use crate::cassandra_sys::cass_uuid_max_from_time;
use crate::cassandra_sys::cass_uuid_min_from_time;
use crate::cassandra_sys::cass_uuid_string;
use crate::cassandra_sys::cass_uuid_timestamp;
use crate::cassandra_sys::cass_uuid_version;
use crate::cassandra_sys::CassUuid as _Uuid;
use crate::cassandra_sys::CassUuidGen as _UuidGen;

use std::cmp::Ordering;
use std::ffi::CStr;
use std::fmt;
use std::fmt::Formatter;
use std::fmt::{Debug, Display};
use std::mem;
use std::os::raw::c_char;
use std::str;

const CASS_UUID_STRING_LENGTH: usize = 37;

#[derive(Copy, Clone)]
/// Version 1 (time-based) or version 4 (random) UUID.
pub struct Uuid(_Uuid);

impl ProtectedInner<_Uuid> for Uuid {
    fn inner(&self) -> _Uuid {
        self.0
    }
}

impl Protected<_Uuid> for Uuid {
    fn build(inner: _Uuid) -> Self {
        Uuid(inner)
    }
}

impl Default for Uuid {
    fn default() -> Uuid {
        unsafe { ::std::mem::zeroed() }
    }
}

/// A UUID generator object.
///
/// Instances of the UUID generator object are thread-safe to generate UUIDs.
#[derive(Debug)]
pub struct UuidGen(*mut _UuidGen);
unsafe impl Sync for UuidGen {}
unsafe impl Send for UuidGen {}

impl Drop for UuidGen {
    fn drop(&mut self) {
        unsafe { cass_uuid_gen_free(self.0) }
    }
}

impl Debug for Uuid {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        unsafe {
            // Allocate an array large enough for cass_uuid_string to write to.
            let mut buf = [0u8; CASS_UUID_STRING_LENGTH];
            cass_uuid_string(self.0, buf.as_mut_ptr() as *mut c_char);
            let str = CStr::from_bytes_with_nul(&buf)
                .map_err(|_| fmt::Error)?
                .to_str()
                .map_err(|_| fmt::Error)?;
            fmt::Display::fmt(&str, f)
        }
    }
}

impl Uuid {
    /// Generates a V1 (time) UUID for the specified time.
    pub fn min_from_time(&mut self, time: u64) {
        unsafe { cass_uuid_min_from_time(time, &mut self.0) }
    }

    /// Sets the UUID to the minimum V1 (time) value for the specified tim
    pub fn max_from_time(&mut self, time: u64) {
        unsafe { cass_uuid_max_from_time(time, &mut self.0) }
    }

    /// Gets the timestamp for a V1 UUID
    pub fn timestamp(&self) -> u64 {
        unsafe { cass_uuid_timestamp(self.0) }
    }

    /// Gets the version for a UUID
    pub fn version(&self) -> u8 {
        unsafe { cass_uuid_version(self.0) }
    }
}

impl From<uuid::Uuid> for Uuid {
    fn from(id: uuid::Uuid) -> Uuid {
        // implementation taken from Datastax C/C++ driver
        // serialization.hpp, encode_uuid()
        let input = id.as_bytes();

        let mut time_and_version = 0u64;
        time_and_version |= input[3] as u64;
        time_and_version |= (input[2] as u64) << 8;
        time_and_version |= (input[1] as u64) << 16;
        time_and_version |= (input[0] as u64) << 24;

        time_and_version |= (input[5] as u64) << 32;
        time_and_version |= (input[4] as u64) << 40;

        time_and_version |= (input[7] as u64) << 48;
        time_and_version |= (input[6] as u64) << 56;

        let mut clock_seq_and_node = 0u64;
        for i in 0..8 {
            clock_seq_and_node |= (input[15 - i] as u64) << (8 * i);
        }
        Uuid(_Uuid {
            time_and_version,
            clock_seq_and_node,
        })
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(id: Uuid) -> uuid::Uuid {
        // implementation taken from Datastax C/C++ driver
        // serialization.hpp decode_uuid()
        let mut output = [0u8; 16];
        output[3] = id.0.time_and_version as u8;
        output[2] = (id.0.time_and_version >> 8) as u8;
        output[1] = (id.0.time_and_version >> 16) as u8;
        output[0] = (id.0.time_and_version >> 24) as u8;

        output[5] = (id.0.time_and_version >> 32) as u8;
        output[4] = (id.0.time_and_version >> 40) as u8;

        output[7] = (id.0.time_and_version >> 48) as u8;
        output[6] = (id.0.time_and_version >> 56) as u8;

        for i in 0..8 {
            output[15 - i] = (id.0.clock_seq_and_node >> (8 * i)) as u8;
        }
        uuid::Uuid::from_bytes(output)
    }
}

impl str::FromStr for Uuid {
    type Err = Error;
    fn from_str(str: &str) -> Result<Uuid> {
        unsafe {
            let mut uuid = mem::zeroed();
            let str_ptr = str.as_ptr() as *const c_char;
            cass_uuid_from_string_n(str_ptr, str.len(), &mut uuid)
                .to_result(())
                .and_then(|_| Ok(Uuid(uuid)))
        }
    }
}

impl PartialEq for Uuid {
    fn eq(&self, other: &Uuid) -> bool {
        self.0.time_and_version == other.0.time_and_version
            && self.0.clock_seq_and_node == other.0.clock_seq_and_node
    }
}

impl Eq for Uuid {}

impl Ord for Uuid {
    fn cmp(&self, other: &Uuid) -> Ordering {
        self.0
            .time_and_version
            .cmp(&other.0.time_and_version)
            .then(self.0.clock_seq_and_node.cmp(&other.0.clock_seq_and_node))
    }
}

impl PartialOrd for Uuid {
    fn partial_cmp(&self, other: &Uuid) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Default for UuidGen {
    /// Creates a new thread-safe UUID generator
    fn default() -> Self {
        unsafe { UuidGen(cass_uuid_gen_new()) }
    }
}

impl UuidGen {
    /// Creates a new UUID generator with custom node information.
    /// <b>Note:</b> This object is thread-safe. It is best practice to create and reuse
    /// a single object per application.
    pub fn new_with_node(node: u64) -> UuidGen {
        unsafe { UuidGen(cass_uuid_gen_new_with_node(node)) }
    }

    /// Generates a V1 (time) UUID.
    pub fn gen_time(&self) -> Uuid {
        unsafe {
            let mut output: _Uuid = mem::zeroed();
            cass_uuid_gen_time(self.0, &mut output);
            Uuid(output)
        }
    }

    /// Generates a new V4 (random) UUID
    pub fn gen_random(&self) -> Uuid {
        unsafe {
            let mut output: _Uuid = mem::zeroed();
            cass_uuid_gen_random(self.0, &mut output);
            Uuid(output)
        }
    }

    /// Generates a V1 (time) UUID for the specified time.
    ///
    /// # Examples
    ///
    /// ```
    /// # use cassandra_cpp::{UuidGen, Uuid};
    /// # #[allow(dead_code)]
    /// # fn example() -> Uuid {
    /// let generator = UuidGen::default();
    /// let uuid = generator.gen_from_time(1457486866742u64);
    /// # uuid
    /// # }
    /// ```
    pub fn gen_from_time(&self, timestamp: u64) -> Uuid {
        unsafe {
            let mut output: _Uuid = mem::zeroed();
            cass_uuid_gen_from_time(self.0, timestamp, &mut output);
            Uuid(output)
        }
    }
}

#[test]
#[allow(unused_variables)]
fn test_uuid_display_gentime() {
    let generator = UuidGen::default();
    let uuid = generator.gen_from_time(1457486866742u64);
    assert_eq!(uuid.timestamp(), 1457486866742u64);
    let uuidstr = format!("{}", uuid); // Test Display trait
}

#[test]
#[allow(unused_variables)]
fn test_uuid_debug_genrand() {
    let generator = UuidGen::default();
    let uuid = generator.gen_random();
    let uuidstr = format!("{:?}", uuid); // Test Debug trait
}
