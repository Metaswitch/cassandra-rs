use crate::cassandra::error::*;
use crate::cassandra::future::CassFuture;
use crate::cassandra::policy::retry::RetryPolicy;
use crate::cassandra::session::Session;
use crate::cassandra::ssl::Ssl;
use crate::cassandra::time::TimestampGen;
use crate::cassandra::util::{Protected, ProtectedInner};

use crate::cassandra_sys::cass_cluster_free;
use crate::cassandra_sys::cass_cluster_new;
use crate::cassandra_sys::cass_cluster_set_connect_timeout;
use crate::cassandra_sys::cass_cluster_set_connection_heartbeat_interval;
use crate::cassandra_sys::cass_cluster_set_connection_idle_timeout;
use crate::cassandra_sys::cass_cluster_set_contact_points_n;
use crate::cassandra_sys::cass_cluster_set_core_connections_per_host;
use crate::cassandra_sys::cass_cluster_set_credentials_n;
use crate::cassandra_sys::cass_cluster_set_latency_aware_routing;
use crate::cassandra_sys::cass_cluster_set_latency_aware_routing_settings;
use crate::cassandra_sys::cass_cluster_set_load_balance_dc_aware_n;
use crate::cassandra_sys::cass_cluster_set_load_balance_round_robin;
use crate::cassandra_sys::cass_cluster_set_local_address_n;
use crate::cassandra_sys::cass_cluster_set_max_concurrent_creation;
use crate::cassandra_sys::cass_cluster_set_max_concurrent_requests_threshold;
use crate::cassandra_sys::cass_cluster_set_max_connections_per_host;
use crate::cassandra_sys::cass_cluster_set_max_requests_per_flush;
use crate::cassandra_sys::cass_cluster_set_num_threads_io;
use crate::cassandra_sys::cass_cluster_set_pending_requests_high_water_mark;
use crate::cassandra_sys::cass_cluster_set_pending_requests_low_water_mark;
use crate::cassandra_sys::cass_cluster_set_port;
use crate::cassandra_sys::cass_cluster_set_protocol_version;
use crate::cassandra_sys::cass_cluster_set_queue_size_event;
use crate::cassandra_sys::cass_cluster_set_queue_size_io;
use crate::cassandra_sys::cass_cluster_set_reconnect_wait_time;
use crate::cassandra_sys::cass_cluster_set_request_timeout;
use crate::cassandra_sys::cass_cluster_set_retry_policy;
use crate::cassandra_sys::cass_cluster_set_ssl;
use crate::cassandra_sys::cass_cluster_set_tcp_keepalive;
use crate::cassandra_sys::cass_cluster_set_tcp_nodelay;
use crate::cassandra_sys::cass_cluster_set_timestamp_gen;
use crate::cassandra_sys::cass_cluster_set_token_aware_routing;
use crate::cassandra_sys::cass_cluster_set_token_aware_routing_shuffle_replicas;
use crate::cassandra_sys::cass_cluster_set_use_schema;
use crate::cassandra_sys::cass_cluster_set_whitelist_filtering;
use crate::cassandra_sys::cass_cluster_set_write_bytes_high_water_mark;
use crate::cassandra_sys::cass_cluster_set_write_bytes_low_water_mark;
use crate::cassandra_sys::cass_false;
use crate::cassandra_sys::cass_future_error_code;
use crate::cassandra_sys::cass_session_connect;
use crate::cassandra_sys::cass_session_connect_keyspace_n;
use crate::cassandra_sys::cass_session_new;
use crate::cassandra_sys::cass_true;
use crate::cassandra_sys::CassCluster as _Cluster;

use std::ffi::CStr;
use std::ffi::NulError;
use std::fmt;
use std::fmt::Display;
use std::iter::Map;
use std::net::AddrParseError;
use std::net::Ipv4Addr;
use std::os::raw::c_char;
use std::result;
use std::str::FromStr;
use time::Duration;

/// A CQL protocol version is just an integer.
pub type CqlProtocol = i32;

///
/// The main class to use when interacting with a Cassandra cluster.
/// Typically, one instance of this class will be created for each separate
/// Cassandra cluster that your application interacts with.
///
/// # Examples
/// ```
/// # async fn test() {
/// use cassandra_cpp::Cluster;
/// let mut cluster = Cluster::default();
/// cluster.set_contact_points("127.0.0.1").unwrap();
/// let session = cluster.connect().await.unwrap();
/// # }
/// ```
#[derive(Debug)]
pub struct Cluster(pub *mut _Cluster);

// The underlying C type has no thread-local state, but does not support access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
unsafe impl Send for Cluster {}

impl Drop for Cluster {
    /// Frees a cluster instance.
    fn drop(&mut self) {
        unsafe { cass_cluster_free(self.0) }
    }
}

impl ProtectedInner<*mut _Cluster> for Cluster {
    fn inner(&self) -> *mut _Cluster {
        self.0
    }
}

impl Protected<*mut _Cluster> for Cluster {
    fn build(inner: *mut _Cluster) -> Self {
        if inner.is_null() {
            panic!("Unexpected null pointer")
        };
        Cluster(inner)
    }
}

impl Default for Cluster {
    /// Creates a new cluster
    fn default() -> Cluster {
        unsafe { Cluster(cass_cluster_new()) }
    }
}

impl Cluster {
    /// Sets/Appends contact points. This *MUST* be set. The first call sets
    /// the contact points and any subsequent calls appends additional contact
    /// points. Passing an empty string will clear the contact points. White space
    /// is stripped from the contact points.
    ///
    ///
    /// {contact points: "127.0.0.1" "127.0.0.1,127.0.0.2", "server1.domain.com"}
    ///
    ///
    pub fn set_contact_points(&mut self, contact_points: &str) -> Result<&mut Self> {
        unsafe {
            let cp_ptr = contact_points.as_ptr() as *const c_char;
            let err = cass_cluster_set_contact_points_n(self.0, cp_ptr, contact_points.len());
            err.to_result(self)
        }
    }

    /// Sets the local address to bind when connecting to the cluster,
    /// if desired.
    ///
    /// Only numeric addresses are supported.
    pub fn set_local_address(&mut self, name: &str) -> Result<&mut Self> {
        unsafe {
            let name_ptr = name.as_ptr() as *const c_char;
            let err = cass_cluster_set_local_address_n(self.0, name_ptr, name.len());
            err.to_result(self)
        }
    }

    /// Sets the port
    ///
    ///
    /// Default: 9042
    ///
    pub fn set_port(&mut self, port: u16) -> Result<&mut Self> {
        unsafe { cass_cluster_set_port(self.0, port as i32).to_result(self) }
    }

    /// Sets the SSL context and enables SSL
    pub fn set_ssl(&mut self, ssl: Ssl) -> &Self {
        unsafe {
            cass_cluster_set_ssl(self.0, ssl.inner());
            self
        }
    }

    /// Connects to the cassandra cluster
    pub async fn connect(&mut self) -> Result<Session> {
        let session = Session::new();
        let connect_future = {
            let connect = unsafe { cass_session_connect(session.inner(), self.0) };
            CassFuture::build(session, connect)
        };
        connect_future.await
    }

    /// Connects to the cassandra cluster, setting the keyspace of the session.
    pub async fn connect_keyspace(&mut self, keyspace: &str) -> Result<Session> {
        let session = Session::new();
        let keyspace_ptr = keyspace.as_ptr() as *const c_char;
        let connect_keyspace = unsafe {
            cass_session_connect_keyspace_n(
                session.inner(),
                self.inner(),
                keyspace_ptr,
                keyspace.len(),
            )
        };
        let connect_future = CassFuture::build(session, connect_keyspace);
        connect_future.await
    }

    /// Sets the protocol version. This will automatically downgrade to the lowest
    /// supported protocol version.
    ///
    ///
    /// Default: version 4
    ///
    pub fn set_protocol_version(&mut self, protocol_version: CqlProtocol) -> Result<&mut Self> {
        unsafe {
            cass_cluster_set_protocol_version(self.0, protocol_version as i32).to_result(self)
        }
    }

    /// Sets the number of IO threads. This is the number of threads
    /// that will handle query requests.
    ///
    ///
    /// Default: 1
    ///
    pub fn set_num_threads_io(&mut self, num_threads: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_num_threads_io(self.0, num_threads).to_result(self) }
    }

    /// Sets the size of the fixed size queue that stores pending requests.
    ///
    ///
    /// Default: 8192
    ///
    pub fn set_queue_size_io(&mut self, queue_size: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_queue_size_io(self.0, queue_size).to_result(self) }
    }

    /// Sets the size of the fixed size queue that stores events.
    ///
    ///
    /// Default: 8192
    ///
    pub fn set_queue_size_event(&mut self, queue_size: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_queue_size_event(self.0, queue_size).to_result(self) }
    }

    /// Sets the number of connections made to each server in each
    /// IO thread.
    ///
    ///
    /// Default: 1
    ///
    pub fn set_core_connections_per_host(&mut self, num_connections: u32) -> Result<&mut Self> {
        unsafe {
            cass_cluster_set_core_connections_per_host(self.0, num_connections).to_result(self)
        }
    }

    /// Sets the maximum number of connections made to each server in each
    /// IO thread.
    ///
    ///
    /// Default: 2
    ///
    pub fn set_max_connections_per_host(&mut self, num_connections: u32) -> Result<&mut Self> {
        unsafe {
            cass_cluster_set_max_connections_per_host(self.0, num_connections).to_result(self)
        }
    }

    /// Sets the amount of time to wait before attempting to reconnect.
    ///
    ///
    /// Default: 1000ms
    ///
    pub fn set_reconnect_wait_time(&mut self, wait_time: u32) -> &Self {
        unsafe {
            cass_cluster_set_reconnect_wait_time(self.0, wait_time);
        }
        self
    }

    /// Sets the maximum number of connections that will be created concurrently.
    /// Connections are created when the current connections are unable to keep up with
    /// request throughput.
    ///
    ///
    /// Default: 1
    pub fn set_max_concurrent_creation(&mut self, num_connections: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_max_concurrent_creation(self.0, num_connections).to_result(self) }
    }

    /// Sets the threshold for the maximum number of concurrent requests in-flight
    /// on a connection before creating a new connection. The number of new connections
    /// created will not exceed max_connections_per_host.
    ///
    ///
    /// Default: 100
    pub fn set_max_concurrent_requests_threshold(
        &mut self,
        num_requests: u32,
    ) -> Result<&mut Self> {
        unsafe {
            cass_cluster_set_max_concurrent_requests_threshold(self.0, num_requests).to_result(self)
        }
    }

    /// Sets the maximum number of requests processed by an IO worker
    /// per flush.
    ///
    ///
    /// Default: 128
    pub fn set_max_requests_per_flush(&mut self, num_requests: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_max_requests_per_flush(self.0, num_requests).to_result(self) }
    }

    /// Sets the high water mark for the number of bytes outstanding
    /// on a connection. Disables writes to a connection if the number
    /// of bytes queued exceed this value.
    ///
    ///
    /// Default: 64KB
    pub fn set_write_bytes_high_water_mark(&mut self, num_bytes: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_write_bytes_high_water_mark(self.0, num_bytes).to_result(self) }
    }

    /// Sets the low water mark for the number of bytes outstanding
    /// on a connection. Disables writes to a connection if the number
    /// of bytes queued fall below this value.
    ///
    ///
    /// Default: 32KB
    pub fn set_write_bytes_low_water_mark(&mut self, num_bytes: u32) -> Result<&mut Self> {
        unsafe { cass_cluster_set_write_bytes_low_water_mark(self.0, num_bytes).to_result(self) }
    }

    /// Sets the high water mark for the number of requests queued waiting
    /// for a connection in a connection pool. Disables writes to a
    /// host on an IO worker if the number of requests queued exceed this
    /// value.
    ///
    ///
    /// Default: 256
    pub fn set_pending_requests_high_water_mark(&mut self, num_requests: u32) -> Result<&mut Self> {
        unsafe {
            cass_cluster_set_pending_requests_high_water_mark(self.0, num_requests).to_result(self)
        }
    }

    /// Sets the low water mark for the number of requests queued waiting
    /// for a connection in a connection pool. After exceeding high water mark
    /// requests, writes to a host will only resume once the number of requests
    /// fall below this value.
    ///
    ///
    /// Default: 128
    pub fn set_pending_requests_low_water_mark(&mut self, num_requests: u32) -> Result<&mut Self> {
        unsafe {
            cass_cluster_set_pending_requests_low_water_mark(self.0, num_requests).to_result(self)
        }
    }

    /// Sets the timeout for connecting to a node.
    ///
    ///
    /// Default: 5000ms
    pub fn set_connect_timeout(&mut self, timeout: Duration) -> &Self {
        unsafe {
            cass_cluster_set_connect_timeout(self.0, timeout.whole_milliseconds() as u32);
        }
        self
    }

    /// Sets the timeout for waiting for a response from a node.
    ///
    ///
    /// Default: 12000ms
    pub fn set_request_timeout(&mut self, timeout: Duration) -> &Self {
        unsafe {
            cass_cluster_set_request_timeout(self.0, timeout.whole_milliseconds() as u32);
        }
        self
    }

    /// Sets credentials for plain text authentication.
    pub fn set_credentials(&mut self, username: &str, password: &str) -> Result<&mut Self> {
        unsafe {
            let username_ptr = username.as_ptr() as *const c_char;
            let password_ptr = password.as_ptr() as *const c_char;
            cass_cluster_set_credentials_n(
                self.0,
                username_ptr,
                username.len(),
                password_ptr,
                password.len(),
            );
        }
        Ok(self)
    }

    /// Configures the cluster to use round-robin load balancing.
    ///
    /// The driver discovers all nodes in a cluster and cycles through
    /// them per request. All are considered 'local'.
    pub fn set_load_balance_round_robin(&mut self) -> &Self {
        unsafe {
            cass_cluster_set_load_balance_round_robin(self.0);
            self
        }
    }

    /// Configures the cluster to use DC-aware load balancing.
    /// For each query, all live nodes in a primary 'local' DC are tried first,
    /// followed by any node from other DCs.
    ///
    /// <b>Note:</b> This is the default, and does not need to be called unless
    /// switching an existing from another policy or changing settings.
    /// Without further configuration, a default local_dc is chosen from the
    /// first connected contact point, and no remote hosts are considered in
    /// query plans. If relying on this mechanism, be sure to use only contact
    /// points from the local DC.
    pub fn set_load_balance_dc_aware<S>(
        &mut self,
        local_dc: &str,
        used_hosts_per_remote_dc: u32,
        allow_remote_dcs_for_local_cl: bool,
    ) -> Result<&mut Self> {
        unsafe {
            {
                let local_dc_ptr = local_dc.as_ptr() as *const c_char;
                cass_cluster_set_load_balance_dc_aware_n(
                    self.0,
                    local_dc_ptr,
                    local_dc.len(),
                    used_hosts_per_remote_dc,
                    if allow_remote_dcs_for_local_cl {
                        cass_true
                    } else {
                        cass_false
                    },
                )
            }
            .to_result(self)
        }
    }

    /// Configures the cluster to use token-aware request routing or not.
    ///
    /// <b>Important:</b> Token-aware routing depends on keyspace information.
    /// For this reason enabling token-aware routing will also enable the usage
    /// of schema metadata.
    ///
    ///
    /// Default: true (enabled).
    ///
    ///
    /// This routing policy composes the base routing policy, routing
    /// requests first to replicas on nodes considered 'local' by
    /// the base load balancing policy.
    pub fn set_token_aware_routing(&mut self, enabled: bool) -> &Self {
        unsafe {
            cass_cluster_set_token_aware_routing(
                self.0,
                if enabled { cass_true } else { cass_false },
            );
        }
        self
    }

    /// Configures token-aware routing to randomly shuffle replicas.
    /// This can reduce the effectiveness of server-side caching, but it
    /// can better distribute load over replicas for a given partition key.
    ///
    ///
    /// Default: true (enabled)
    ///
    ///
    /// Note: Token-aware routing must be enabled for the setting to be
    /// applicable.
    pub fn set_token_aware_routing_shuffle_replicas(&mut self, enabled: bool) -> &Self {
        unsafe {
            cass_cluster_set_token_aware_routing_shuffle_replicas(
                self.0,
                if enabled { cass_true } else { cass_false },
            );
        }
        self
    }

    /// Configures the cluster to use latency-aware request routing or not.
    ///
    ///
    /// Default: false (disabled).
    ///
    ///
    /// This routing policy is a top-level routing policy. It uses the
    /// base routing policy to determine locality (dc-aware) and/or
    /// placement (token-aware) before considering the latency.
    pub fn set_latency_aware_routing(&mut self, enabled: bool) -> &Self {
        unsafe {
            cass_cluster_set_latency_aware_routing(
                self.0,
                if enabled { cass_true } else { cass_false },
            );
        }
        self
    }

    /// Configures the settings for latency-aware request routing.
    ///
    ///
    /// Defaults:
    ///
    /// <ul>
    ///   <li>exclusion_threshold: 2.0</li>
    ///   <li>scale_ms: 100 milliseconds</li>
    ///  <li>retry_period_ms: 10,000 milliseconds (10 seconds)</li>
    ///  <li>update_rate_ms: 100 milliseconds</li>
    ///  <li>min_measured: 50</li>
    /// </ul>
    pub fn set_latency_aware_routing_settings(
        &mut self,
        exclusion_threshold: f64,
        scale: Duration,
        retry_period: Duration,
        update_rate: Duration,
        min_measured: u64,
    ) -> &Self {
        unsafe {
            cass_cluster_set_latency_aware_routing_settings(
                self.0,
                exclusion_threshold,
                scale.whole_milliseconds() as u64,
                retry_period.whole_milliseconds() as u64,
                update_rate.whole_milliseconds() as u64,
                min_measured,
            );
        }
        self
    }

    /// /Sets/Appends whitelist hosts. The first call sets the whitelist hosts and
    /// any subsequent calls appends additional hosts. Passing an empty string will
    /// clear and disable the whitelist. White space is striped from the hosts.
    ///
    /// This policy filters requests to all other policies, only allowing requests
    /// to the hosts contained in the whitelist. Any host not in the whitelist will
    /// be ignored and a connection will not be established. This policy is useful
    /// for ensuring that the driver will only connect to a predefined set of hosts.
    ///
    ///
    /// Examples: "127.0.0.1" "127.0.0.1,127.0.0.2", "server1.domain.com"
    pub fn set_whitelist_filtering(&mut self, hosts: Vec<String>) -> &Self {
        unsafe {
            cass_cluster_set_whitelist_filtering(self.0, hosts.join(",").as_ptr() as *const i8);
        }
        self
    }

    /// Enable/Disable Nagel's algorithm on connections.
    ///
    ///
    /// <b>Default:</b> true (disables Nagel's algorithm).
    pub fn set_tcp_nodelay(&mut self, enable: bool) -> &Self {
        unsafe {
            cass_cluster_set_tcp_nodelay(self.0, if enable { cass_true } else { cass_false });
        }
        self
    }

    /// Enable/Disable TCP keep-alive
    ///
    ///
    /// Default: false (disabled).
    pub fn set_tcp_keepalive(&mut self, enable: bool, delay: Duration) -> &Self {
        unsafe {
            cass_cluster_set_tcp_keepalive(
                self.0,
                if enable { cass_true } else { cass_false },
                delay.whole_seconds() as u32,
            );
        }
        self
    }

    /// Sets the timestamp generator used to assign timestamps to all requests
    /// unless overridden by setting the timestamp on a statement or a batch.
    ///
    ///
    /// Default: server-side timestamp generator.
    pub fn set_timestamp_gen(&mut self, tsg: &TimestampGen) -> &mut Self {
        unsafe {
            cass_cluster_set_timestamp_gen(self.0, TimestampGen::inner(tsg));
            self
        }
    }

    /// Sets the amount of time between heartbeat messages and controls the amount
    /// of time the connection must be idle before sending heartbeat messages. This
    /// is useful for preventing intermediate network devices from dropping
    /// connections.
    ///
    ///
    /// Default: 30 seconds
    pub fn set_connection_heartbeat_interval(&mut self, hearbeat: Duration) -> &mut Self {
        unsafe {
            cass_cluster_set_connection_heartbeat_interval(self.0, hearbeat.whole_seconds() as u32);
            self
        }
    }

    /// Sets the amount of time a connection is allowed to be without a successful
    /// heartbeat response before being terminated and scheduled for reconnection.
    ///
    ///
    /// Default: 60 seconds
    pub fn set_connection_idle_timeout(&mut self, timeout: Duration) -> &mut Self {
        unsafe {
            cass_cluster_set_connection_idle_timeout(self.0, timeout.whole_seconds() as u32);
            self
        }
    }

    /// Sets the retry policy used for all requests unless overridden by setting
    /// a retry policy on a statement or a batch.
    ///
    ///
    /// Default: The same policy as would be created by the function:
    /// cass_retry_policy_default_new(). This policy will retry on a read timeout
    /// if there was enough replicas, but no data present, on a write timeout if a
    /// logged batch request failed to write the batch log, and on a unavailable
    /// error it retries using a new host. In all other cases the default policy
    /// will return an error.
    pub fn set_retry_policy(&mut self, retry_policy: RetryPolicy) -> &mut Self {
        unsafe {
            cass_cluster_set_retry_policy(self.0, retry_policy.inner());
            self
        }
    }

    /// Enable/Disable retrieving and updating schema metadata. If disabled
    /// this is allows the driver to skip over retrieving and updating schema
    /// metadata, but it also disables the usage of token-aware routing and
    /// cass_session_get_schema() will always return an empty object. This can be
    /// useful for reducing the startup overhead of short-lived sessions.
    ///
    ///
    /// Default: true (enabled).
    pub fn set_use_schema(&mut self, enabled: bool) -> &Self {
        unsafe {
            cass_cluster_set_use_schema(self.0, if enabled { cass_true } else { cass_false });
        }
        self
    }
}
