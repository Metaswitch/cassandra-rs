use crate::cassandra::error::*;
use crate::cassandra::prepared::PreparedStatement;
use crate::cassandra::result::CassResult;
use crate::cassandra::util::{Protected, ProtectedWithSession};
use crate::cassandra::write_type::WriteType;
use crate::cassandra_sys::cass_future_error_code;
use crate::cassandra_sys::cass_future_error_message;
use crate::cassandra_sys::cass_future_free;
use crate::cassandra_sys::cass_future_get_error_result;
use crate::cassandra_sys::cass_future_get_prepared;
use crate::cassandra_sys::cass_future_get_result;
use crate::cassandra_sys::cass_future_ready;
use crate::cassandra_sys::cass_future_set_callback;
use crate::cassandra_sys::CassError_;
use crate::cassandra_sys::CassFuture as _Future;
use crate::cassandra_sys::CASS_OK;
use crate::cassandra_sys::{cass_false, cass_true};
use crate::{cassandra::consistency::Consistency, Session};

use parking_lot::Mutex;

use std::future::Future;
use std::marker::PhantomData;
use std::mem;
use std::pin::Pin;
use std::slice;
use std::str;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// A future representing the result of a Cassandra driver operation.
///
/// On success, returns a result of type `T`. On failure, returns a Cassandra error.
///
/// When constructing this take care to supply the correct type argument, since it will
/// be used to control how the result is extracted from the underlying Cassandra
/// driver future (see `Completable`).
#[must_use]
#[derive(Debug)]
pub(crate) struct CassFuture<T> {
    /// The underlying Cassandra driver future object.
    inner: *mut _Future,

    /// The current state of this future.
    state: Arc<FutureTarget>,

    /// The session the future is being executed upon.
    session: Option<Session>,

    /// Treat as if it contains a T.
    phantom: PhantomData<T>,
}

// The underlying C type has no thread-local state, and explicitly supports access
// from multiple threads: https://datastax.github.io/cpp-driver/topics/#thread-safety
//
// But it can be used to move a value of type `T`, so `T` needs to be `Send` if the
// future is.
//
// The same is not true for `Sync`, because a future doesn't give multiple threads
// concurrent access to the value (you can only poll a future once).
unsafe impl<T> Sync for CassFuture<T> {}
unsafe impl<T> Send for CassFuture<T> where T: Send {}
impl<T> Unpin for CassFuture<T> {}

impl<T> CassFuture<T> {
    /// Wrap a Cassandra driver future to make it a proper Rust future.
    ///
    /// When invoking this take care to supply the correct type argument, since it will
    /// be used to control how the result is extracted from the underlying Cassandra
    /// driver future (see `Completable`).
    pub(crate) fn build(session: Session, inner: *mut _Future) -> Self {
        CassFuture {
            inner,
            session: Some(session),
            state: Arc::new(FutureTarget {
                inner: Mutex::new(FutureState::Created),
            }),
            phantom: PhantomData,
        }
    }

    fn take_session(&mut self) -> Session {
        self.session.take().expect(
            "invariant: could not take session from CassFuture that already has had session taken.",
        )
    }
}

impl<T> Drop for CassFuture<T> {
    /// Drop this CassFuture.
    ///
    /// This also drops its reference to the FutureTarget, but if
    /// we're waiting to be called back the FutureState::Awaiting holds another reference to
    /// the target, which keeps it alive until the callback fires.
    fn drop(&mut self) {
        unsafe { cass_future_free(self.inner) };
    }
}

/// A type is Completable if it can be returned from a Cassandra driver future.
/// You should only use this if you reasonably expect that a particular future will
/// have such a result; for `CassFuture`s we ensure this by construction.
pub trait Completable
where
    Self: Sized,
{
    /// Extract the result from the future, if present.
    unsafe fn get(session: Session, inner: *mut _Future) -> Option<Self>;
}

/// Futures that complete with no value, or report an error.
impl Completable for () {
    unsafe fn get(_session: Session, _inner: *mut _Future) -> Option<Self> {
        Some(())
    }
}

/// Futures that completes with just the session.
impl Completable for Session {
    unsafe fn get(session: Session, _inner: *mut _Future) -> Option<Self> {
        Some(session)
    }
}

/// The mainline case - a CassResult.
impl Completable for CassResult {
    unsafe fn get(_session: Session, inner: *mut _Future) -> Option<Self> {
        cass_future_get_result(inner)
            .as_ref()
            .map(|r| CassResult::build(r as *const _))
    }
}

/// Futures that complete with a prepared statement.
impl Completable for PreparedStatement {
    unsafe fn get(session: Session, inner: *mut _Future) -> Option<Self> {
        cass_future_get_prepared(inner)
            .as_ref()
            .map(|r| PreparedStatement::build(r as *const _, session))
    }
}

/// A Cassandra future is a normal Rust future.
impl<T: Completable> Future for CassFuture<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut install_callback = false;
        let ret = {
            // Perform the following computations under the lock, and then release it.
            //
            // We must take care to avoid deadlock. The lock hierarchy is: take the Rust lock
            // (self.state.inner) first, then take the C++ lock (internal to the C++
            // implementation of futures).
            //
            // Poll is always called by the Rust event loop, never from within C++ code or
            // from notify_task. `self.ready()` and `self.get_completion()` take an internal
            // mutex within C++ code, but they never call back to Rust and so cannot violate
            // the lock hierarchy. However `self.set_callback()` calls into Rust code
            // (`notify_task`) if the future is already complete, so we must avoid holding the
            // Rust lock while calling it. We achieve this by using a boolean flag to request
            // the callback be set outside the lock region.
            let mut lock = self.state.as_ref().inner.lock();
            match *lock {
                ref mut state @ FutureState::Created => {
                    // No task yet - schedule a callback. But as an optimization, if it's ready
                    // already, complete now without scheduling a callback.
                    if unsafe { cass_future_ready(self.inner) } == cass_true {
                        // Future is ready; wrap success in `Ok(Ready)` or report failure as `Err`.
                        Poll::Ready(self.inner)
                    } else {
                        // Future is not ready; park this task and arrange to be called back when
                        // it is.
                        *state = FutureState::Awaiting {
                            waker: cx.waker().clone(),
                            keep_alive: self.state.clone(),
                        };
                        install_callback = true;
                        Poll::Pending
                    }
                }
                FutureState::Awaiting { ref mut waker, .. } => {
                    // Callback already scheduled; don't set it again (C doesn't support it anyway),
                    // but be sure to swizzle the new task into place. No need to check for
                    // readiness here; we have to wait for the callback anyway so we might as well
                    // do all the work in one place.
                    if !waker.will_wake(cx.waker()) {
                        *waker = cx.waker().clone();
                    }
                    Poll::Pending
                }
                FutureState::Ready => {
                    // Future has been marked ready by callback. Safe to return now.
                    Poll::Ready(self.inner)
                }
            }
        };

        if install_callback {
            // Install the callback. If callback cannot be sent, report immediate `Err`.
            let data = (self.state.as_ref() as *const FutureTarget) as *mut ::std::os::raw::c_void;
            unsafe { cass_future_set_callback(self.inner, Some(notify_task), data) }
                .to_result(())?;
        }

        match ret {
            Poll::Pending => Poll::Pending,
            Poll::Ready(inner) => {
                Poll::Ready(unsafe { get_completion(self.take_session(), inner) })
            }
        }
    }
}

/// Extract success or failure from a future.
///
/// This function will block if the future is not yet ready. In order to ensure that this
/// function will not block, you can check if the future is ready prior to calling this using
/// `cass_future_ready`. If the future is ready, this function will not block, otherwise
/// it will block on waiting for the future to become ready.
unsafe fn get_completion<T: Completable>(session: Session, inner: *mut _Future) -> Result<T> {
    // Wrap success in `Ok(Ready)` or report failure as `Err`.
    // This will block if the future is not yet ready.
    let rc = cass_future_error_code(inner);
    match rc {
        CASS_OK => match Completable::get(session, inner) {
            None => Err(CassErrorCode::LIB_NULL_VALUE.to_error()),
            Some(v) => Ok(v),
        },
        _ => Err(get_cass_future_error(rc, inner)),
    }
}

/// The target of a C++ Cassandra driver callback.
///
/// The C++ Cassandra driver calls the callback in the following two scenarios only:
///
/// * When the future is completed, if a callback is set.
/// * When a callback is set, if the future is completed.
///
/// Given a future can only be completed once, and a callback can only be set once, enforced
/// by internal locking, it is clear that we should expect at most one callback to occur.
///
/// The important thing to ensure is that Rust has not freed the target of that callback when
/// it occurs. The simplest way to achieve that is only to free the callback target once the
/// callback has occurred. Since the callback target is owned by the future, and the future is
/// typically freed after completion, that means we must only complete after we receive the
/// callback.
///
/// The FutureTarget is held by a CassFuture, and (in the FutureState::Awaiting state) by
/// a C++ Cassandra driver callback. The latter pointer is represented by one inside that state,
/// so that it is not freed early.
#[derive(Debug)]
struct FutureTarget {
    inner: Mutex<FutureState>,
}

/// The state of a Cassandra future.
///
/// This is an FSM.
#[derive(Debug)]
enum FutureState {
    /// Initial state: the future has been created but no callback has yet been installed.
    Created,

    /// The future has been created and a callback has been installed, invoking this task.
    /// `keep_alive` is an Arc to the enclosing target (i.e., a cycle) which stands for the
    /// pointer held by the C++ Cassandra driver future as the callback target. This prevents
    /// it from being freed early.
    Awaiting {
        waker: Waker,
        keep_alive: Arc<FutureTarget>,
    },

    /// The future has called back to indicate completion.
    Ready,
}

/// Callback which wakes the task waiting on this future.
/// Called by the C++ driver when the future is ready,
/// with a pointer to the `CassFuture`.
unsafe extern "C" fn notify_task(_c_future: *mut _Future, data: *mut ::std::os::raw::c_void) {
    let future_target: &FutureTarget = &*(data as *const FutureTarget);
    // The future is now ready, so transition to the appropriate state.
    let state = {
        let mut lock = future_target.inner.lock();
        mem::replace(&mut *lock, FutureState::Ready)
    };
    if let FutureState::Awaiting { ref waker, .. } = state {
        waker.wake_by_ref();
    } else {
        // This can never happen.
        panic!("Callback invoked before callback set");
    }
}
