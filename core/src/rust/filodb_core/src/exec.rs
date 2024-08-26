//! Helpers for executing code in a JNI method

use jni::{sys::jobject, JNIEnv};

use crate::errors::JavaResult;

/// Execs a function in a JNI context, supplying an environment
/// and translating any errors to exceptions
///
/// All JNI functions should use this to ensure error handling
/// is properly done
///
/// Do *not* panic in any calls - avoid unwrap, expect, etc.
pub fn jni_exec<F, T>(env: &mut JNIEnv, func: F) -> T
where
    F: FnOnce(&mut JNIEnv) -> JavaResult<T>,
    T: EarlyReturn,
{
    let ret = func(env);
    match ret {
        Ok(r) => r,
        Err(e) => {
            // An error occurred, throw an exception
            e.set_exception_details(env);

            T::abort_value()
        }
    }
}

/// Trait for early return values when an exception is being thrown
pub trait EarlyReturn {
    fn abort_value() -> Self;
}

impl EarlyReturn for jobject {
    fn abort_value() -> Self {
        std::ptr::null_mut()
    }
}

impl EarlyReturn for i32 {
    fn abort_value() -> Self {
        0
    }
}

impl EarlyReturn for i64 {
    fn abort_value() -> Self {
        0
    }
}

impl EarlyReturn for () {
    fn abort_value() -> Self {}
}
