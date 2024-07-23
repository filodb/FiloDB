//! Error types to translate to exceptions for the JVM

use std::borrow::Cow;

use jni::JNIEnv;

const RUNTIME_EXCEPTION_CLASS: &str = "java/lang/RuntimeException";

/// Result type for java exception methods
pub type JavaResult<T> = Result<T, JavaException>;

/// Error type that can be thrown as an exception
pub struct JavaException {
    class: &'static str,
    message: Cow<'static, str>,
}

impl JavaException {
    /// Create a new java.lang.RuntimeException
    pub fn new_runtime_exception(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(RUNTIME_EXCEPTION_CLASS, message)
    }

    /// Create a new exception with a specified class and message
    pub fn new(class: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            class,
            message: message.into(),
        }
    }

    /// Throw the generated exception on a JNIEnv
    pub fn set_exception_details(&self, env: &mut JNIEnv) {
        let _ = env.throw_new(self.class, &self.message);
    }
}

// Default conversion for Rust std errors - throw RuntimeException
impl<T: std::error::Error> From<T> for JavaException {
    fn from(value: T) -> Self {
        Self::new_runtime_exception(format!("{value}"))
    }
}
