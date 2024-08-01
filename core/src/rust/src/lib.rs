//! Native methods for FiloDB core
//!
//! This library extensively uses JNI to interop with JVM code.
//!
//! Any new code should do the following to ensure consistency:
//!
//! * All JNI methods should be wrapped in jni_exec.  This turns any
//!   Rust errors into RuntimeExceptions and allows for cleaner Rust
//!   error handling.
//! * No panic/unwrap/expect calls should be used.  Panicing will destroy
//!   the JVM process.
//! * Try to use primitive types when possible.  Getting fields on JVM
//!   objects requires reflection like overhead that can't be optimized
//!   as well
//! * Minimize the calls back into the JVM.  Perfer to get passed in
//!   needed information as arguments vs calling object methods.
//!

#![deny(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

mod errors;
mod exec;
mod index;
mod jnienv;
mod state;
