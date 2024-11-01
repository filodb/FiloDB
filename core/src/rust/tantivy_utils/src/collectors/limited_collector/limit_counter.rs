//! Counter for limiting

use super::{LimitExceeded, LimitResult};

/// Counter to keep track of and enforce a limit
pub struct LimitCounter {
    limit: usize,
    count: usize,
}

impl LimitCounter {
    pub fn new(limit: usize) -> Self {
        Self { limit, count: 0 }
    }

    /// Increment the seen items, fail if hit the limit
    #[inline]
    pub fn increment(&mut self) -> LimitResult {
        self.count += 1;

        if self.count >= self.limit {
            Err(LimitExceeded)
        } else {
            Ok(())
        }
    }

    pub fn at_limit(&self) -> bool {
        self.count >= self.limit
    }
}

pub trait LimitCounterOptionExt {
    fn increment(&mut self) -> LimitResult;
}

impl LimitCounterOptionExt for Option<&mut LimitCounter> {
    #[inline]
    fn increment(&mut self) -> LimitResult {
        match self {
            Some(limiter) => limiter.increment(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_counter() {
        let mut counter = LimitCounter::new(2);

        // Count 0
        assert!(!counter.at_limit());

        // Count 1
        assert!(counter.increment().is_ok());
        assert!(!counter.at_limit());

        // Count 2
        assert!(counter.increment().is_err());
        assert!(counter.at_limit());
    }
}
