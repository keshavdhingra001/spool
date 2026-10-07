//! A reference downstream store that checks fencing tokens (D43).
//!
//! A worker writes its job's effect with the token of its lease. The store
//! accepts the write only if that token is at least the highest one it has
//! accepted for the key, so a worker whose lease expired (a zombie, D5) cannot
//! overwrite what a newer lease wrote. Equal tokens are accepted so one lease
//! can rewrite its own value, which is what makes a retried effect idempotent.
//!
//! The same rule in SQL is one conditional write:
//!
//! ```sql
//! UPDATE effects SET value = $v, token = $t WHERE key = $k AND token <= $t
//! ```
//!
//! (plus an insert for a new key), and a write that changes no row is the
//! refusal.

use std::collections::BTreeMap;

use crate::types::Token;

/// A write refused because a newer lease already wrote the key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stale {
    /// The highest token accepted for the key.
    pub current: Token,
}

#[derive(Clone, Debug, Default)]
pub struct FencedStore<K: Ord, V> {
    entries: BTreeMap<K, (Token, V)>,
    accepted: u64,
    refused: u64,
}

impl<K: Ord, V> FencedStore<K, V> {
    pub fn new() -> Self {
        FencedStore {
            entries: BTreeMap::new(),
            accepted: 0,
            refused: 0,
        }
    }

    /// Store `value` under `key` if `token` is not older than the token of
    /// the value there now.
    pub fn write(&mut self, key: K, token: Token, value: V) -> Result<(), Stale> {
        if let Some(&(current, _)) = self.entries.get(&key)
            && token < current
        {
            self.refused += 1;
            return Err(Stale { current });
        }
        self.entries.insert(key, (token, value));
        self.accepted += 1;
        Ok(())
    }

    /// The value under `key` and the token that wrote it.
    pub fn get(&self, key: &K) -> Option<(Token, &V)> {
        self.entries.get(key).map(|(t, v)| (*t, v))
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Writes accepted and refused so far.
    pub fn writes(&self) -> (u64, u64) {
        (self.accepted, self.refused)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn older_tokens_are_refused() {
        let mut s = FencedStore::new();
        s.write("job-1", Token(5), "a").unwrap();
        // The same lease may write again (an idempotent retry).
        s.write("job-1", Token(5), "a2").unwrap();
        s.write("job-1", Token(6), "b").unwrap();
        // The zombie with token 5 comes back.
        assert_eq!(
            s.write("job-1", Token(5), "late"),
            Err(Stale { current: Token(6) })
        );
        assert_eq!(s.get(&"job-1"), Some((Token(6), &"b")));
        // Tokens are global (D10): one fence orders writes of different jobs.
        s.write("job-2", Token(3), "c").unwrap();
        assert_eq!(s.writes(), (4, 1));
        assert_eq!(s.len(), 2);
    }
}
