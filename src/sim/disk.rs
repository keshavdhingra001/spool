//! A node's disk in the simulator (D50): a [`MemStorage`] the world keeps
//! across the node's crashes, shared with the node's durable queue.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;

use super::rng::Rng;
use crate::storage::{MemStorage, Storage};

#[derive(Clone, Debug, Default)]
pub struct SimDisk(Rc<RefCell<MemStorage>>);

impl SimDisk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the contents with one of the crash images (D28) the disk could
    /// leave right now, chosen by `rng`. Returns how many images there were.
    pub fn crash(&self, rng: &mut Rng) -> usize {
        let mut images = self.0.borrow().crash_images();
        let n = images.len();
        let i = rng.below(n as u64) as usize;
        *self.0.borrow_mut() = images.swap_remove(i);
        n
    }

    /// Let `n` more mutating calls succeed and fail the rest (D50).
    pub fn fail_in(&self, n: u64) {
        self.0.borrow_mut().fail_in(n);
    }

    /// Mutating calls made so far in this incarnation of the disk.
    pub fn calls(&self) -> u64 {
        self.0.borrow().calls()
    }

    pub fn files(&self) -> BTreeMap<String, Vec<u8>> {
        self.0.borrow().files()
    }
}

impl Storage for SimDisk {
    fn list(&self) -> io::Result<Vec<String>> {
        self.0.borrow().list()
    }
    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        self.0.borrow().read(name)
    }
    fn create(&mut self, name: &str) -> io::Result<()> {
        self.0.borrow_mut().create(name)
    }
    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().append(name, data)
    }
    fn sync(&mut self, name: &str) -> io::Result<()> {
        self.0.borrow_mut().sync(name)
    }
    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()> {
        self.0.borrow_mut().truncate(name, len)
    }
    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        self.0.borrow_mut().rename(from, to)
    }
    fn remove(&mut self, name: &str) -> io::Result<()> {
        self.0.borrow_mut().remove(name)
    }
    fn sync_dir(&mut self) -> io::Result<()> {
        self.0.borrow_mut().sync_dir()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_crash_keeps_synced_bytes_and_may_drop_the_rest() {
        let disk = SimDisk::new();
        let mut d = disk.clone();
        d.create("a").unwrap();
        d.append("a", b"xy").unwrap();
        d.sync("a").unwrap();
        d.sync_dir().unwrap();
        d.append("a", b"pq").unwrap();
        let mut rng = Rng::new(3);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let copy = SimDisk(Rc::new(RefCell::new(disk.0.borrow().clone())));
            assert_eq!(copy.crash(&mut rng), 4);
            seen.insert(copy.files()["a"].clone());
        }
        let expected: Vec<&[u8]> = vec![b"xy", b"xy\0\0", b"xyp", b"xypq"];
        assert_eq!(seen.into_iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn fail_in_counts_from_now() {
        let mut d = SimDisk::new();
        d.create("a").unwrap();
        d.fail_in(1);
        d.append("a", b"x").unwrap();
        assert!(d.append("a", b"y").is_err());
        assert_eq!(d.calls(), 3);
    }
}
