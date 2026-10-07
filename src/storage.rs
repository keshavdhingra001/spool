//! The only door to the disk (D28). The durable queue does every file
//! operation through [`Storage`], so tests can swap the real directory for
//! [`MemStorage`], which can fail at any call and show every state a crash
//! could leave behind.
//!
//! Every name is a plain file name inside one directory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub trait Storage {
    /// Names of the files in the directory, sorted.
    fn list(&self) -> io::Result<Vec<String>>;
    fn read(&self, name: &str) -> io::Result<Vec<u8>>;
    /// Create an empty file. Fails if it already exists.
    fn create(&mut self, name: &str) -> io::Result<()>;
    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()>;
    /// Make the file's contents durable (`fdatasync`). Its name is durable only
    /// after `sync_dir`.
    fn sync(&mut self, name: &str) -> io::Result<()>;
    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()>;
    /// Atomically rename, replacing `to` if it exists.
    fn rename(&mut self, from: &str, to: &str) -> io::Result<()>;
    fn remove(&mut self, name: &str) -> io::Result<()>;
    /// Make every create, rename and remove so far durable.
    fn sync_dir(&mut self) -> io::Result<()>;
}

/// A real directory, locked against other processes (D29).
pub struct FileStorage {
    dir: PathBuf,
    /// Held for as long as this value lives; the OS drops the lock on exit.
    _lock: File,
    /// Open append handles, so appends don't reopen the file.
    files: BTreeMap<String, File>,
}

/// The lock file's name. `list` leaves it out.
pub const LOCK: &str = "LOCK";

impl FileStorage {
    /// Open `dir`, creating it if needed. Fails with `WouldBlock` if another
    /// `FileStorage`, in this process or another, has it open.
    pub fn open(dir: &Path) -> io::Result<FileStorage> {
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(LOCK))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("{} is in use by another process", dir.display()),
                ));
            }
            Err(TryLockError::Error(e)) => return Err(e),
        }
        Ok(FileStorage {
            dir: dir.to_path_buf(),
            _lock: lock,
            files: BTreeMap::new(),
        })
    }

    fn path(&self, name: &str) -> PathBuf {
        debug_assert!(!name.contains('/') && name != LOCK, "bad file name {name}");
        self.dir.join(name)
    }

    fn handle(&mut self, name: &str) -> io::Result<&mut File> {
        if !self.files.contains_key(name) {
            let f = OpenOptions::new().append(true).open(self.path(name))?;
            self.files.insert(name.to_string(), f);
        }
        Ok(self.files.get_mut(name).expect("just inserted"))
    }
}

impl Storage for FileStorage {
    fn list(&self) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().into_string().map_err(|n| {
                io::Error::new(io::ErrorKind::InvalidData, format!("file name {n:?}"))
            })?;
            if name != LOCK {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(name))
    }

    fn create(&mut self, name: &str) -> io::Result<()> {
        let f = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(self.path(name))?;
        self.files.insert(name.to_string(), f);
        Ok(())
    }

    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.handle(name)?.write_all(data)
    }

    fn sync(&mut self, name: &str) -> io::Result<()> {
        self.handle(name)?.sync_data()
    }

    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()> {
        // set_len works on an append-mode handle; later appends go to the new end.
        self.handle(name)?.set_len(len)
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        fs::rename(self.path(from), self.path(to))?;
        self.files.remove(to);
        if let Some(f) = self.files.remove(from) {
            self.files.insert(to.to_string(), f);
        }
        Ok(())
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        self.files.remove(name);
        fs::remove_file(self.path(name))
    }

    fn sync_dir(&mut self) -> io::Result<()> {
        File::open(&self.dir)?.sync_all()
    }
}

/// An in-memory directory that remembers what is durable (D28).
///
/// Each file keeps its current bytes and the bytes as of its last `sync`; the
/// directory keeps its current names and the names as of the last `sync_dir`.
/// [`MemStorage::crash_images`] lists the directories a crash could leave.
#[derive(Clone, Debug, Default)]
pub struct MemStorage {
    /// File contents by inode number; a rename moves a name, not the bytes.
    inodes: Vec<Inode>,
    dir: BTreeMap<String, usize>,
    synced_dir: BTreeMap<String, usize>,
    /// Mutating calls made so far, including the failed ones.
    calls: u64,
    fail_after: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct Inode {
    data: Vec<u8>,
    synced: Vec<u8>,
}

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// A directory holding `files`, all of it durable: what a disk looks like
    /// when the machine comes back after a crash.
    pub fn from_files(files: BTreeMap<String, Vec<u8>>) -> Self {
        let mut s = MemStorage::new();
        for (name, data) in files {
            s.dir.insert(name, s.inodes.len());
            s.inodes.push(Inode {
                synced: data.clone(),
                data,
            });
        }
        s.synced_dir = s.dir.clone();
        s
    }

    /// Let `n` mutating calls succeed and fail every one after them, as if the
    /// disk died or the process was about to. The first failing `append` still
    /// leaves its bytes unsynced in the file, so a crash image can hold any
    /// prefix of a write that reported an error.
    pub fn fail_after(mut self, n: u64) -> Self {
        self.fail_after = Some(n);
        self
    }

    /// Mutating calls made so far.
    pub fn calls(&self) -> u64 {
        self.calls
    }

    /// Current contents of every file, by name.
    pub fn files(&self) -> BTreeMap<String, Vec<u8>> {
        self.dir
            .iter()
            .map(|(name, &ino)| (name.clone(), self.inodes[ino].data.clone()))
            .collect()
    }

    /// Every directory a crash right now could leave, each one fully durable.
    ///
    /// The directory is the one as of the last `sync_dir` or the current one.
    /// In it, every file has its synced bytes; then, one file at a time, each
    /// file with unsynced bytes after its synced ones gets every prefix of them
    /// and a zero-filled tail of the same length; then every file at its
    /// current bytes. A file truncated since its last sync gets its synced or
    /// its current bytes. Duplicates are removed.
    pub fn crash_images(&self) -> Vec<MemStorage> {
        let mut images = BTreeSet::new();
        for dir in [&self.synced_dir, &self.dir] {
            let base: BTreeMap<String, Vec<u8>> = dir
                .iter()
                .map(|(n, &ino)| (n.clone(), self.inodes[ino].synced.clone()))
                .collect();
            images.insert(base.clone());
            let mut current = base.clone();
            for (name, &ino) in dir {
                let Inode { data, synced } = &self.inodes[ino];
                current.insert(name.clone(), data.clone());
                let mut variants = Vec::new();
                if let Some(extra) = data.strip_prefix(synced.as_slice()) {
                    for k in 1..=extra.len() {
                        variants.push([synced.as_slice(), &extra[..k]].concat());
                    }
                    if !extra.is_empty() {
                        variants.push([synced.clone(), vec![0; extra.len()]].concat());
                    }
                } else {
                    variants.push(data.clone());
                }
                for v in variants {
                    let mut image = base.clone();
                    image.insert(name.clone(), v);
                    images.insert(image);
                }
            }
            images.insert(current);
        }
        images.into_iter().map(MemStorage::from_files).collect()
    }

    /// Count one mutating call. Past the failure point it returns `Err(first)`,
    /// where `first` says whether this is the call that hit the point.
    fn step(&mut self) -> Result<(), bool> {
        let n = self.calls;
        self.calls += 1;
        match self.fail_after {
            Some(limit) if n >= limit => Err(n == limit),
            _ => Ok(()),
        }
    }

    fn inode(&self, name: &str) -> io::Result<usize> {
        self.dir
            .get(name)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, format!("no file {name}")))
    }
}

fn injected() -> io::Error {
    io::Error::other("injected storage failure")
}

impl Storage for MemStorage {
    fn list(&self) -> io::Result<Vec<String>> {
        Ok(self.dir.keys().cloned().collect())
    }

    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        Ok(self.inodes[self.inode(name)?].data.clone())
    }

    fn create(&mut self, name: &str) -> io::Result<()> {
        self.step().map_err(|_| injected())?;
        if self.dir.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                name.to_string(),
            ));
        }
        self.dir.insert(name.to_string(), self.inodes.len());
        self.inodes.push(Inode::default());
        Ok(())
    }

    fn append(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        let ino = self.inode(name)?;
        let result = self.step();
        if result.is_ok() || result == Err(true) {
            self.inodes[ino].data.extend_from_slice(data);
        }
        result.map_err(|_| injected())
    }

    fn sync(&mut self, name: &str) -> io::Result<()> {
        let ino = self.inode(name)?;
        self.step().map_err(|_| injected())?;
        let inode = &mut self.inodes[ino];
        inode.synced = inode.data.clone();
        Ok(())
    }

    fn truncate(&mut self, name: &str, len: u64) -> io::Result<()> {
        let ino = self.inode(name)?;
        self.step().map_err(|_| injected())?;
        let len = usize::try_from(len).expect("in-memory file fits in memory");
        self.inodes[ino].data.truncate(len);
        Ok(())
    }

    fn rename(&mut self, from: &str, to: &str) -> io::Result<()> {
        let ino = self.inode(from)?;
        self.step().map_err(|_| injected())?;
        self.dir.remove(from);
        self.dir.insert(to.to_string(), ino);
        Ok(())
    }

    fn remove(&mut self, name: &str) -> io::Result<()> {
        self.inode(name)?;
        self.step().map_err(|_| injected())?;
        self.dir.remove(name);
        Ok(())
    }

    fn sync_dir(&mut self) -> io::Result<()> {
        self.step().map_err(|_| injected())?;
        self.synced_dir = self.dir.clone();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn images(s: &MemStorage) -> Vec<BTreeMap<String, Vec<u8>>> {
        s.crash_images().iter().map(MemStorage::files).collect()
    }

    fn files(entries: &[(&str, &[u8])]) -> BTreeMap<String, Vec<u8>> {
        entries
            .iter()
            .map(|(n, d)| (n.to_string(), d.to_vec()))
            .collect()
    }

    #[test]
    fn unsynced_bytes_survive_as_any_prefix_or_zeros() {
        let mut s = MemStorage::new();
        s.create("a").unwrap();
        s.append("a", b"xy").unwrap();
        s.sync("a").unwrap();
        s.sync_dir().unwrap();
        s.append("a", b"pq").unwrap();
        assert_eq!(
            images(&s),
            vec![
                files(&[("a", b"xy")]),
                files(&[("a", b"xy\0\0")]),
                files(&[("a", b"xyp")]),
                files(&[("a", b"xypq")]),
            ]
        );
        // An image is fully durable: crashing it again changes nothing.
        let image = &s.crash_images()[2];
        assert_eq!(images(image), vec![image.files()]);
    }

    #[test]
    fn names_need_a_directory_sync() {
        let mut s = MemStorage::new();
        s.create("old").unwrap();
        s.append("old", b"1").unwrap();
        s.sync("old").unwrap();
        s.sync_dir().unwrap();
        s.create("new.tmp").unwrap();
        s.append("new.tmp", b"2").unwrap();
        s.sync("new.tmp").unwrap();
        s.rename("new.tmp", "new").unwrap();
        s.remove("old").unwrap();
        assert_eq!(
            images(&s),
            vec![files(&[("new", b"2")]), files(&[("old", b"1")])]
        );
        s.sync_dir().unwrap();
        assert_eq!(images(&s), vec![files(&[("new", b"2")])]);
    }

    #[test]
    fn unsynced_truncate_may_or_may_not_land() {
        let mut s = MemStorage::new();
        s.create("a").unwrap();
        s.append("a", b"abc").unwrap();
        s.sync("a").unwrap();
        s.sync_dir().unwrap();
        s.truncate("a", 1).unwrap();
        assert_eq!(
            images(&s),
            vec![files(&[("a", b"a")]), files(&[("a", b"abc")])]
        );
    }

    #[test]
    fn fail_after_counts_mutating_calls() {
        let mut s = MemStorage::new().fail_after(2);
        s.create("a").unwrap();
        assert_eq!(s.read("a").unwrap(), b"");
        s.append("a", b"x").unwrap();
        // The first failing append still leaves its bytes, unsynced.
        assert!(s.append("a", b"y").is_err());
        assert!(s.append("a", b"z").is_err());
        assert!(s.sync("a").is_err());
        assert_eq!(s.calls(), 5);
        assert_eq!(s.files(), files(&[("a", b"xy")]));
        let images = images(&s);
        assert!(images.contains(&files(&[("a", b"xy")])));
        assert!(
            images.contains(&BTreeMap::new()),
            "create never reached the directory"
        );
    }
}
