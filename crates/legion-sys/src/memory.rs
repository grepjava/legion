//! A jail's memory, counted the way a container's memory limit means it
//! (DESIGN.md §6.3): every resident page once, however many of the jail's
//! processes map it. rctl's `memoryuse` sums each process's resident
//! pages, so memory many processes share (a database's buffers) counts
//! once for each of them.
//!
//! What is counted, from each process's map (`kern.proc.vmmap`) and the
//! anonymous objects (`vm.swap_objects`):
//! - anonymous memory: each object in the chain behind a mapping, its
//!   resident pages once (a forked process's copy-on-write pages are in an
//!   object it shares with its parent, behind one of its own);
//! - files: each mapped range's resident pages, once;
//! - shared memory the map names no object for (SysV and POSIX segments):
//!   by name, and for anonymous POSIX segments, which all have the same
//!   name, by address as well, which forked processes inherit.
//!
//! Devices and guard pages are not memory and are left out.

// Only FreeBSD reads maps; the counting is tested everywhere.
#![cfg_attr(not(target_os = "freebsd"), allow(dead_code))]

use std::collections::{BTreeMap, HashMap};

/// The memory of a jail's processes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JailMemory {
    /// Bytes resident, each page once.
    pub bytes: u64,
    /// The process with the most memory no other process of the jail
    /// maps, and how much: the one to kill when the jail is over its
    /// limit, as Linux's OOM killer picks the largest.
    pub largest: Option<(u32, u64)>,
}

/// One mapping in a process's map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Mapping {
    pub kind: Kind,
    pub start: u64,
    pub end: u64,
    pub offset: u64,
    /// Pages resident in the mapped range.
    pub resident: u64,
    /// The anonymous object's handle; 0 when the kernel names none.
    pub object: u64,
    /// A file's file system and inode.
    pub file: (u64, u64),
    pub path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Anonymous,
    File,
    /// Memory, but neither (physical pages: SysV segments with
    /// `kern.ipc.shm_use_phys`).
    Other,
    /// Not memory: devices, guard pages, dead mappings.
    Ignored,
}

/// An anonymous object: the one behind it, and its own resident pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Object {
    pub backing: u64,
    pub resident: u64,
}

/// What a mapped range that is not an object of `vm.swap_objects` is
/// told apart by.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Range {
    File {
        file: (u64, u64),
        offset: u64,
        len: u64,
    },
    Named {
        path: String,
        offset: u64,
        len: u64,
    },
    Mapped {
        object: u64,
        path: String,
        start: u64,
        offset: u64,
        len: u64,
    },
}

/// Who maps a page set: the first process seen, and whether any other.
#[derive(Debug, Clone, Copy)]
struct Seen {
    pages: u64,
    first: u32,
    shared: bool,
}

impl Seen {
    fn by(&mut self, pid: u32) {
        if pid != self.first {
            self.shared = true;
        }
    }
}

/// The memory of these processes (pid and map), with `objects` read from
/// `vm.swap_objects` and pages of `page` bytes.
pub(crate) fn count(
    processes: &[(u32, Vec<Mapping>)],
    objects: &HashMap<u64, Object>,
    page: u64,
) -> JailMemory {
    let mut counted: HashMap<u64, Seen> = HashMap::new();
    let mut ranges: BTreeMap<Range, Seen> = BTreeMap::new();
    for (pid, map) in processes {
        let pid = *pid;
        for m in map {
            if m.kind == Kind::Ignored {
                continue;
            }
            if m.kind == Kind::Anonymous && objects.contains_key(&m.object) {
                let mut handle = m.object;
                // A chain is short; the bound only guards against a cycle
                // read across a change.
                for _ in 0..64 {
                    let Some(o) = objects.get(&handle) else { break };
                    counted
                        .entry(handle)
                        .and_modify(|s| s.by(pid))
                        .or_insert(Seen {
                            pages: o.resident,
                            first: pid,
                            shared: false,
                        });
                    if o.backing == 0 {
                        break;
                    }
                    handle = o.backing;
                }
                continue;
            }
            let len = m.end - m.start;
            let range = match m.kind {
                Kind::File => Range::File {
                    file: m.file,
                    offset: m.offset,
                    len,
                },
                _ if m.object == 0 && !m.path.is_empty() && !m.path.ends_with("@anon") => {
                    Range::Named {
                        path: m.path.clone(),
                        offset: m.offset,
                        len,
                    }
                }
                _ => Range::Mapped {
                    object: m.object,
                    path: m.path.clone(),
                    start: m.start,
                    offset: m.offset,
                    len,
                },
            };
            ranges
                .entry(range)
                .and_modify(|s| {
                    s.by(pid);
                    s.pages = s.pages.max(m.resident);
                })
                .or_insert(Seen {
                    pages: m.resident,
                    first: pid,
                    shared: false,
                });
        }
    }
    let mut pages = 0;
    let mut own: BTreeMap<u32, u64> = processes.iter().map(|(pid, _)| (*pid, 0)).collect();
    for s in counted.values().chain(ranges.values()) {
        pages += s.pages;
        if !s.shared {
            *own.entry(s.first).or_default() += s.pages;
        }
    }
    let largest = own
        .into_iter()
        .max_by_key(|(pid, pages)| (*pages, std::cmp::Reverse(*pid)))
        .map(|(pid, pages)| (pid, pages * page));
    JailMemory {
        bytes: pages * page,
        largest,
    }
}

/// Reading the maps and objects from the kernel.
#[cfg(target_os = "freebsd")]
pub(crate) mod freebsd {
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::io;

    use libc::c_int;

    use super::{Kind, Mapping, Object};

    /// `struct kinfo_vmentry` up to `kve_path` (sys/user.h).
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Entry {
        structsize: c_int,
        kind: c_int,
        start: u64,
        end: u64,
        offset: u64,
        fileid: u64,
        _fsid_freebsd11: u32,
        _flags: c_int,
        resident: c_int,
        _private_resident: c_int,
        _protection: c_int,
        _ref_count: c_int,
        _shadow_count: c_int,
        _vn_type: c_int,
        _vn_size: u64,
        _rdev_freebsd11: u32,
        _mode: u16,
        _status: u16,
        /// `kve_vn_fsid` for files, `kve_obj` for anonymous objects.
        type_spec: u64,
        _rdev: u64,
        _spare: [c_int; 8],
    }

    /// `struct kinfo_vmobject` up to `kvo_path`.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct VmObject {
        structsize: c_int,
        _kind: c_int,
        _size: u64,
        _fileid: u64,
        _fsid_freebsd11: u32,
        _ref_count: c_int,
        _shadow_count: c_int,
        _memattr: c_int,
        resident: u64,
        _active: u64,
        _inactive: u64,
        /// `kvo_backing_obj` for anonymous objects.
        backing: u64,
        me: u64,
        _laundry: u64,
        _wired: u64,
        _qspare: [u64; 4],
        _swapped: u32,
        _flags: u32,
        _ispare: [u32; 6],
    }

    const _: () = assert!(std::mem::size_of::<Entry>() + libc::PATH_MAX as usize == 1160);

    // sys/user.h KVME_TYPE_*.
    const SWAP: c_int = 3;
    const VNODE: c_int = 2;
    const PHYS: c_int = 5;

    /// The packed records of a sysctl, each starting with its size.
    fn records<T: Copy>(buf: &[u8], mut each: impl FnMut(T, &[u8])) {
        let mut at = 0;
        while at + std::mem::size_of::<T>() <= buf.len() {
            let size = c_int::from_ne_bytes(buf[at..at + 4].try_into().expect("four bytes"));
            if size <= 0 || at + size as usize > buf.len() {
                break;
            }
            let size = size as usize;
            if size >= std::mem::size_of::<T>() {
                // SAFETY: the record holds a whole `T`, read unaligned.
                let t = unsafe { std::ptr::read_unaligned(buf[at..].as_ptr().cast::<T>()) };
                each(t, &buf[at + std::mem::size_of::<T>()..at + size]);
            }
            at += size;
        }
    }

    fn path(tail: &[u8]) -> String {
        let end = tail.iter().position(|b| *b == 0).unwrap_or(tail.len());
        String::from_utf8_lossy(&tail[..end]).into_owned()
    }

    /// Reads a sysctl whose size may grow between asking and reading.
    fn read(mib: &[c_int]) -> io::Result<Vec<u8>> {
        loop {
            let mut len = 0;
            // SAFETY: a size query, no buffer.
            let r = unsafe {
                libc::sysctl(
                    mib.as_ptr(),
                    mib.len() as u32,
                    std::ptr::null_mut(),
                    &mut len,
                    std::ptr::null(),
                    0,
                )
            };
            if r != 0 {
                return Err(io::Error::last_os_error());
            }
            len += len / 4 + 4096;
            let mut buf = vec![0u8; len];
            // SAFETY: `buf` holds `len` bytes.
            let r = unsafe {
                libc::sysctl(
                    mib.as_ptr(),
                    mib.len() as u32,
                    buf.as_mut_ptr().cast(),
                    &mut len,
                    std::ptr::null(),
                    0,
                )
            };
            if r == 0 {
                buf.truncate(len);
                return Ok(buf);
            }
            let e = io::Error::last_os_error();
            if e.raw_os_error() != Some(libc::ENOMEM) {
                return Err(e);
            }
        }
    }

    fn mib(name: &CStr) -> io::Result<Vec<c_int>> {
        let mut mib = vec![0; libc::CTL_MAXNAME as usize];
        let mut len = mib.len();
        // SAFETY: `mib` holds `len` entries.
        let r = unsafe { libc::sysctlnametomib(name.as_ptr(), mib.as_mut_ptr(), &mut len) };
        if r != 0 {
            return Err(io::Error::last_os_error());
        }
        mib.truncate(len);
        Ok(mib)
    }

    /// A process's map; `NotFound` if it has exited.
    pub fn mappings(pid: u32) -> io::Result<Vec<Mapping>> {
        let buf = read(&[
            libc::CTL_KERN,
            libc::KERN_PROC,
            libc::KERN_PROC_VMMAP,
            pid as c_int,
        ])
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::ESRCH) {
                io::Error::new(io::ErrorKind::NotFound, e)
            } else {
                e
            }
        })?;
        let mut map = Vec::new();
        records(&buf, |e: Entry, tail| {
            let kind = match e.kind {
                SWAP => Kind::Anonymous,
                VNODE => Kind::File,
                PHYS => Kind::Other,
                _ => Kind::Ignored,
            };
            map.push(Mapping {
                kind,
                start: e.start,
                end: e.end,
                offset: e.offset,
                resident: e.resident.max(0) as u64,
                object: if kind == Kind::File { 0 } else { e.type_spec },
                file: if kind == Kind::File {
                    (e.type_spec, e.fileid)
                } else {
                    (0, 0)
                },
                path: path(tail),
            });
        });
        Ok(map)
    }

    /// Every anonymous object, by handle.
    pub fn objects() -> io::Result<HashMap<u64, Object>> {
        let buf = read(&mib(c"vm.swap_objects")?)?;
        let mut objects = HashMap::new();
        records(&buf, |o: VmObject, _| {
            objects.insert(
                o.me,
                Object {
                    backing: o.backing,
                    resident: o.resident,
                },
            );
        });
        Ok(objects)
    }

    pub fn page_size() -> u64 {
        // SAFETY: no arguments to get wrong.
        let p = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if p > 0 { p as u64 } else { 4096 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anon(object: u64, resident: u64) -> Mapping {
        Mapping {
            kind: Kind::Anonymous,
            start: 0x1000,
            end: 0x1000 + resident * 4096,
            offset: 0,
            resident,
            object,
            file: (0, 0),
            path: String::new(),
        }
    }

    fn named(kind: Kind, path: &str, start: u64, resident: u64) -> Mapping {
        Mapping {
            kind,
            start,
            end: start + resident * 4096,
            offset: 0,
            resident,
            object: 0,
            file: (0, 0),
            path: path.into(),
        }
    }

    const MIB: u64 = 256;

    #[test]
    fn shared_memory_counts_once() {
        // A parent and three forked children, all mapping one 256 MiB
        // segment, each with 16 MiB of its own and the parent's 128 MiB
        // they share copy-on-write; the shared library's text is a file.
        let mut objects = HashMap::new();
        objects.insert(
            1,
            Object {
                backing: 0,
                resident: 256 * MIB,
            },
        );
        objects.insert(
            2,
            Object {
                backing: 0,
                resident: 128 * MIB,
            },
        );
        let libc = Mapping {
            kind: Kind::File,
            file: (7, 42),
            ..named(Kind::File, "/lib/libc.so.7", 0x8000, 100)
        };
        let mut processes = Vec::new();
        for pid in 10..14 {
            // Each process's own heap object sits in front of the shared one.
            let heap = 100 + u64::from(pid);
            objects.insert(
                heap,
                Object {
                    backing: 2,
                    resident: 16 * MIB,
                },
            );
            processes.push((
                pid,
                vec![anon(1, 256 * MIB), anon(heap, 144 * MIB), libc.clone()],
            ));
        }
        // The child 12 has more of its own.
        objects.get_mut(&112).unwrap().resident = 40 * MIB;
        let m = count(&processes, &objects, 4096);
        assert_eq!(m.bytes, (256 + 128 + 16 * 3 + 40) * (1 << 20) + 100 * 4096);
        assert_eq!(m.largest, Some((12, 40 << 20)));
    }

    #[test]
    fn segments_without_objects() {
        let objects = HashMap::new();
        let processes = vec![
            (
                1,
                vec![
                    named(Kind::Anonymous, "sysvshm(0:22)", 0x1000_0000, 10),
                    named(Kind::Anonymous, "posixshm@anon", 0x2000_0000, 20),
                    named(Kind::Anonymous, "posixshm@anon", 0x3000_0000, 5),
                    named(Kind::Ignored, "/dev/mem", 0x4000_0000, 1000),
                ],
            ),
            (
                2,
                vec![
                    // SysV by name wherever it is attached; an anonymous
                    // POSIX segment where the fork left it.
                    named(Kind::Anonymous, "sysvshm(0:22)", 0x5000_0000, 10),
                    named(Kind::Anonymous, "posixshm@anon", 0x2000_0000, 20),
                    // A segment not in vm.swap_objects (made since): by range.
                    anon(99, 3),
                ],
            ),
        ];
        let m = count(&processes, &objects, 4096);
        assert_eq!(m.bytes, (10 + 20 + 5 + 3) * 4096);
        assert_eq!(m.largest, Some((1, 5 * 4096)));
    }

    #[test]
    fn nothing_running() {
        assert_eq!(count(&[], &HashMap::new(), 4096), JailMemory::default());
    }
}
