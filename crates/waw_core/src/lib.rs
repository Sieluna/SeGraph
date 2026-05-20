#![no_std]

extern crate alloc;

mod bitfield;
mod cursor;
mod mutex;
mod pointer;
mod storage;

use core::sync::atomic::AtomicUsize;

use alloc::sync::Arc;
use alloc::vec::{Drain, Vec};

use self::bitfield::PointerData;
use self::storage::StorageInner;

pub use self::cursor::{Cursor, CursorItem, Slice};
pub use self::mutex::Mutex;
pub use self::pointer::{DeadComponentError, Pointer, WeakPointer};
pub use self::storage::{Item, Iter, IterMut, Storage};

pub type Index = usize;
type RefCount = u16;
type Epoch = u16;

type StorageId = u8;
static STORAGE_UID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct Pending {
    add_ref: Vec<Index>,
    sub_ref: Vec<Index>,
    epoch: Vec<Epoch>,
}

impl Pending {
    #[inline]
    fn drain_sub(&mut self) -> (Drain<'_, Index>, &mut [Epoch]) {
        (self.sub_ref.drain(..), self.epoch.as_mut_slice())
    }

    #[inline]
    fn get_epoch(&self, index: usize) -> Epoch {
        *self.epoch.get(index).unwrap_or(&0)
    }
}

type PendingRef = Arc<Mutex<Pending>>;
