use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

use super::{Index, PendingRef, Pointer, PointerData, StorageId, StorageInner};

/// Storage slice for cursor iteration.
#[derive(Debug)]
pub struct Slice<'a, T> {
    pub slice: &'a mut [T],
    pub offset: PointerData,
}

impl<'a, T> Slice<'a, T> {
    /// Check if the slice contains no elements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }

    /// Get a reference by pointer. Returns `None` if out of bounds.
    #[must_use]
    pub fn get(&'a self, pointer: &Pointer<T>) -> Option<&'a T> {
        debug_assert_eq!(pointer.data.get_storage_id(), self.offset.get_storage_id());
        let index = pointer
            .data
            .get_index()
            .wrapping_sub(self.offset.get_index());
        self.slice.get(index)
    }

    /// Get a mutable reference by pointer. Returns `None` if out of bounds.
    #[must_use]
    pub fn get_mut(&'a mut self, pointer: &Pointer<T>) -> Option<&'a mut T> {
        debug_assert_eq!(pointer.data.get_storage_id(), self.offset.get_storage_id());
        let index = pointer
            .data
            .get_index()
            .wrapping_sub(self.offset.get_index());
        self.slice.get_mut(index)
    }
}

/// Streaming iterator item. Allows accessing sibling components while iterating.
#[derive(Debug)]
pub struct CursorItem<'a, T> {
    item: &'a mut T,
    pending: &'a PendingRef,
    data: PointerData,
}

impl<T> Deref for CursorItem<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.item
    }
}

impl<T> DerefMut for CursorItem<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.item
    }
}

impl<T> CursorItem<'_, T> {
    /// Pin the item with a strong pointer.
    #[must_use]
    pub fn pin(&self) -> Pointer<T> {
        let epoch = {
            let mut pending = self.pending.lock();
            pending.add_ref.push(self.data.get_index());
            pending.get_epoch(self.data.get_index())
        };
        Pointer {
            data: self.data.with_epoch(epoch),
            pending: self.pending.clone(),
            marker: PhantomData,
        }
    }
}

/// Streaming mutable iterator with look-back/ahead.
#[derive(Debug)]
pub struct Cursor<'a, T> {
    pub(crate) storage: &'a mut StorageInner<T>,
    pub(crate) pending: &'a PendingRef,
    pub(crate) index: Index,
    pub(crate) storage_id: StorageId,
}

impl<T> Cursor<'_, T> {
    fn split(&mut self, index: usize) -> (Slice<'_, T>, CursorItem<'_, T>, Slice<'_, T>) {
        let data = PointerData::new(index, 0, self.storage_id);
        let (left, item, right) = self.storage.split(data);
        let item = CursorItem {
            item,
            data,
            pending: self.pending,
        };
        (left, item, right)
    }

    /// Advance the stream to the next item.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(Slice<'_, T>, CursorItem<'_, T>, Slice<'_, T>)> {
        loop {
            let id = self.index;
            self.index += 1;
            match self.storage.meta.get(id) {
                None => {
                    self.index = id; // prevent the bump of the index
                    return None;
                }
                Some(&0) => (),
                Some(_) => return Some(self.split(id)),
            }
        }
    }

    /// Advance the stream to the previous item.
    pub fn prev(&mut self) -> Option<(Slice<'_, T>, CursorItem<'_, T>, Slice<'_, T>)> {
        loop {
            if self.index == 0 {
                return None;
            }
            self.index -= 1;
            let id = self.index;
            debug_assert!(id < self.storage.meta.len());
            if unsafe { *self.storage.meta.get_unchecked(id) } != 0 {
                return Some(self.split(id));
            }
        }
    }
}
