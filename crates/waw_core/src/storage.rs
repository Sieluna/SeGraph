use core::iter::FromIterator;
use core::marker::PhantomData;
use core::sync::atomic::Ordering;

use alloc::{sync::Arc, vec::Vec};

use super::{
    Cursor, Epoch, Index, Mutex, Pending, PendingRef, Pointer, PointerData, RefCount, STORAGE_UID,
    Slice, StorageId,
};

#[derive(Debug)]
pub struct StorageInner<T> {
    pub data: Vec<T>,
    pub meta: Vec<RefCount>,
    free_list: Vec<PointerData>,
}

impl<T> StorageInner<T> {
    pub fn split(&mut self, offset: PointerData) -> (Slice<'_, T>, &mut T, Slice<'_, T>) {
        let sid = offset.get_storage_id();
        let index = offset.get_index();
        let (left, temp) = self.data.split_at_mut(index);
        let (item, right) = temp.split_at_mut(1);
        (
            Slice {
                slice: left,
                offset: PointerData::new(0, 0, sid),
            },
            unsafe { item.get_unchecked_mut(0) },
            Slice {
                slice: right,
                offset: PointerData::new(index + 1, 0, sid),
            },
        )
    }
}

#[derive(Debug)]
pub struct Storage<T> {
    inner: StorageInner<T>,
    pending: PendingRef,
    id: StorageId,
}

impl<T> core::ops::Index<&Pointer<T>> for Storage<T> {
    type Output = T;
    #[inline]
    fn index(&self, pointer: &Pointer<T>) -> &T {
        debug_assert_eq!(pointer.data.get_storage_id(), self.id);
        debug_assert!(pointer.data.get_index() < self.inner.data.len());
        unsafe { self.inner.data.get_unchecked(pointer.data.get_index()) }
    }
}

impl<T> core::ops::IndexMut<&Pointer<T>> for Storage<T> {
    #[inline]
    fn index_mut(&mut self, pointer: &Pointer<T>) -> &mut T {
        debug_assert_eq!(pointer.data.get_storage_id(), self.id);
        debug_assert!(pointer.data.get_index() < self.inner.data.len());
        unsafe { self.inner.data.get_unchecked_mut(pointer.data.get_index()) }
    }
}

impl<T> FromIterator<T> for Storage<T> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        let data: Vec<T> = iter.into_iter().collect();
        let count = data.len();
        Self::new_impl(data, alloc::vec![0; count], alloc::vec![0; count])
    }
}

impl<'a, T> IntoIterator for &'a Storage<T> {
    type Item = Item<'a, T>;
    type IntoIter = Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        Iter {
            storage: &self.inner,
            skip_lost: true,
            index: 0,
        }
    }
}

impl<'a, T> IntoIterator for &'a mut Storage<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T> Storage<T> {
    fn new_impl(data: Vec<T>, meta: Vec<RefCount>, epoch: Vec<Epoch>) -> Self {
        assert_eq!(data.len(), meta.len());
        assert!(epoch.len() <= meta.len());
        let uid = STORAGE_UID.fetch_add(1, Ordering::Relaxed) as StorageId;
        Self {
            inner: StorageInner {
                data,
                meta,
                free_list: Vec::new(),
            },
            pending: Arc::new(Mutex::new(Pending {
                add_ref: Vec::new(),
                sub_ref: Vec::new(),
                epoch,
            })),
            id: uid,
        }
    }

    #[must_use]
    pub fn new() -> Self {
        Self::new_impl(Vec::new(), Vec::new(), Vec::new())
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new_impl(
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
            Vec::with_capacity(capacity),
        )
    }

    pub fn sync_pending(&mut self) {
        let mut pending = self.pending.lock();
        while pending.epoch.len() < self.inner.data.len() {
            pending.epoch.push(0);
        }
        for index in pending.add_ref.drain(..) {
            self.inner.meta[index] += 1;
        }
        {
            let (refs, epoch) = pending.drain_sub();
            for index in refs {
                self.inner.meta[index] -= 1;
                if self.inner.meta[index] == 0 {
                    epoch[index] += 1;
                    let data = PointerData::new(index, epoch[index], self.id);
                    self.inner.free_list.push(data);
                }
            }
        }
    }

    #[inline]
    #[must_use]
    pub const fn iter(&self) -> Iter<'_, T> {
        Iter {
            storage: &self.inner,
            skip_lost: true,
            index: 0,
        }
    }

    #[inline]
    #[must_use]
    pub const fn iter_all(&self) -> Iter<'_, T> {
        Iter {
            storage: &self.inner,
            skip_lost: false,
            index: 0,
        }
    }

    #[inline]
    #[must_use]
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        IterMut {
            data: self.inner.data.iter_mut(),
            meta: self.inner.meta.iter(),
        }
    }

    #[inline]
    pub fn iter_all_mut(&mut self) -> core::slice::IterMut<'_, T> {
        self.inner.data.iter_mut()
    }

    #[must_use]
    pub fn pin(&self, item: &Item<T>) -> Pointer<T> {
        let mut pending = self.pending.lock();
        pending.add_ref.push(item.index);
        Pointer {
            data: PointerData::new(item.index, pending.get_epoch(item.index), self.id),
            pending: self.pending.clone(),
            marker: PhantomData,
        }
    }

    pub fn split(&mut self, pointer: &Pointer<T>) -> (Slice<'_, T>, &mut T, Slice<'_, T>) {
        debug_assert_eq!(pointer.data.get_storage_id(), self.id);
        self.inner.split(pointer.data)
    }

    #[inline]
    #[must_use]
    pub const fn cursor(&mut self) -> Cursor<'_, T> {
        Cursor {
            storage: &mut self.inner,
            pending: &self.pending,
            index: 0,
            storage_id: self.id,
        }
    }

    #[inline]
    #[must_use]
    pub const fn cursor_end(&mut self) -> Cursor<'_, T> {
        let total = self.inner.data.len();
        Cursor {
            storage: &mut self.inner,
            pending: &self.pending,
            index: total,
            storage_id: self.id,
        }
    }

    #[must_use]
    pub fn create(&mut self, value: T) -> Pointer<T> {
        let data = if let Some(data) = self.inner.free_list.pop() {
            let i = data.get_index();
            debug_assert_eq!(self.inner.meta[i], 0);
            self.inner.data[i] = value;
            self.inner.meta[i] = 1;
            data
        } else {
            let i = self.inner.meta.len();
            debug_assert_eq!(self.inner.data.len(), i);
            self.inner.data.push(value);
            self.inner.meta.push(1);
            PointerData::new(i, 0, self.id)
        };
        Pointer {
            data,
            pending: self.pending.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> Default for Storage<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Item<'a, T> {
    pub data: &'a T,
    pub index: Index,
}

impl<T> core::ops::Deref for Item<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.data
    }
}

#[derive(Debug)]
pub struct Iter<'a, T> {
    storage: &'a StorageInner<T>,
    skip_lost: bool,
    index: Index,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = Item<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let id = self.index;
            if id >= self.storage.data.len() {
                return None;
            }
            self.index += 1;
            if !self.skip_lost || unsafe { *self.storage.meta.get_unchecked(id) } != 0 {
                return Some(Item {
                    data: unsafe { self.storage.data.get_unchecked(id) },
                    index: id,
                });
            }
        }
    }
}

impl<T> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Iter {
            storage: self.storage,
            skip_lost: self.skip_lost,
            index: self.index,
        }
    }
}

#[derive(Debug)]
pub struct IterMut<'a, T> {
    data: core::slice::IterMut<'a, T>,
    meta: core::slice::Iter<'a, RefCount>,
}

impl<'a, T> Iterator for IterMut<'a, T> {
    type Item = &'a mut T;
    fn next(&mut self) -> Option<Self::Item> {
        while self.meta.next() == Some(&0) {
            self.data.next();
        }
        self.data.next()
    }
}

impl<T> DoubleEndedIterator for IterMut<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        while self.meta.next_back() == Some(&0) {
            self.data.next_back();
        }
        self.data.next_back()
    }
}
