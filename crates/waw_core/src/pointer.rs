use core::fmt::{Debug, Formatter};
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;

use super::{PendingRef, PointerData};

/// Error from upgrading a `WeakPointer` whose component was destroyed.
#[derive(Debug, PartialEq, Eq)]
pub struct DeadComponentError;

/// Strong reference to a component. The component outlives this pointer.
pub struct Pointer<T> {
    pub data: PointerData,
    pub(crate) pending: PendingRef,
    pub(crate) marker: PhantomData<T>,
}

impl<T> Debug for Pointer<T> {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        f.debug_struct("Pointer")
            .field("index", &self.data.get_index())
            .field("epoch", &usize::from(self.data.get_epoch()))
            .field("storage_id", &usize::from(self.data.get_storage_id()))
            .field("pending", &self.pending.lock())
            .finish()
    }
}

impl<T> Pointer<T> {
    /// Creates a new `WeakPointer` to this component.
    #[inline]
    #[must_use]
    pub fn downgrade(&self) -> WeakPointer<T> {
        WeakPointer {
            data: self.data,
            pending: self.pending.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> PartialOrd for Pointer<T> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        if self.data.get_storage_id() == other.data.get_storage_id() {
            debug_assert!(
                self.data.get_index() != other.data.get_index()
                    || self.data.get_epoch() == self.data.get_epoch()
            );
            self.data.get_index().partial_cmp(&other.data.get_index())
        } else {
            None
        }
    }
}

impl<T> Clone for Pointer<T> {
    #[inline]
    fn clone(&self) -> Self {
        self.pending.lock().add_ref.push(self.data.get_index());
        Self {
            data: self.data,
            pending: self.pending.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> PartialEq for Pointer<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for Pointer<T> {}

impl<T> Hash for Pointer<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl<T> Drop for Pointer<T> {
    #[inline]
    fn drop(&mut self) {
        self.pending.lock().sub_ref.push(self.data.get_index());
    }
}

/// Weak variant of `Pointer`. Breaks reference cycles. Upgrade to access.
#[derive(Debug)]
pub struct WeakPointer<T> {
    data: PointerData,
    pending: PendingRef,
    marker: PhantomData<T>,
}

impl<T> WeakPointer<T> {
    /// Upgrade to a `Pointer`. Returns `DeadComponentError` if the component was destroyed.
    pub fn upgrade(&self) -> Result<Pointer<T>, DeadComponentError> {
        let mut pending = self.pending.lock();
        if pending.get_epoch(self.data.get_index()) != self.data.get_epoch() {
            return Err(DeadComponentError);
        }
        pending.add_ref.push(self.data.get_index());
        Ok(Pointer {
            data: self.data,
            pending: self.pending.clone(),
            marker: PhantomData,
        })
    }
}

impl<T> Clone for WeakPointer<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            data: self.data,
            pending: self.pending.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> PartialEq for WeakPointer<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for WeakPointer<T> {}
