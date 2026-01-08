//! Safe(r) wrapper around trait object pointer manipulation.
//!
//! This module provides utilities for deconstructing and reconstructing trait object
//! references. While the operations are inherently unsafe, this wrapper centralizes
//! the unsafety and provides additional runtime checks.
//!
//! # Safety Invariants
//!
//! The safety of this module relies on Rust's trait object representation being
//! a pair of pointers: `(data_ptr, vtable_ptr)`. This is the de-facto standard
//! representation but is not guaranteed by the language specification.
//!
//! # Important
//!
//! This module is designed for **trait objects only** (`&dyn Trait`), not for
//! other DSTs like slices (`&[T]`). While both are fat pointers, they have
//! different metadata (vtable vs length).
//!
//! # Usage
//!
//! ```text
//! let value = 42i32;
//! let dyn_ref: &dyn Any = &value;
//!
//! // Deconstruct the trait object
//! let raw = TraitObject::from_dyn(dyn_ref);
//!
//! // ... store raw.data and raw.vtable separately ...
//!
//! // Reconstruct the trait object (requires same trait type)
//! let reconstructed: &dyn Any = unsafe { raw.as_dyn_ref() };
//! assert!(reconstructed.is::<i32>());
//! ```
//!
//! See the unit tests in this module for complete working examples.

use core::mem;

/// A deconstructed trait object containing the data pointer and vtable pointer.
///
/// # Memory Layout
///
/// This struct uses `#[repr(C)]` to ensure a predictable memory layout matching
/// the expected trait object representation: `(data_ptr, vtable_ptr)`.
///
/// # Intended Use
///
/// This type should **only** be used with trait object references (`&dyn Trait`).
/// Using it with slices or other DSTs will result in undefined behavior.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct TraitObject {
    /// Pointer to the actual data of the concrete type.
    pub data: *mut (),
    /// Pointer to the vtable containing trait method implementations.
    pub vtable: *mut (),
}

/// Size of a trait object reference (two pointers: data + vtable).
const TRAIT_OBJECT_SIZE: usize = mem::size_of::<usize>() * 2;

impl TraitObject {
    /// Creates a `TraitObject` from a trait object reference.
    ///
    /// This deconstructs the fat pointer into its data and vtable components.
    ///
    /// # Type Parameter
    ///
    /// `Dyn` should be a trait object type (e.g., `dyn MyTrait`). While this
    /// cannot be enforced at compile time on stable Rust, using non-trait-object
    /// types will trigger a debug assertion and cause undefined behavior.
    ///
    /// # Example
    ///
    /// ```text
    /// let value = 42i32;
    /// let dyn_ref: &dyn Any = &value;
    /// let raw = TraitObject::from_dyn(dyn_ref);
    ///
    /// assert!(!raw.data.is_null());
    /// assert!(!raw.vtable.is_null());
    /// ```
    ///
    /// # Panics (Debug Mode)
    ///
    /// Panics if the reference is not a fat pointer (two pointers wide).
    #[inline]
    pub fn from_dyn<Dyn: ?Sized>(dyn_ref: &Dyn) -> Self {
        // Compile-time assertion that trait objects are two pointers
        const {
            assert!(
                mem::size_of::<&dyn core::any::Any>() == TRAIT_OBJECT_SIZE,
                "TraitObject size mismatch: expected trait object to be two pointers"
            );
        }

        debug_assert!(
            mem::size_of::<&Dyn>() == TRAIT_OBJECT_SIZE,
            "TraitObject::from_dyn expects a trait object reference (&dyn Trait), \
             but got a pointer of size {} (expected {})",
            mem::size_of::<&Dyn>(),
            TRAIT_OBJECT_SIZE
        );

        // SAFETY: We've verified the size matches a fat pointer.
        // The transmute converts the trait object reference into our repr(C) struct.
        unsafe { mem::transmute_copy(&dyn_ref) }
    }

    /// Reconstructs a trait object reference from the stored pointers.
    ///
    /// # Safety
    ///
    /// The caller must guarantee:
    /// 1. The `data` pointer is valid and points to a value of the correct concrete type
    /// 2. The `vtable` pointer is valid and corresponds to trait `Dyn` for that concrete type
    /// 3. The lifetime `'a` does not outlive the validity of both pointers
    /// 4. `Dyn` must be the exact same trait type used when creating this `TraitObject`
    ///
    /// # Type Parameter
    ///
    /// `Dyn` must be the same trait object type (e.g., `dyn MyTrait`) that was
    /// used to create this `TraitObject`. Using a different trait will cause
    /// undefined behavior.
    ///
    /// # Example
    ///
    /// ```text
    /// let value = 42i32;
    /// let dyn_ref: &dyn Any = &value;
    /// let raw = TraitObject::from_dyn(dyn_ref);
    ///
    /// // SAFETY: raw was created from &dyn Any, so we reconstruct as &dyn Any
    /// let reconstructed: &dyn Any = unsafe { raw.as_dyn_ref() };
    /// assert!(reconstructed.is::<i32>());
    /// ```
    #[inline]
    pub unsafe fn as_dyn_ref<'a, Dyn: ?Sized>(&self) -> &'a Dyn {
        debug_assert!(!self.data.is_null(), "TraitObject data pointer is null");
        debug_assert!(!self.vtable.is_null(), "TraitObject vtable pointer is null");

        debug_assert!(
            mem::size_of::<&Dyn>() == TRAIT_OBJECT_SIZE,
            "TraitObject::to_dyn expects a trait object type (dyn Trait), \
             but got a pointer of size {} (expected {})",
            mem::size_of::<&Dyn>(),
            TRAIT_OBJECT_SIZE
        );

        // SAFETY: Caller guarantees the pointers are valid for type Dyn and lifetime 'a
        mem::transmute_copy(self)
    }

    /// Creates a new `TraitObject` from raw data and vtable pointers.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// 1. `data` points to a valid instance of some concrete type
    /// 2. `vtable` is a valid vtable pointer for some trait implemented by that type
    /// 3. The pointers remain valid for as long as this `TraitObject` is used
    ///
    /// # Example
    ///
    /// ```text
    /// let value = 42i32;
    /// let dyn_ref: &dyn Any = &value;
    /// let original = TraitObject::from_dyn(dyn_ref);
    ///
    /// // Store pointers separately, then reconstruct
    /// let data_ptr = original.data;
    /// let vtable_ptr = original.vtable;
    ///
    /// // SAFETY: pointers came from a valid TraitObject
    /// let reconstructed = unsafe { TraitObject::from_raw_parts(data_ptr, vtable_ptr) };
    /// ```
    #[inline]
    pub const unsafe fn from_raw_parts(data: *mut (), vtable: *mut ()) -> Self {
        Self { data, vtable }
    }

    /// Returns `true` if both pointers are non-null.
    ///
    /// Note: This is a basic sanity check. Non-null pointers can still be invalid
    /// (dangling, misaligned, etc.).
    #[inline]
    #[allow(dead_code)]
    pub fn is_apparently_valid(&self) -> bool {
        !self.data.is_null() && !self.vtable.is_null()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::any::Any;

    #[test]
    fn test_trait_object_roundtrip() {
        let value: i32 = 42;
        let dyn_ref: &dyn Any = &value;

        let raw = TraitObject::from_dyn(dyn_ref);
        assert!(raw.is_apparently_valid());

        // SAFETY: We're immediately reconstructing with the same trait type
        let reconstructed: &dyn Any = unsafe { raw.as_dyn_ref() };

        assert!(reconstructed.is::<i32>());
        assert_eq!(reconstructed.downcast_ref::<i32>(), Some(&42));
    }

    #[test]
    fn test_size_matches_fat_pointer() {
        assert_eq!(mem::size_of::<TraitObject>(), mem::size_of::<&dyn Any>());
        assert_eq!(mem::size_of::<TraitObject>(), TRAIT_OBJECT_SIZE);
    }

    #[test]
    fn test_multiple_traits() {
        use core::fmt::Debug;

        let value: i32 = 123;

        // Test with dyn Any
        let any_ref: &dyn Any = &value;
        let raw_any = TraitObject::from_dyn(any_ref);
        let restored_any: &dyn Any = unsafe { raw_any.as_dyn_ref() };
        assert!(restored_any.is::<i32>());

        // Test with dyn Debug
        let debug_ref: &dyn Debug = &value;
        let raw_debug = TraitObject::from_dyn(debug_ref);
        let restored_debug: &dyn Debug = unsafe { raw_debug.as_dyn_ref() };
        assert_eq!(format!("{:?}", restored_debug), "123");
    }
}
