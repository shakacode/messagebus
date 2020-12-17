#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct TraitObject {
    pub data: *mut (),
    pub vtable: *mut (),
}
