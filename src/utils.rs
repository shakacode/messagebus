use core::cmp::{Ord, Ordering};
use core::ops::Range;

pub fn binary_search_range_by_key<'a, T, B, F>(data: &'a [T], item: &B, mut f: F) -> Range<usize>
where
    F: FnMut(&'a T) -> B,
    B: Ord,
{
    if let Ok(index) = data.binary_search_by_key(item, &mut f) {
        let mut begin = index;
        let mut end = index + 1;

        for i in (0..index).rev() {
            if f(unsafe { data.get_unchecked(i) }).cmp(item) != Ordering::Equal {
                break;
            }

            begin = i;
        }

        for i in end..data.len() {
            end = i;

            if f(unsafe { data.get_unchecked(i) }).cmp(item) != Ordering::Equal {
                break;
            }
        }

        begin..end
    } else {
        data.len()..data.len()
    }
}
