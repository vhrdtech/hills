use rkyv::{Archive, Deserialize, Serialize};
use std::ops::Range;

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
// #[archive_attr(derive(Debug))]
pub struct KeyPool {
    ranges: Vec<Range<u32>>,
}

impl KeyPool {
    pub fn new(ranges: Vec<Range<u32>>) -> Self {
        KeyPool { ranges }
    }

    pub fn push(&mut self, additional_range: Range<u32>) {
        self.ranges.push(additional_range);
    }

    pub fn get(&mut self) -> Option<u32> {
        if self.ranges.is_empty() {
            None
        } else {
            let next_key = self.ranges[0].start;
            if next_key + 1 >= self.ranges[0].end {
                self.ranges.remove(0);
            } else {
                self.ranges[0].start += 1;
            }
            Some(next_key)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::key_pool::KeyPool;

    #[test]
    fn empty() {
        let mut pool = KeyPool { ranges: vec![] };
        assert_eq!(pool.get(), None);
    }

    #[test]
    fn several_ranges() {
        let mut pool = KeyPool {
            ranges: vec![(0..2), (10..11)],
        };
        assert_eq!(pool.get(), Some(0));
        assert_eq!(pool.get(), Some(1));
        assert_eq!(pool.get(), Some(10));
        assert_eq!(pool.get(), None);
    }
}
