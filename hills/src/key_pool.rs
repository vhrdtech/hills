use crate::common::Error;
use crate::consts::KEY_POOL;
use rkyv::{check_archived_root, to_bytes, Archive, Deserialize, Serialize};
use sled::transaction::ConflictableTransactionError;
use sled::Tree;
use std::ops::Range;

#[derive(Archive, Debug, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct KeyPool {
    ranges: Vec<Range<u32>>,
    _dummy22: [u8; 6],
}

impl KeyPool {
    pub fn new(ranges: Vec<Range<u32>>) -> Self {
        KeyPool {
            ranges,
            _dummy22: [0u8; 6],
        }
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

    pub fn total_keys_available(&self) -> u32 {
        self.ranges.iter().fold(0, |acc, r| acc + r.end - r.start)
    }

    pub fn feed_for(tree: &Tree, additional_range: Range<u32>) -> Result<(), String> {
        let r = tree.transaction(|tx_db| match tx_db.get(KEY_POOL)? {
            Some(key_pool) => {
                let key_pool: &ArchivedKeyPool = check_archived_root::<KeyPool>(&key_pool)
                    .map_err(|_| ConflictableTransactionError::Abort("check_archived_root"))?;
                let mut key_pool: KeyPool =
                    key_pool.deserialize(&mut rkyv::Infallible).map_err(|_| {
                        ConflictableTransactionError::Abort("get_next_key: deserialize")
                    })?;
                key_pool.push(additional_range.clone());
                let key_pool = to_bytes::<_, 8>(&key_pool)
                    .map_err(|_| ConflictableTransactionError::Abort("to_bytes"))?;
                tx_db.insert(KEY_POOL, &*key_pool)?;
                Ok(())
            }
            None => {
                let key_pool = KeyPool::new(vec![additional_range.clone()]);
                let key_pool = to_bytes::<_, 8>(&key_pool)
                    .map_err(|_| ConflictableTransactionError::Abort("to_bytes"))?;
                tx_db.insert(KEY_POOL, &*key_pool)?;
                Ok(())
            }
        });
        r.map_err(|e| format!("{e:?}"))
    }

    pub fn stats_for(tree: &Tree) -> Result<u32, Error> {
        if let Some(key_pool) = tree.get(KEY_POOL)? {
            // let key_pool: &ArchivedKeyPool = unsafe { archived_root::<KeyPool>(&key_pool) };
            let key_pool: &ArchivedKeyPool = check_archived_root::<KeyPool>(&key_pool)?;
            let key_pool: KeyPool = key_pool.deserialize(&mut rkyv::Infallible)?;
            Ok(key_pool.total_keys_available())
        } else {
            Ok(0)
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
