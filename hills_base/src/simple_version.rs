use rkyv::{Archive, Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

#[derive(Archive, PartialEq, Eq, Debug, Copy, Clone, Hash, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(derive(PartialEq, Eq, Debug, Hash))]
pub struct SimpleVersion {
    /// Backwards compatibility breaking
    pub major: u16,
    /// Backwards and Future compatible changes
    pub minor: u16,
}

impl SimpleVersion {
    pub const fn new(major: u16, minor: u16) -> SimpleVersion {
        SimpleVersion { major, minor }
    }

    pub fn as_archived(&self) -> ArchivedSimpleVersion {
        ArchivedSimpleVersion {
            major: self.major,
            minor: self.minor,
        }
    }

    pub fn rust_version() -> SimpleVersion {
        SimpleVersion {
            major: 1,
            minor: 74,
        }
    }

    pub fn rkyv_version() -> SimpleVersion {
        SimpleVersion { major: 0, minor: 7 }
    }
}

impl ArchivedSimpleVersion {
    pub fn as_original(&self) -> SimpleVersion {
        SimpleVersion {
            major: self.major,
            minor: self.minor,
        }
    }
}

impl Display for SimpleVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl PartialOrd for SimpleVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SimpleVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.major > other.major {
            Ordering::Greater
        } else if self.major < other.major {
            Ordering::Less
        } else {
            self.minor.cmp(&other.minor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SimpleVersion;

    #[test]
    fn test_version_cmp() {
        assert!(SimpleVersion::new(1, 0) > SimpleVersion::new(0, 0));
        assert!(SimpleVersion::new(0, 10) > SimpleVersion::new(0, 9));
        assert_eq!(SimpleVersion::new(0, 0), SimpleVersion::new(0, 0));
    }
}
