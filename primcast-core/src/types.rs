use serde::Deserialize;
use serde::Serialize;
use smallvec::SmallVec;

/// Peer/Process ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct Pid(pub u32);

/// Group ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct Gid(pub u8);

/// Message ID
pub type MsgId = u128;

/// Logical clock value
pub type Clock = u64;

/// Group epoch
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub struct Epoch(pub u32, pub Pid);

impl Epoch {
    pub fn initial() -> Epoch {
        Epoch(0, Pid(0))
    }

    /// Next epoch higher than this one, owned by the given Pid
    pub fn next_for(self, pid: Pid) -> Self {
        Epoch(self.0 + 1, pid)
    }

    pub fn owner(self) -> Pid {
        self.1
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Epoch({},{})", self.0, (self.1).0)
    }
}

/// Set of groups. SmallVec avoids allocations for arrays smaller than the type parameter.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GidSet(pub SmallVec<[Gid; 8]>);

impl From<SmallVec<[Gid; 8]>> for GidSet {
    fn from(mut other: SmallVec<[Gid; 8]>) -> GidSet {
        other.sort();
        other.dedup();
        GidSet(other)
    }
}

impl AsRef<SmallVec<[Gid; 8]>> for GidSet {
    fn as_ref(&self) -> &SmallVec<[Gid; 8]> {
        &self.0
    }
}

impl AsMut<SmallVec<[Gid; 8]>> for GidSet {
    fn as_mut(&mut self) -> &mut SmallVec<[Gid; 8]> {
        &mut self.0
    }
}

impl std::iter::IntoIterator for GidSet {
    type Item = Gid;
    type IntoIter = smallvec::IntoIter<[Gid; 8]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl std::iter::FromIterator<Gid> for GidSet {
    fn from_iter<I: IntoIterator<Item = Gid>>(iter: I) -> Self {
        let mut vec: SmallVec<_> = iter.into_iter().collect();
        vec.sort();
        vec.dedup();
        GidSet(vec)
    }
}

impl GidSet {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn clear(&mut self) {
        self.0.clear()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Gid> {
        self.0.iter()
    }

    // returns true if the element was inserted (false if already present)
    pub fn insert(&mut self, g: Gid) -> bool {
        if self.contains(g) {
            false
        } else {
            self.0.push(g);
            true
        }
    }

    // insert all entries from the other
    pub fn merge(&mut self, other: &Self) {
        for o in &other.0 {
            self.insert(*o);
        }
    }

    // returns true if the element was present
    pub fn remove(&mut self, g: Gid) -> bool {
        if let Some(idx) = self.0.iter().position(|it| *it == g) {
            self.0.swap_remove(idx);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, g: Gid) -> bool {
        self.0.iter().find(|it| **it == g).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gidset() {
        let mut s1 = GidSet::new();
        let mut s2 = GidSet::new();

        assert!(s1.is_empty());
        assert_eq!(s1.len(), 0);

        s1.insert(Gid(0));
        s1.insert(Gid(1));
        assert!(s1.contains(Gid(0)));
        assert!(s1.contains(Gid(1)));
        s2.insert(Gid(2));
        s2.merge(&s1);
        assert!(s2.contains(Gid(0)));
        assert!(s2.contains(Gid(1)));
        assert!(s2.contains(Gid(2)));
        assert_eq!(s2.len(), 3);

        s1.remove(Gid(0));
        assert!(!s1.contains(Gid(0)));
        assert_eq!(s1.len(), 1);

        s1.clear();
        assert!(s1.is_empty());
    }
}
