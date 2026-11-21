use rustc_hash::FxHashMap;
use std::{
    cmp::Ordering,
    sync::{Arc, RwLock},
};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id(u64);

const EXTERNAL: u64 = 1 << 63;

#[derive(Default)]
struct Pool {
    indices: FxHashMap<Arc<[u8]>, u64>,
    values: Vec<Arc<[u8]>>,
}

#[derive(Default)]
pub struct Ids(RwLock<Pool>);

impl Ids {
    #[inline]
    pub fn key(&self, bytes: &[u8]) -> Id {
        // Nine nonzero ASCII bytes fit losslessly in 63 bits. Pad on the
        // right so integer order is also lexical order for these keys.
        #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
        if bytes.len() == 8 || bytes.len() == 9 {
            let first = u64::from_be_bytes(bytes[..8].try_into().unwrap());
            let high = 0x8080_8080_8080_8080;
            let zeros = first.wrapping_sub(0x0101_0101_0101_0101) & !first & high;
            let ninth = bytes.get(8).copied().unwrap_or(0);
            if first & high == 0 && zeros == 0 && (bytes.len() == 8 || (1..128).contains(&ninth)) {
                let packed = std::arch::x86_64::_pext_u64(first, !high);
                return Id((packed << 7) | u64::from(ninth));
            }
        }
        if bytes.len() <= 9 && !bytes.is_empty() {
            let mut value = 0u64;
            let mut flags = 0u8;
            for &byte in bytes {
                flags |= byte | byte.wrapping_sub(1);
                value = (value << 7) | u64::from(byte);
            }
            if flags < 128 {
                return Id(value << ((9 - bytes.len()) * 7));
            }
        }
        self.external(bytes)
    }

    #[cold]
    fn external(&self, bytes: &[u8]) -> Id {
        if let Some(&index) = self.0.read().unwrap().indices.get(bytes) {
            return Id(EXTERNAL | index);
        }
        let mut pool = self.0.write().unwrap();
        if let Some(&index) = pool.indices.get(bytes) {
            return Id(EXTERNAL | index);
        }
        let index = pool.values.len() as u64;
        let value = Arc::<[u8]>::from(bytes);
        pool.values.push(value.clone());
        pool.indices.insert(value, index);
        Id(EXTERNAL | index)
    }

    pub fn bytes(&self, id: Id) -> Vec<u8> {
        if id.0 & EXTERNAL != 0 {
            return self.0.read().unwrap().values[(id.0 ^ EXTERNAL) as usize].to_vec();
        }
        (0..9)
            .rev()
            .map(|i| ((id.0 >> (7 * i)) & 127) as u8)
            .take_while(|&b| b != 0)
            .collect()
    }

    pub fn cmp(&self, a: Id, b: Id) -> Ordering {
        if a == b {
            return Ordering::Equal;
        }
        if (a.0 | b.0) < EXTERNAL {
            a.cmp(&b)
        } else {
            self.bytes(a).cmp(&self.bytes(b))
        }
    }
}

pub const SHARDS: usize = 128;
impl Id {
    #[inline]
    pub fn hash(self) -> u64 {
        (self.0 ^ self.0.rotate_left(29)).wrapping_mul(0x9e3779b97f4a7c15)
    }
    #[inline]
    pub fn shard(self) -> usize {
        (self.hash() >> (64 - SHARDS.trailing_zeros())) as usize
    }
}

#[derive(Default)]
pub struct Links {
    index: FxHashMap<Id, usize>,
    parent: Vec<usize>,
    size: Vec<usize>,
    smallest: Vec<Id>,
}

impl Links {
    fn index(&mut self, id: Id) -> usize {
        *self.index.entry(id).or_insert_with(|| {
            let i = self.parent.len();
            self.parent.push(i);
            self.size.push(1);
            self.smallest.push(id);
            i
        })
    }
    fn root(&mut self, mut i: usize) -> usize {
        while self.parent[i] != i {
            self.parent[i] = self.parent[self.parent[i]];
            i = self.parent[i];
        }
        i
    }
    pub fn join(&mut self, ids: &Ids, a: Id, b: Id) {
        let (i, j) = (self.index(a), self.index(b));
        let (mut a, mut b) = (self.root(i), self.root(j));
        if a == b {
            return;
        }
        if self.size[a] < self.size[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parent[b] = a;
        self.size[a] += self.size[b];
        if ids.cmp(self.smallest[b], self.smallest[a]).is_lt() {
            self.smallest[a] = self.smallest[b];
        }
    }
    pub fn finish(mut self) -> FxHashMap<Id, Id> {
        std::mem::take(&mut self.index)
            .into_iter()
            .filter_map(|(id, i)| {
                let root = self.root(i);
                let person = self.smallest[root];
                (id != person).then_some((id, person))
            })
            .collect()
    }
}
