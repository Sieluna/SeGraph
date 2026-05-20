use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Blob(BlobRef),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PropertyType {
    Int = 1,
    Float = 2,
    Text = 3,
    Bool = 4,
    Blob = 5,
}

impl PropertyType {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Int),
            2 => Some(Self::Float),
            3 => Some(Self::Text),
            4 => Some(Self::Bool),
            5 => Some(Self::Blob),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: u64,
    pub size_bytes: u64,
}

impl BlobRef {
    #[must_use]
    pub const fn new(hash: u64, size_bytes: u64) -> Self {
        Self { hash, size_bytes }
    }
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct Property {
    pub key: String,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct EntityData {
    pub id: u64,
    pub properties: Vec<Property>,
    pub blob_refs: Vec<(String, BlobRef)>,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct EdgeData {
    pub id: u64,
    pub source: u64,
    pub target: u64,
    pub label: u32,
    pub properties: Vec<Property>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct GetEntity {
    pub id: u64,
    pub want_properties: bool,
    pub want_blobs: bool,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct GetEdges {
    pub entity_id: u64,
    pub direction: Direction,
    pub label_filter: Vec<u32>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct Traverse {
    pub start_id: u64,
    pub depth: u32,
    pub edge_labels: Vec<u32>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub enum IndexQuery {
    Property {
        key: String,
        value: Value,
        limit: u32,
    },
    Range {
        key: String,
        min: Value,
        max: Value,
        limit: u32,
    },
    Spatial {
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct GetBlob {
    pub hash: u64,
    pub offset: u64,
    pub chunk_size: u32,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct BlobChunk {
    pub hash: u64,
    pub offset: u64,
    pub total_size: u64,
    pub data: Bytes,
}

#[derive(Clone, Debug, PartialEq, Archive, Serialize, Deserialize)]
pub struct BatchHeader {
    pub query_id: u32,
    pub entity_count: u16,
    pub flags: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct ServerStats {
    pub entities: u64,
    pub edges: u64,
    pub blob_bytes: u64,
    pub supported_codecs: CodecSet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct CodecSet(u16);

impl CodecSet {
    pub const RAW: Self = Self(1 << 0);
    pub const QUANTIZED_U16: Self = Self(1 << 1);
    pub const ZSTD: Self = Self(1 << 2);

    #[must_use]
    pub const fn all() -> Self {
        Self(Self::RAW.0 | Self::QUANTIZED_U16.0 | Self::ZSTD.0)
    }

    #[must_use]
    pub const fn contains(self, codec: Self) -> bool {
        self.0 & codec.0 != 0
    }
}

impl Default for CodecSet {
    fn default() -> Self {
        Self::all()
    }
}
