use std::collections::VecDeque;
use std::path::PathBuf;

use pallas::ledger::traverse::wellknown::GenesisValues;
use pallas::network::miniprotocols::Point;
use serde::Deserialize;
use utxorpc::proto::cardano::v1::Block;

pub mod errors;
pub mod model;

pub use errors::*;

use self::model::{BlockContext, CRDTCommand};

#[derive(Debug, Clone)]
pub enum Record {
    RawBlockPayload(Vec<u8>),
    EnrichedBlockPayload(Vec<u8>, BlockContext),
    UtxoRpcBlockPayload(Block),
    CRDTCommand(Vec<CRDTCommand>),
    SQLCommand(Vec<String>),
}

#[derive(Debug, Clone)]
pub enum ChainEvent {
    Apply(Point, Record),
    Undo(Point, Record),
    Reset(Point),
}

impl ChainEvent {
    pub fn apply(point: Point, record: impl Into<Record>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Apply(point, record.into()),
        }
    }

    pub fn undo(point: Point, record: impl Into<Record>) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Undo(point, record.into()),
        }
    }

    pub fn reset(point: Point) -> gasket::messaging::Message<Self> {
        gasket::messaging::Message {
            payload: Self::Reset(point),
        }
    }

    pub fn point(&self) -> &Point {
        match self {
            Self::Apply(x, _) => x,
            Self::Undo(x, _) => x,
            Self::Reset(x) => x,
        }
    }

    pub fn record(&self) -> Option<&Record> {
        match self {
            Self::Apply(_, x) => Some(x),
            Self::Undo(_, x) => Some(x),
            _ => None,
        }
    }

    pub fn map_record(self, f: fn(Record) -> Record) -> Self {
        match self {
            Self::Apply(p, x) => Self::Apply(p, f(x)),
            Self::Undo(p, x) => Self::Undo(p, f(x)),
            Self::Reset(x) => Self::Reset(x),
        }
    }

    pub fn try_map_record<E>(self, f: fn(Record) -> Result<Record, E>) -> Result<Self, E> {
        let out = match self {
            Self::Apply(p, x) => Self::Apply(p, f(x)?),
            Self::Undo(p, x) => Self::Undo(p, f(x)?),
            Self::Reset(x) => Self::Reset(x),
        };

        Ok(out)
    }

    pub fn try_map_record_to_many<E>(
        self,
        f: fn(Record) -> Result<Vec<Record>, E>,
    ) -> Result<Vec<Self>, E> {
        let out = match self {
            Self::Apply(p, x) => f(x)?
                .into_iter()
                .map(|i| Self::Apply(p.clone(), i))
                .collect(),
            Self::Undo(p, x) => f(x)?
                .into_iter()
                .map(|i| Self::Undo(p.clone(), i))
                .collect(),
            Self::Reset(x) => vec![Self::Reset(x)],
        };

        Ok(out)
    }
}

pub type SourceOutputPort = gasket::messaging::OutputPort<ChainEvent>;
pub type EnrichInputPort = gasket::messaging::InputPort<ChainEvent>;
pub type EnrichOutputPort = gasket::messaging::OutputPort<ChainEvent>;
pub type ReducerInputPort = gasket::messaging::InputPort<ChainEvent>;
pub type ReducerOutputPort = gasket::messaging::OutputPort<ChainEvent>;
pub type StorageInputPort = gasket::messaging::InputPort<ChainEvent>;
pub type StorageOutputPort = gasket::messaging::OutputPort<ChainEvent>;

pub type OutputAdapter = gasket::messaging::tokio::ChannelSendAdapter<ChainEvent>;
pub type InputAdapter = gasket::messaging::tokio::ChannelRecvAdapter<ChainEvent>;

pub trait StageBootstrapper {
    fn connect_output(&mut self, adapter: OutputAdapter);
    fn connect_input(&mut self, adapter: InputAdapter);
    fn spawn(self, policy: gasket::runtime::Policy) -> gasket::runtime::Tether;
}

#[derive(Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ChainConfig {
    Mainnet,
    Testnet,
    PreProd,
    Preview,
    Custom(GenesisValues),
}
impl Default for ChainConfig {
    fn default() -> Self {
        Self::Mainnet
    }
}
impl From<ChainConfig> for GenesisValues {
    fn from(other: ChainConfig) -> Self {
        match other {
            ChainConfig::Mainnet => GenesisValues::mainnet(),
            ChainConfig::Testnet => GenesisValues::testnet(),
            ChainConfig::PreProd => GenesisValues::preprod(),
            ChainConfig::Preview => GenesisValues::preview(),
            ChainConfig::Custom(x) => x,
        }
    }
}

const MAX_BREADCRUMBS: usize = 20;

#[derive(Clone)]
pub struct Breadcrumbs {
    state: VecDeque<Point>,
}

impl Breadcrumbs {
    pub fn new() -> Self {
        Self {
            state: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    pub fn from_points(points: Vec<Point>) -> Self {
        Self {
            state: VecDeque::from_iter(points),
        }
    }

    pub fn to_points(&self) -> Vec<Point> {
        self.state.iter().map(Clone::clone).collect()
    }

    pub fn from_data(data: Vec<(u64, String)>) -> Result<Self, Error> {
        let points: Vec<_> = data
            .into_iter()
            .map::<Result<_, Error>, _>(|(slot, hash)| {
                let hash = hex::decode(hash).map_err(Error::config)?;
                Ok(Point::Specific(slot, hash))
            })
            .collect::<Result<_, _>>()?;

        Ok(Breadcrumbs::from_points(points))
    }

    pub fn to_data(&self) -> Vec<(u64, String)> {
        self.to_points()
            .into_iter()
            .filter_map(|p| match p {
                Point::Origin => None,
                Point::Specific(slot, hash) => Some((slot, hex::encode(hash))),
            })
            .collect()
    }

    pub fn track(&mut self, point: Point) {
        // if we have a rollback, retain only older points
        self.state
            .retain(|p| p.slot_or_default() < point.slot_or_default());

        // add the new point we're tracking
        self.state.push_front(point);

        // if we have too many points, remove the older ones
        if self.state.len() > MAX_BREADCRUMBS {
            self.state.pop_back();
        }
    }

    pub fn latest_known_point(&self) -> Option<Point> {
        self.state.front().cloned()
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", content = "value")]
pub enum IntersectConfig {
    Tip,
    Origin,
    Point(u64, String),
    Breadcrumbs(Vec<(u64, String)>),
}

impl IntersectConfig {
    pub fn points(&self) -> Option<Vec<Point>> {
        match self {
            IntersectConfig::Breadcrumbs(all) => {
                let mapped = all
                    .iter()
                    .map(|(slot, hash)| {
                        let hash = hex::decode(hash).expect("valid hex hash");
                        Point::Specific(*slot, hash)
                    })
                    .collect();

                Some(mapped)
            }
            IntersectConfig::Point(slot, hash) => {
                let hash = hex::decode(hash).expect("valid hex hash");
                Some(vec![Point::Specific(*slot, hash)])
            }
            _ => None,
        }
    }
}

/// Optional configuration to stop processing new blocks after processing:
///   1. a block with the given hash
///   2. the first block on or after a given absolute slot
///   3. TODO: a total of X blocks
#[derive(Deserialize, Debug, Clone)]
pub struct FinalizeConfig {
    until_hash: Option<String>,
    max_block_slot: Option<u64>,
    // max_block_quantity: Option<u64>,
}

pub fn should_finalize(config: &Option<FinalizeConfig>, last_point: &Point) -> bool {
    let config = match config {
        Some(x) => x,
        None => return false,
    };

    if let Some(expected) = &config.until_hash {
        if let Point::Specific(_, current) = last_point {
            return expected == &hex::encode(current);
        }
    }

    if let Some(max) = config.max_block_slot {
        if last_point.slot_or_default() >= max {
            return true;
        }
    }

    false
}

pub struct Context {
    pub current_dir: PathBuf,
    pub chain: ChainConfig,
    pub intersect: IntersectConfig,
    pub cursor: Breadcrumbs,
    pub finalize: Option<FinalizeConfig>,
    pub storage_type: String,
}
