// GENERATED from schemas/universal-adapter.schema.yml.
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Envelope {
    pub canonical_event: String,
    pub message_id: String,
    pub interaction_id: String,
    pub correlation_id: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Delivery {
    pub delivery_id: String,
    pub canonical_event: String,
    pub transport_name: String,
}

#[derive(Debug, Clone, Copy)]
pub struct Capabilities {
    pub reliable: bool,
    pub ordered: bool,
    pub durable: bool,
    pub replay: bool,
    pub request_reply: bool,
    pub pub_sub: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum Health { Healthy, Degraded, Unhealthy }

#[derive(Debug)]
pub enum AdapterError {
    InvalidConfig(String),
    NotOpen,
    UnsupportedCapability(String),
    SecurityRequirementNotMet(String),
    MappingNotReversible,
    Transport(String),
}

pub trait UniversalAdapter: Send + Sync {
    fn open(&mut self, config_yaml: &str) -> Result<(), AdapterError>;
    fn close(&mut self);
    fn publish(&self, envelope: &Envelope) -> Result<(), AdapterError>;
    fn subscribe(&self, canonical_event: &str) -> Result<(), AdapterError>;
    fn ack(&self, delivery: &Delivery) -> Result<(), AdapterError>;
    fn nack(&self, delivery: &Delivery, reason: &str) -> Result<(), AdapterError>;
    fn health(&self) -> Health;
    fn capabilities(&self) -> Capabilities;
    fn map_canonical_to_transport(&self, canonical_event: &str) -> Result<String, AdapterError>;
    fn map_transport_to_canonical(&self, transport_name: &str) -> Result<String, AdapterError>;
}

pub type AdapterFactory = fn() -> Box<dyn UniversalAdapter>;
pub type AdapterRegistry = BTreeMap<String, AdapterFactory>;

pub fn assert_canonical_round_trip(adapter: &dyn UniversalAdapter, canonical: &str) -> Result<(), AdapterError> {
    let mapped = adapter.map_canonical_to_transport(canonical)?;
    let restored = adapter.map_transport_to_canonical(&mapped)?;
    if restored != canonical { return Err(AdapterError::MappingNotReversible); }
    Ok(())
}
