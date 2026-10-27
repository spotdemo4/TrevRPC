//! Connection metadata: request metadata a client sets once for all its calls, such as a bearer
//! token, so that it crosses each connection once instead of in every REQUEST.
//!
//! A client offers the set (`RpcRequest::connection_metadata` with entries) on its calls until a
//! HEADERS confirms that the server stored it for the connection, then refers to it by ID. Calls
//! on different streams reach the server in any order, so a reference only follows a
//! confirmation. The server merges the set into each call's metadata before validation and
//! authorization, the call's own entries winning; see the wiki's Connection Metadata section.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::sync::{MutexExt as _, RwLockExt as _};
use crate::wire::{
    ConnectionMetadata, Metadata, RpcRequest, normalize_metadata_key, validate_metadata,
};
use crate::{Result, Status};

/// How many sets a server keeps per connection: the newest, and a few the client's calls in
/// flight may still refer to.
const MAX_SETS: usize = 4;

/// One version of a channel's connection metadata.
struct Set {
    id: u32,
    metadata: Metadata,
}

/// A channel's connection metadata, shared by its connections: the current set, whose ID
/// changes with it.
#[derive(Default)]
pub struct ConnectionMetadataSource {
    current: RwLock<Option<Arc<Set>>>,
    ids: AtomicU32,
}

impl ConnectionMetadataSource {
    /// A source whose connection metadata starts as `metadata`.
    pub(crate) fn new(metadata: Metadata) -> Result<Arc<Self>> {
        let source = Arc::new(Self::default());
        source.set(metadata)?;
        Ok(source)
    }

    /// Replaces the connection metadata; empty metadata removes it.
    pub(crate) fn set(&self, metadata: Metadata) -> Result<()> {
        let set = if metadata.is_empty() {
            None
        } else {
            let metadata: Metadata = metadata
                .into_iter()
                .map(|(key, value)| (normalize_metadata_key(&key), value))
                .collect();
            validate_metadata(&metadata)?;
            let mut id = self.ids.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            if id == 0 {
                // IDs wrap after 2^32 sets, past 0, which means none.
                id = self.ids.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
            }
            Some(Arc::new(Set { id, metadata }))
        };
        *self.current.write_unpoisoned() = set;
        Ok(())
    }

    fn current(&self) -> Option<Arc<Set>> {
        self.current.read_unpoisoned().clone()
    }
}

/// A connection's view of its channel's connection metadata: the set the server confirmed on
/// this connection.
pub struct ClientConnectionMetadata {
    source: Arc<ConnectionMetadataSource>,
    confirmed: AtomicU32,
}

/// A call that offered its connection's set, which the call's HEADERS may confirm.
#[derive(Debug)]
pub struct OfferedSet {
    connection: Arc<ClientConnectionMetadata>,
}

impl std::fmt::Debug for ClientConnectionMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientConnectionMetadata")
            .field("confirmed", &self.confirmed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ClientConnectionMetadata {
    /// The state of a new connection of the channel whose connection metadata is `source`.
    pub(crate) fn new(source: Arc<ConnectionMetadataSource>) -> Arc<Self> {
        Arc::new(Self {
            source,
            confirmed: AtomicU32::new(0),
        })
    }

    /// Sets the connection metadata of `request`: a reference to the current set once the
    /// connection confirmed it, an offer of it otherwise, which the call's HEADERS may confirm.
    pub(crate) fn prepare(self: &Arc<Self>, request: &mut RpcRequest) -> Option<OfferedSet> {
        let Some(set) = self.source.current() else {
            request.connection_metadata = None;
            return None;
        };
        if self.confirmed.load(Ordering::Relaxed) == set.id {
            request.connection_metadata = Some(ConnectionMetadata {
                id: set.id,
                metadata: Metadata::new(),
            });
            return None;
        }
        request.connection_metadata = Some(ConnectionMetadata {
            id: set.id,
            metadata: set.metadata.clone(),
        });
        Some(OfferedSet {
            connection: Arc::clone(self),
        })
    }

    /// The set the server confirmed on this connection, zero for none.
    #[cfg(test)]
    pub(crate) fn confirmed(&self) -> u32 {
        self.confirmed.load(Ordering::Relaxed)
    }
}

impl OfferedSet {
    /// Records the server's answer in the call's HEADERS: `confirmed` is the set it stored. Only
    /// the current set counts: a confirmation of one the channel has since replaced would not
    /// make calls refer to the new one.
    pub(crate) fn learn(self, confirmed: u32) {
        if confirmed != 0
            && let Some(set) = self.connection.source.current()
            && set.id == confirmed
        {
            self.connection
                .confirmed
                .store(confirmed, Ordering::Relaxed);
        }
    }
}

/// The connection metadata a server stored for one connection, `QMux` session or `WebTransport`
/// session.
#[derive(Debug, Default)]
pub struct ConnectionMetadataStore {
    sets: Mutex<Vec<(u32, Arc<Metadata>)>>,
}

impl ConnectionMetadataStore {
    fn store(&self, id: u32, metadata: Arc<Metadata>) {
        let mut sets = self.sets.lock_unpoisoned();
        sets.retain(|(stored, _)| *stored != id);
        if sets.len() == MAX_SETS {
            sets.remove(0);
        }
        sets.push((id, metadata));
    }

    fn lookup(&self, id: u32) -> Option<Arc<Metadata>> {
        self.sets
            .lock_unpoisoned()
            .iter()
            .find(|(stored, _)| *stored == id)
            .map(|(_, metadata)| Arc::clone(metadata))
    }
}

/// Merges the connection metadata `request` offers or refers to into its metadata, the call's own
/// entries winning, and stores an offered set in `store`. Returns the ID of the stored set, which
/// the call's HEADERS confirm, or zero.
///
/// `store` is `None` on a connection that keeps no connection metadata, the ordinary HTTP
/// mapping's: an offer serves its own call only, and references are unknown.
///
/// # Errors
///
/// `InvalidArgument` for an offer whose metadata is invalid, or a reference to a set the
/// connection does not hold.
pub fn resolve(
    store: Option<&ConnectionMetadataStore>,
    request: &mut RpcRequest,
) -> std::result::Result<u32, Status> {
    let Some(ConnectionMetadata { id, metadata }) = request.connection_metadata.take() else {
        return Ok(0);
    };
    let (set, confirmed) = if metadata.is_empty() {
        let set = store.and_then(|store| store.lookup(id)).ok_or_else(|| {
            Status::invalid_argument(format!("unknown connection metadata set {id}"))
        })?;
        (set, 0)
    } else {
        validate_metadata(&metadata)?;
        let set = Arc::new(metadata);
        match store {
            Some(store) => {
                store.store(id, Arc::clone(&set));
                (set, id)
            }
            None => (set, 0),
        }
    };
    for (key, value) in set.iter() {
        request
            .metadata
            .entry(key.clone())
            .or_insert_with(|| value.clone());
    }
    Ok(confirmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn offer(id: u32, value: &str) -> RpcRequest {
        let mut request = RpcRequest::new("/svc/m");
        request.connection_metadata = Some(ConnectionMetadata {
            id,
            metadata: [
                ("k".to_owned(), value.as_bytes().to_vec()),
                ("shared".to_owned(), value.as_bytes().to_vec()),
            ]
            .into_iter()
            .collect(),
        });
        request
    }

    fn reference(id: u32) -> RpcRequest {
        let mut request = RpcRequest::new("/svc/m");
        request
            .metadata
            .insert("shared".to_owned(), b"call".to_vec());
        request.connection_metadata = Some(ConnectionMetadata {
            id,
            metadata: Metadata::new(),
        });
        request
    }

    #[test]
    fn offers_are_stored_and_merged_and_the_oldest_set_is_dropped() {
        let store = ConnectionMetadataStore::default();
        for id in 1..=5_u32 {
            let value = id.to_string();
            let mut request = offer(id, &value);
            assert_eq!(resolve(Some(&store), &mut request), Ok(id));
            assert_eq!(request.metadata["k"], value.as_bytes());
        }
        assert_eq!(
            resolve(Some(&store), &mut reference(1)).map_err(|status| status.code()),
            Err(crate::Code::InvalidArgument)
        );
        let mut request = reference(2);
        assert_eq!(resolve(Some(&store), &mut request), Ok(0));
        assert_eq!(request.metadata["k"], b"2");
        assert_eq!(request.metadata["shared"], b"call");
    }

    #[test]
    fn invalid_offers_and_references_without_a_store_fail() {
        let store = ConnectionMetadataStore::default();
        let mut bad = RpcRequest::new("/svc/m");
        bad.connection_metadata = Some(ConnectionMetadata {
            id: 9,
            metadata: Metadata::from([("trevrpc-x".to_owned(), b"1".to_vec())]),
        });
        assert!(resolve(Some(&store), &mut bad).is_err());

        let mut request = offer(1, "x");
        assert_eq!(resolve(None, &mut request), Ok(0));
        assert_eq!(request.metadata["k"], b"x");
        assert!(resolve(None, &mut reference(1)).is_err());
    }

    #[test]
    fn a_client_offers_until_confirmed_then_refers() {
        let source =
            ConnectionMetadataSource::new(Metadata::from([("Suffix".to_owned(), b"!".to_vec())]))
                .unwrap();
        let connection = ClientConnectionMetadata::new(Arc::clone(&source));
        let mut request = RpcRequest::new("/svc/m");
        let offered = connection.prepare(&mut request).unwrap();
        let sent = request.connection_metadata.clone().unwrap();
        assert_eq!(
            (sent.id, sent.metadata["suffix"].as_slice()),
            (1, &b"!"[..])
        );
        offered.learn(1);
        assert_eq!(connection.confirmed(), 1);
        assert!(connection.prepare(&mut request).is_none());
        assert!(request.connection_metadata.unwrap().metadata.is_empty());

        source
            .set(Metadata::from([("suffix".to_owned(), b"#".to_vec())]))
            .unwrap();
        let mut request = RpcRequest::new("/svc/m");
        let offered = connection.prepare(&mut request).unwrap();
        assert_eq!(request.connection_metadata.unwrap().id, 2);
        // A confirmation of a replaced set does not count.
        offered.learn(1);
        assert_eq!(connection.confirmed(), 1);
        assert!(connection.prepare(&mut RpcRequest::new("/svc/m")).is_some());
        source.set(Metadata::new()).unwrap();
        let mut request = RpcRequest::new("/svc/m");
        assert!(connection.prepare(&mut request).is_none());
        assert!(request.connection_metadata.is_none());
    }
}
