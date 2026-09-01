use std::sync::Arc;

use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

use crate::storage::OwnerIdentity;

#[derive(Debug, Clone)]
pub struct OwnerIdentityHandle {
    inner: Arc<RwLock<OwnerIdentity>>,
}

impl OwnerIdentityHandle {
    #[must_use]
    pub fn new(identity: OwnerIdentity) -> Self {
        Self {
            inner: Arc::new(RwLock::new(identity)),
        }
    }

    pub async fn snapshot(&self) -> OwnerIdentity {
        self.inner.read().await.clone()
    }

    pub(crate) async fn write_gate(&self) -> OwnedRwLockWriteGuard<OwnerIdentity> {
        Arc::clone(&self.inner).write_owned().await
    }
}
