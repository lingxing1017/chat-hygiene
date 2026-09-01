use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, Stat, fchown, fstat, fsync, linkat, open, openat, statat,
    unlinkat,
};
use rustix::io::{Errno, dup, read, write};
use rustix::process;
use rustix::process::{getegid, geteuid};
use secrecy::zeroize::Zeroizing;
use secrecy::{ExposeSecret, SecretSlice};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::storage::{OwnerIdentity, ServiceDatabaseDescriptor};

const TARGET_NAME: &str = "claim-code";
const TEMP_NAME: &str = ".claim-code.tmp";
const COMMAND_LEN: usize = 72;
const MAX_READ_LEN: usize = COMMAND_LEN + 1;

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum ClaimFileError {
    #[error("claim-code token is invalid")]
    InvalidToken,
    #[error("claim-code entry is unsafe or belongs to another installation")]
    UnsafeEntry,
    #[error("claim-code filesystem operation failed: {0}")]
    Filesystem(&'static str),
}

#[derive(Clone)]
pub struct ClaimFileManager {
    directory: Arc<OwnedFd>,
    directory_identity: Stat,
    expected_uid: u32,
    expected_gid: u32,
    service_uid: u32,
    service_gid: u32,
    #[cfg(test)]
    injected_failure: Option<Arc<Mutex<Option<InjectedFailure>>>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InjectedFailure {
    AfterCreate,
    AfterOwnership,
    PartialWrite,
    BeforeFileSync,
    BeforePublish,
    BeforePublishDirectorySync,
    BeforeCleanup,
    BeforeCleanupDirectorySync,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryContent {
    Exact,
    Partial,
}

struct Entry {
    _fd: OwnedFd,
    stat: Stat,
    content: EntryContent,
}

impl ClaimFileManager {
    #[allow(dead_code)]
    pub(crate) fn from_database_descriptor(
        database: &ServiceDatabaseDescriptor,
    ) -> Result<Self, ClaimFileError> {
        let directory = database
            .database_path()
            .parent()
            .ok_or(ClaimFileError::Filesystem("database parent is unavailable"))?
            .to_path_buf();
        let directory_fd = open_directory(&directory)?;
        let stat = fstat(&directory_fd)
            .map_err(|_| ClaimFileError::Filesystem("cannot inspect claim-code directory"))?;
        let effective_user = geteuid();
        let effective_group = getegid();
        if !effective_user.is_root() && stat.st_uid != effective_user.as_raw() {
            return Err(ClaimFileError::Filesystem(
                "claim-code directory is not service-owned",
            ));
        }
        Ok(Self {
            directory: Arc::new(directory_fd),
            directory_identity: stat,
            expected_uid: stat.st_uid,
            expected_gid: stat.st_gid,
            service_uid: effective_user.as_raw(),
            service_gid: effective_group.as_raw(),
            #[cfg(test)]
            injected_failure: None,
        })
    }

    #[allow(dead_code)]
    pub(crate) fn validate_existing_before_reconciliation(
        &self,
        token: &SecretSlice<u8>,
    ) -> Result<(), ClaimFileError> {
        let expected = expected_command(token)?;
        let directory = self.open_directory()?;
        let target = self.inspect_entry(&directory, TARGET_NAME, false, &expected)?;
        let temp = self.inspect_entry(&directory, TEMP_NAME, true, &expected)?;
        validate_pair(target.as_ref(), temp.as_ref())
    }

    /// Reconciles the protected claim-code artifacts with persisted Owner state.
    ///
    /// # Errors
    ///
    /// Returns a value-free error for an invalid token, unsafe entry, or
    /// filesystem failure.
    pub fn reconcile(
        &self,
        owner: &OwnerIdentity,
        token: &SecretSlice<u8>,
    ) -> Result<(), ClaimFileError> {
        match owner {
            OwnerIdentity::Unclaimed => self.reconcile_unclaimed(token),
            OwnerIdentity::Claimed { .. } => self.retire_after_claim(token),
        }
    }

    /// Removes only exact safe claim-code artifacts after Owner commit.
    ///
    /// # Errors
    ///
    /// Returns a value-free error for an invalid token, unsafe entry, or
    /// filesystem failure.
    pub fn retire_after_claim(&self, token: &SecretSlice<u8>) -> Result<(), ClaimFileError> {
        let expected = expected_command(token)?;
        let directory = self.open_directory()?;
        let target = self.inspect_entry(&directory, TARGET_NAME, false, &expected)?;
        let temp = self.inspect_entry(&directory, TEMP_NAME, true, &expected)?;
        validate_pair(target.as_ref(), temp.as_ref())?;
        if let Some(target) = target.as_ref() {
            Self::unlink_verified(&directory, TARGET_NAME, target)?;
            sync_directory(&directory)?;
        }
        if let Some(temp) = temp.as_ref()
            && path_matches_entry(&directory, TEMP_NAME, temp)?
        {
            Self::unlink_verified(&directory, TEMP_NAME, temp)?;
            sync_directory(&directory)?;
        }
        Ok(())
    }

    fn reconcile_unclaimed(&self, token: &SecretSlice<u8>) -> Result<(), ClaimFileError> {
        let expected = expected_command(token)?;
        let directory = self.open_directory()?;
        let target = self.inspect_entry(&directory, TARGET_NAME, false, &expected)?;
        let temp = self.inspect_entry(&directory, TEMP_NAME, true, &expected)?;
        validate_pair(target.as_ref(), temp.as_ref())?;
        if target.is_some() {
            if let Some(temp) = temp.as_ref()
                && path_matches_entry(&directory, TEMP_NAME, temp)?
            {
                Self::unlink_verified(&directory, TEMP_NAME, temp)?;
                sync_directory(&directory)?;
            }
            return Ok(());
        }
        if let Some(temp) = temp.as_ref() {
            Self::unlink_verified(&directory, TEMP_NAME, temp)?;
            sync_directory(&directory)?;
        }
        self.create_and_publish(&directory, &expected)
    }

    fn create_and_publish(
        &self,
        directory: &OwnedFd,
        expected: &[u8],
    ) -> Result<(), ClaimFileError> {
        let temp = openat(
            directory,
            TEMP_NAME,
            OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| ClaimFileError::Filesystem("cannot create claim-code temporary file"))?;
        #[cfg(test)]
        self.fail_if(InjectedFailure::AfterCreate)?;
        if geteuid().is_root() {
            fchown(
                &temp,
                Some(process::Uid::from_raw(self.expected_uid)),
                Some(process::Gid::from_raw(self.expected_gid)),
            )
            .map_err(|_| ClaimFileError::Filesystem("cannot set claim-code ownership"))?;
        }
        self.verify_new_temp(&temp)?;
        #[cfg(test)]
        self.fail_if(InjectedFailure::AfterOwnership)?;
        #[cfg(test)]
        if self.take_failure(InjectedFailure::PartialWrite) {
            write_all(&temp, &expected[..19])?;
            return Err(ClaimFileError::Filesystem("cannot write claim-code entry"));
        }
        write_all(&temp, expected)?;
        #[cfg(test)]
        self.fail_if(InjectedFailure::BeforeFileSync)?;
        fsync(&temp).map_err(|_| ClaimFileError::Filesystem("cannot sync claim-code file"))?;
        #[cfg(test)]
        self.fail_if(InjectedFailure::BeforePublish)?;
        linkat(
            directory,
            TEMP_NAME,
            directory,
            TARGET_NAME,
            AtFlags::empty(),
        )
        .map_err(|_| ClaimFileError::Filesystem("cannot publish claim-code file"))?;
        #[cfg(test)]
        self.fail_if(InjectedFailure::BeforePublishDirectorySync)?;
        sync_directory(directory)?;
        let published_temp = Entry {
            stat: fstat(&temp)
                .map_err(|_| ClaimFileError::Filesystem("cannot inspect published claim-code"))?,
            _fd: temp,
            content: EntryContent::Exact,
        };
        #[cfg(test)]
        self.fail_if(InjectedFailure::BeforeCleanup)?;
        Self::unlink_verified(directory, TEMP_NAME, &published_temp)?;
        #[cfg(test)]
        self.fail_if(InjectedFailure::BeforeCleanupDirectorySync)?;
        sync_directory(directory)
    }

    fn verify_new_temp(&self, fd: &OwnedFd) -> Result<(), ClaimFileError> {
        let stat = fstat(fd)
            .map_err(|_| ClaimFileError::Filesystem("cannot inspect claim-code temporary file"))?;
        if !FileType::from_raw_mode(stat.st_mode).is_file()
            || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
            || stat.st_uid != self.expected_uid
            || stat.st_gid != self.expected_gid
            || stat.st_nlink != 1
        {
            return Err(ClaimFileError::UnsafeEntry);
        }
        Ok(())
    }

    fn inspect_entry(
        &self,
        directory: &OwnedFd,
        name: &str,
        temporary: bool,
        expected: &[u8],
    ) -> Result<Option<Entry>, ClaimFileError> {
        let fd = match openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return Ok(None),
            Err(_) => return Err(ClaimFileError::UnsafeEntry),
        };
        let stat = fstat(&fd).map_err(|_| ClaimFileError::UnsafeEntry)?;
        if !FileType::from_raw_mode(stat.st_mode).is_file()
            || Mode::from_raw_mode(stat.st_mode) != Mode::RUSR | Mode::WUSR
            || stat.st_nlink == 0
            || stat.st_nlink > 2
            || stat.st_size < 0
            || usize::try_from(stat.st_size).map_or(true, |size| size > MAX_READ_LEN)
        {
            return Err(ClaimFileError::UnsafeEntry);
        }
        let expected_owner = stat.st_uid == self.expected_uid && stat.st_gid == self.expected_gid;
        let service_owner = stat.st_uid == self.service_uid && stat.st_gid == self.service_gid;
        if !(expected_owner || temporary && service_owner && stat.st_nlink == 1) {
            return Err(ClaimFileError::UnsafeEntry);
        }
        let bytes = read_capped(&fd, MAX_READ_LEN)?;
        let content =
            if bytes.len() == expected.len() && bool::from(bytes.as_slice().ct_eq(expected)) {
                EntryContent::Exact
            } else if temporary && bytes.len() < expected.len() && expected.starts_with(&bytes) {
                EntryContent::Partial
            } else {
                return Err(ClaimFileError::UnsafeEntry);
            };
        if content == EntryContent::Exact && !expected_owner {
            return Err(ClaimFileError::UnsafeEntry);
        }
        Ok(Some(Entry {
            _fd: fd,
            stat,
            content,
        }))
    }

    fn unlink_verified(
        directory: &OwnedFd,
        name: &str,
        entry: &Entry,
    ) -> Result<(), ClaimFileError> {
        if !path_matches_entry(directory, name, entry)? {
            return Err(ClaimFileError::UnsafeEntry);
        }
        unlinkat(directory, name, AtFlags::empty())
            .map_err(|_| ClaimFileError::Filesystem("cannot remove claim-code entry"))
    }

    fn open_directory(&self) -> Result<OwnedFd, ClaimFileError> {
        let directory = dup(self.directory.as_ref())
            .map_err(|_| ClaimFileError::Filesystem("cannot open claim-code directory"))?;
        let stat = fstat(&directory)
            .map_err(|_| ClaimFileError::Filesystem("cannot inspect claim-code directory"))?;
        if !FileType::from_raw_mode(stat.st_mode).is_dir()
            || stat.st_dev != self.directory_identity.st_dev
            || stat.st_ino != self.directory_identity.st_ino
            || stat.st_uid != self.expected_uid
            || stat.st_gid != self.expected_gid
        {
            return Err(ClaimFileError::UnsafeEntry);
        }
        Ok(directory)
    }

    #[cfg(test)]
    fn with_injected_failure(mut self, failure: InjectedFailure) -> Self {
        self.injected_failure = Some(Arc::new(Mutex::new(Some(failure))));
        self
    }

    #[cfg(test)]
    fn take_failure(&self, point: InjectedFailure) -> bool {
        let Some(failure) = &self.injected_failure else {
            return false;
        };
        let mut failure = failure.lock().expect("claim-file failure lock poisoned");
        if failure.as_ref() == Some(&point) {
            *failure = None;
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    fn fail_if(&self, point: InjectedFailure) -> Result<(), ClaimFileError> {
        if self.take_failure(point) {
            Err(ClaimFileError::Filesystem("injected claim-code failure"))
        } else {
            Ok(())
        }
    }
}

fn expected_command(token: &SecretSlice<u8>) -> Result<Zeroizing<Vec<u8>>, ClaimFileError> {
    if token.expose_secret().len() != 32 {
        return Err(ClaimFileError::InvalidToken);
    }
    let mut command = Zeroizing::new(vec![0_u8; COMMAND_LEN]);
    command[..7].copy_from_slice(b"/claim ");
    hex::encode_to_slice(token.expose_secret(), &mut command[7..71])
        .map_err(|_| ClaimFileError::InvalidToken)?;
    command[71] = b'\n';
    Ok(command)
}

fn open_directory(path: &PathBuf) -> Result<OwnedFd, ClaimFileError> {
    open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::DIRECTORY,
        Mode::empty(),
    )
    .map_err(|_| ClaimFileError::Filesystem("cannot open claim-code directory"))
}

fn validate_pair(target: Option<&Entry>, temp: Option<&Entry>) -> Result<(), ClaimFileError> {
    if let Some(target) = target {
        if target.content != EntryContent::Exact {
            return Err(ClaimFileError::UnsafeEntry);
        }
        match temp {
            None if target.stat.st_nlink != 1 => return Err(ClaimFileError::UnsafeEntry),
            Some(temp) if same_inode(target, temp) => {
                if target.stat.st_nlink != 2
                    || temp.stat.st_nlink != 2
                    || temp.content != EntryContent::Exact
                {
                    return Err(ClaimFileError::UnsafeEntry);
                }
            }
            Some(temp) if target.stat.st_nlink != 1 || temp.stat.st_nlink != 1 => {
                return Err(ClaimFileError::UnsafeEntry);
            }
            _ => {}
        }
    } else if let Some(temp) = temp
        && temp.stat.st_nlink != 1
    {
        return Err(ClaimFileError::UnsafeEntry);
    }
    Ok(())
}

fn same_inode(left: &Entry, right: &Entry) -> bool {
    left.stat.st_dev == right.stat.st_dev && left.stat.st_ino == right.stat.st_ino
}

fn path_matches_entry(
    directory: &OwnedFd,
    name: &str,
    entry: &Entry,
) -> Result<bool, ClaimFileError> {
    let current = statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)
        .map_err(|_| ClaimFileError::UnsafeEntry)?;
    Ok(current.st_dev == entry.stat.st_dev && current.st_ino == entry.stat.st_ino)
}

fn read_capped(fd: &OwnedFd, capacity: usize) -> Result<Zeroizing<Vec<u8>>, ClaimFileError> {
    let mut bytes = Zeroizing::new(vec![0_u8; capacity]);
    let mut offset = 0;
    while offset < capacity {
        let count = read(fd, &mut bytes[offset..])
            .map_err(|_| ClaimFileError::Filesystem("cannot read claim-code entry"))?;
        if count == 0 {
            break;
        }
        offset += count;
    }
    bytes.truncate(offset);
    Ok(bytes)
}

fn write_all(fd: &OwnedFd, bytes: &[u8]) -> Result<(), ClaimFileError> {
    let mut offset = 0;
    while offset < bytes.len() {
        let count = write(fd, &bytes[offset..])
            .map_err(|_| ClaimFileError::Filesystem("cannot write claim-code entry"))?;
        if count == 0 {
            return Err(ClaimFileError::Filesystem("cannot write claim-code entry"));
        }
        offset += count;
    }
    Ok(())
}

fn sync_directory(directory: &OwnedFd) -> Result<(), ClaimFileError> {
    fsync(directory).map_err(|_| ClaimFileError::Filesystem("cannot sync claim-code directory"))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    use secrecy::SecretSlice;

    use super::*;
    use crate::storage::ServiceDatabaseDescriptor;

    async fn manager() -> (
        tempfile::TempDir,
        ClaimFileManager,
        SecretSlice<u8>,
        ServiceDatabaseDescriptor,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let descriptor =
            ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path()).unwrap();
        let manager = ClaimFileManager::from_database_descriptor(&descriptor).unwrap();
        let pool = descriptor.connect().await.unwrap();
        pool.close().await;
        (
            directory,
            manager,
            SecretSlice::from(vec![0x11; 32]),
            descriptor,
        )
    }

    fn read_target(manager: &ClaimFileManager) -> Zeroizing<Vec<u8>> {
        let directory = manager.open_directory().unwrap();
        let fd = openat(
            &directory,
            TARGET_NAME,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
            Mode::empty(),
        )
        .unwrap();
        read_capped(&fd, MAX_READ_LEN).unwrap()
    }

    #[tokio::test]
    async fn unclaimed_restart_recreates_same_code_deletion_is_not_revocation() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .validate_existing_before_reconciliation(&token)
            .unwrap();
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let first = read_target(&manager);
        assert_eq!(first.len(), COMMAND_LEN);
        assert_eq!(&first[..7], b"/claim ");
        assert_eq!(first[71], b'\n');
        assert!(first[7..71].iter().all(u8::is_ascii_hexdigit));
        let metadata = std::fs::symlink_metadata(directory.path().join(TARGET_NAME)).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);

        std::fs::remove_file(directory.path().join(TARGET_NAME)).unwrap();
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let recreated = read_target(&manager);
        assert!(bool::from(first.as_slice().ct_eq(&recreated)));
    }

    #[tokio::test]
    async fn claimed_state_retires_only_an_exact_safe_copy() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        manager
            .reconcile(
                &OwnerIdentity::Claimed {
                    owner_user_id: 100,
                    owner_chat_id: 500,
                    owner_chat_source: crate::storage::OwnerChatSource::Claim,
                    connection_floor_established_at: Some(1),
                    bound_at: chrono::Utc::now(),
                },
                &token,
            )
            .unwrap();
        assert!(!directory.path().join(TARGET_NAME).exists());
        assert!(!directory.path().join(TEMP_NAME).exists());
    }

    #[tokio::test]
    async fn claimed_restart_retires_an_exact_interrupted_temp() {
        let (directory, manager, token, _descriptor) = manager().await;
        let failing = manager
            .clone()
            .with_injected_failure(InjectedFailure::BeforeFileSync);
        assert!(
            failing
                .reconcile(&OwnerIdentity::Unclaimed, &token)
                .is_err()
        );
        assert!(!directory.path().join(TARGET_NAME).exists());
        assert!(directory.path().join(TEMP_NAME).exists());

        manager
            .reconcile(
                &OwnerIdentity::Claimed {
                    owner_user_id: 100,
                    owner_chat_id: 500,
                    owner_chat_source: crate::storage::OwnerChatSource::Claim,
                    connection_floor_established_at: None,
                    bound_at: chrono::Utc::now(),
                },
                &token,
            )
            .unwrap();
        assert!(!directory.path().join(TARGET_NAME).exists());
        assert!(!directory.path().join(TEMP_NAME).exists());
    }

    #[tokio::test]
    async fn partial_safe_temp_converges_without_publishing_partial_content() {
        let (directory, manager, token, _descriptor) = manager().await;
        let expected = expected_command(&token).unwrap();
        let directory_fd = manager.open_directory().unwrap();
        let temp = openat(
            &directory_fd,
            TEMP_NAME,
            OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        )
        .unwrap();
        write_all(&temp, &expected[..19]).unwrap();
        fsync(&temp).unwrap();
        drop(temp);
        assert!(!directory.path().join(TARGET_NAME).exists());

        manager
            .validate_existing_before_reconciliation(&token)
            .unwrap();
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        assert!(bool::from(
            read_target(&manager).as_slice().ct_eq(expected.as_slice())
        ));
        assert!(!directory.path().join(TEMP_NAME).exists());
    }

    #[tokio::test]
    async fn every_publication_failure_leaves_a_restart_convergent_state() {
        for failure in [
            InjectedFailure::AfterCreate,
            InjectedFailure::AfterOwnership,
            InjectedFailure::PartialWrite,
            InjectedFailure::BeforeFileSync,
            InjectedFailure::BeforePublish,
            InjectedFailure::BeforePublishDirectorySync,
            InjectedFailure::BeforeCleanup,
            InjectedFailure::BeforeCleanupDirectorySync,
        ] {
            let (directory, manager, token, _descriptor) = manager().await;
            let failing = manager.clone().with_injected_failure(failure);
            assert!(
                failing
                    .reconcile(&OwnerIdentity::Unclaimed, &token)
                    .is_err(),
                "failure point was not exercised: {failure:?}"
            );
            let target = directory.path().join(TARGET_NAME);
            let temp = directory.path().join(TEMP_NAME);
            match failure {
                InjectedFailure::AfterCreate | InjectedFailure::AfterOwnership => {
                    assert_eq!(std::fs::metadata(&temp).unwrap().len(), 0);
                    assert!(!target.exists());
                }
                InjectedFailure::PartialWrite => {
                    assert_eq!(std::fs::metadata(&temp).unwrap().len(), 19);
                    assert!(!target.exists());
                }
                InjectedFailure::BeforeFileSync | InjectedFailure::BeforePublish => {
                    assert_eq!(std::fs::metadata(&temp).unwrap().len(), COMMAND_LEN as u64);
                    assert!(!target.exists());
                }
                InjectedFailure::BeforePublishDirectorySync | InjectedFailure::BeforeCleanup => {
                    assert_eq!(
                        std::fs::metadata(&target).unwrap().len(),
                        COMMAND_LEN as u64
                    );
                    assert_eq!(std::fs::metadata(&temp).unwrap().len(), COMMAND_LEN as u64);
                }
                InjectedFailure::BeforeCleanupDirectorySync => {
                    assert_eq!(
                        std::fs::metadata(&target).unwrap().len(),
                        COMMAND_LEN as u64
                    );
                    assert!(!temp.exists());
                }
            }

            manager
                .validate_existing_before_reconciliation(&token)
                .unwrap();
            manager
                .reconcile(&OwnerIdentity::Unclaimed, &token)
                .unwrap();
            let expected = expected_command(&token).unwrap();
            assert!(bool::from(
                read_target(&manager).as_slice().ct_eq(expected.as_slice())
            ));
            assert!(!directory.path().join(TEMP_NAME).exists());
            let metadata = std::fs::symlink_metadata(directory.path().join(TARGET_NAME)).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
            assert_eq!(metadata.nlink(), 1);
        }
    }

    #[tokio::test]
    async fn complete_temp_and_hard_link_crash_shapes_converge_safely() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let target = directory.path().join(TARGET_NAME);
        let temp = directory.path().join(TEMP_NAME);

        std::fs::hard_link(&target, &temp).unwrap();
        manager
            .validate_existing_before_reconciliation(&token)
            .unwrap();
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        assert!(!temp.exists());
        assert_eq!(std::fs::metadata(&target).unwrap().nlink(), 1);

        let expected = expected_command(&token).unwrap();
        let directory_fd = manager.open_directory().unwrap();
        let distinct_temp = openat(
            &directory_fd,
            TEMP_NAME,
            OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        )
        .unwrap();
        write_all(&distinct_temp, &expected).unwrap();
        fsync(&distinct_temp).unwrap();
        drop(distinct_temp);
        manager
            .validate_existing_before_reconciliation(&token)
            .unwrap();
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        assert!(!temp.exists());
        assert!(target.exists());
    }

    #[tokio::test]
    async fn unexpected_hard_links_types_and_owner_are_never_removed() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let target = directory.path().join(TARGET_NAME);
        let extra_one = directory.path().join("extra-one");
        let extra_two = directory.path().join("extra-two");
        std::fs::hard_link(&target, &extra_one).unwrap();
        std::fs::hard_link(&target, &extra_two).unwrap();
        assert_eq!(std::fs::metadata(&target).unwrap().nlink(), 3);
        assert_eq!(
            manager.reconcile(&OwnerIdentity::Unclaimed, &token),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert!(target.exists() && extra_one.exists() && extra_two.exists());

        std::fs::remove_file(&extra_one).unwrap();
        std::fs::remove_file(&extra_two).unwrap();
        let mut wrong_owner_manager = manager.clone();
        wrong_owner_manager.expected_uid = wrong_owner_manager.expected_uid.wrapping_add(1);
        assert_eq!(
            wrong_owner_manager.retire_after_claim(&token),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert_eq!(
            wrong_owner_manager.reconcile(&OwnerIdentity::Unclaimed, &token),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert_eq!(
            wrong_owner_manager.reconcile(
                &OwnerIdentity::Claimed {
                    owner_user_id: 100,
                    owner_chat_id: 500,
                    owner_chat_source: crate::storage::OwnerChatSource::Claim,
                    connection_floor_established_at: None,
                    bound_at: chrono::Utc::now(),
                },
                &token,
            ),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert!(target.exists());

        std::fs::remove_file(&target).unwrap();
        std::fs::create_dir(&target).unwrap();
        assert_eq!(
            manager.reconcile(&OwnerIdentity::Unclaimed, &token),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert!(target.is_dir());
    }

    #[tokio::test]
    async fn wrong_length_target_is_rejected_without_truncation_or_deletion() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let target = directory.path().join(TARGET_NAME);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&target)
            .unwrap();
        file.set_len(10).unwrap();
        drop(file);

        assert_eq!(
            manager.reconcile(&OwnerIdentity::Unclaimed, &token),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert_eq!(std::fs::metadata(target).unwrap().len(), 10);
    }

    #[tokio::test]
    async fn inode_swap_immediately_before_unlink_preserves_replacement() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let directory_fd = manager.open_directory().unwrap();
        let expected = expected_command(&token).unwrap();
        let inspected = manager
            .inspect_entry(&directory_fd, TARGET_NAME, false, &expected)
            .unwrap()
            .unwrap();
        let target = directory.path().join(TARGET_NAME);
        let displaced = directory.path().join("displaced");
        std::fs::rename(&target, &displaced).unwrap();
        let replacement = openat(
            &directory_fd,
            TARGET_NAME,
            OFlags::WRONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        )
        .unwrap();
        write_all(&replacement, &expected).unwrap();
        fsync(&replacement).unwrap();
        drop(replacement);

        assert_eq!(
            ClaimFileManager::unlink_verified(&directory_fd, TARGET_NAME, &inspected),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert!(target.exists());
        assert!(displaced.exists());
    }

    #[tokio::test]
    async fn parent_path_swap_cannot_redirect_claim_file_operations() {
        let workspace = tempfile::tempdir().unwrap();
        let original_directory = workspace.path().join("data");
        let displaced_directory = workspace.path().join("displaced-data");
        std::fs::create_dir(&original_directory).unwrap();
        let descriptor =
            ServiceDatabaseDescriptor::parse("sqlite://data/service.db", workspace.path()).unwrap();
        let manager = ClaimFileManager::from_database_descriptor(&descriptor).unwrap();
        let pool = descriptor.connect().await.unwrap();
        pool.close().await;

        std::fs::rename(&original_directory, &displaced_directory).unwrap();
        std::fs::create_dir(&original_directory).unwrap();
        let token = SecretSlice::from(vec![0x11; 32]);
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();

        assert!(displaced_directory.join(TARGET_NAME).exists());
        assert!(!original_directory.join(TARGET_NAME).exists());
    }

    #[tokio::test]
    async fn unsafe_or_foreign_entries_are_left_untouched() {
        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let target = directory.path().join(TARGET_NAME);
        let original = read_target(&manager);

        let mut permissions = std::fs::metadata(&target).unwrap().permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&target, permissions).unwrap();
        assert_eq!(
            manager.reconcile(&OwnerIdentity::Unclaimed, &token),
            Err(ClaimFileError::UnsafeEntry)
        );
        let mut permissions = std::fs::metadata(&target).unwrap().permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(&target, permissions).unwrap();
        assert!(bool::from(
            read_target(&manager).as_slice().ct_eq(original.as_slice())
        ));

        let foreign = SecretSlice::from(vec![0x22; 32]);
        assert_eq!(
            manager.validate_existing_before_reconciliation(&foreign),
            Err(ClaimFileError::UnsafeEntry)
        );
        assert!(bool::from(
            read_target(&manager).as_slice().ct_eq(original.as_slice())
        ));
    }

    #[tokio::test]
    async fn symlink_target_is_rejected_without_following_or_deleting_it() {
        for name in [TARGET_NAME, TEMP_NAME] {
            let (directory, manager, token, _descriptor) = manager().await;
            let outside = directory.path().join("outside");
            std::fs::write(&outside, b"sentinel").unwrap();
            let entry = directory.path().join(name);
            symlink(&outside, &entry).unwrap();
            assert_eq!(
                manager.reconcile(&OwnerIdentity::Unclaimed, &token),
                Err(ClaimFileError::UnsafeEntry)
            );
            assert!(
                std::fs::symlink_metadata(&entry)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert_eq!(std::fs::read(outside).unwrap(), b"sentinel");
        }
    }

    #[tokio::test]
    async fn errors_reveal_neither_token_nor_absolute_path() {
        const SENTINEL_HEX: &str =
            "2222222222222222222222222222222222222222222222222222222222222222";

        let (directory, manager, token, _descriptor) = manager().await;
        manager
            .reconcile(&OwnerIdentity::Unclaimed, &token)
            .unwrap();
        let error = manager
            .validate_existing_before_reconciliation(&SecretSlice::from(vec![0x22; 32]))
            .unwrap_err();
        for rendered in [format!("{error}"), format!("{error:?}")] {
            assert!(!rendered.contains(SENTINEL_HEX));
            assert!(!rendered.contains(&directory.path().display().to_string()));
        }
    }
}
