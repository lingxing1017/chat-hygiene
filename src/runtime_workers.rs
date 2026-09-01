use std::future::Future;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const WORKER_STOP_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, Error)]
#[error("runtime worker shutdown failed")]
pub(crate) struct WorkerShutdownError;

pub(crate) struct WorkerTask {
    stop: watch::Sender<bool>,
    join: Option<JoinHandle<()>>,
}

impl WorkerTask {
    pub(crate) fn new(stop: watch::Sender<bool>, join: JoinHandle<()>) -> Self {
        Self {
            stop,
            join: Some(join),
        }
    }

    fn signal_stop(&self) {
        self.stop.send_replace(true);
    }

    async fn wait(mut self) -> Result<(), WorkerShutdownError> {
        let mut join = self.join.take().ok_or(WorkerShutdownError)?;
        match tokio::time::timeout(WORKER_STOP_TIMEOUT, &mut join).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(WorkerShutdownError),
            Err(_) => {
                join.abort();
                let _ = join.await;
                Err(WorkerShutdownError)
            }
        }
    }

    pub(crate) async fn stop_and_wait(self) -> Result<(), WorkerShutdownError> {
        self.signal_stop();
        self.wait().await
    }

    fn abort(&mut self) {
        self.signal_stop();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

impl Drop for WorkerTask {
    fn drop(&mut self) {
        self.abort();
    }
}

pub(crate) struct WorkerGroup {
    workers: Vec<WorkerTask>,
}

impl WorkerGroup {
    pub(crate) fn new(workers: Vec<WorkerTask>) -> Self {
        Self { workers }
    }

    pub(crate) async fn stop_and_wait(mut self) -> Result<(), WorkerShutdownError> {
        let workers = std::mem::take(&mut self.workers);
        for worker in &workers {
            worker.signal_stop();
        }
        let mut failed = false;
        for worker in workers {
            failed |= worker.wait().await.is_err();
        }
        if failed {
            Err(WorkerShutdownError)
        } else {
            Ok(())
        }
    }
}

impl Drop for WorkerGroup {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            worker.abort();
        }
    }
}

#[derive(Debug)]
pub(crate) enum RuntimeExitError<E> {
    Server(E),
    WorkerShutdown,
    ServerAndWorker(E),
}

pub(crate) async fn run_server_with_workers<S, E>(
    server: S,
    workers: WorkerGroup,
) -> Result<(), RuntimeExitError<E>>
where
    S: Future<Output = Result<(), E>>,
{
    let server_result = server.await;
    let worker_result = workers.stop_and_wait().await;
    match (server_result, worker_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(RuntimeExitError::Server(error)),
        (Ok(()), Err(_)) => Err(RuntimeExitError::WorkerShutdown),
        (Err(error), Err(_)) => Err(RuntimeExitError::ServerAndWorker(error)),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{oneshot, watch};

    use super::*;

    fn cooperative_worker(
        completions: Arc<AtomicUsize>,
        mutations: Option<Arc<AtomicUsize>>,
    ) -> WorkerTask {
        let (stop, mut stopped) = watch::channel(false);
        let join = tokio::spawn(async move {
            loop {
                if *stopped.borrow() {
                    break;
                }
                tokio::select! {
                    result = stopped.changed() => {
                        if result.is_err() || *stopped.borrow() {
                            break;
                        }
                    }
                    () = tokio::time::sleep(Duration::from_millis(2)) => {
                        if let Some(mutations) = mutations.as_ref() {
                            mutations.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
            completions.fetch_add(1, Ordering::SeqCst);
        });
        WorkerTask::new(stop, join)
    }

    #[tokio::test]
    async fn server_success_stops_every_worker_and_mutation() {
        let completions = Arc::new(AtomicUsize::new(0));
        let mutations = Arc::new(AtomicUsize::new(0));
        let workers = WorkerGroup::new(vec![
            cooperative_worker(Arc::clone(&completions), Some(Arc::clone(&mutations))),
            cooperative_worker(Arc::clone(&completions), None),
            cooperative_worker(Arc::clone(&completions), None),
        ]);

        run_server_with_workers(
            async {
                tokio::time::sleep(Duration::from_millis(15)).await;
                Ok::<(), Infallible>(())
            },
            workers,
        )
        .await
        .unwrap();

        assert_eq!(completions.load(Ordering::SeqCst), 3);
        let stopped_at = mutations.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert_eq!(mutations.load(Ordering::SeqCst), stopped_at);
    }

    #[tokio::test]
    async fn graceful_completion_stops_and_awaits_every_worker_once() {
        let completions = Arc::new(AtomicUsize::new(0));
        let workers = WorkerGroup::new(
            (0..3)
                .map(|_| cooperative_worker(Arc::clone(&completions), None))
                .collect(),
        );
        let (shutdown, graceful) = oneshot::channel();
        shutdown.send(()).unwrap();

        run_server_with_workers(
            async move {
                graceful.await.unwrap();
                Ok::<(), Infallible>(())
            },
            workers,
        )
        .await
        .unwrap();

        assert_eq!(completions.load(Ordering::SeqCst), 3);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct TestServerError;

    #[tokio::test]
    async fn server_error_is_preserved_after_worker_cleanup() {
        let completions = Arc::new(AtomicUsize::new(0));
        let workers = WorkerGroup::new(
            (0..3)
                .map(|_| cooperative_worker(Arc::clone(&completions), None))
                .collect(),
        );

        let error = run_server_with_workers(async { Err(TestServerError) }, workers)
            .await
            .unwrap_err();

        assert!(matches!(error, RuntimeExitError::Server(TestServerError)));
        assert_eq!(completions.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn panic_and_uncooperative_worker_abort_without_stranding_peers() {
        let completions = Arc::new(AtomicUsize::new(0));
        let (panic_stop, _panic_stopped) = watch::channel(false);
        let panicking = WorkerTask::new(
            panic_stop,
            tokio::spawn(async { panic!("sentinel worker panic") }),
        );
        let (stuck_stop, _stuck_stopped) = watch::channel(false);
        let stuck = WorkerTask::new(stuck_stop, tokio::spawn(std::future::pending::<()>()));
        let workers = WorkerGroup::new(vec![
            panicking,
            cooperative_worker(Arc::clone(&completions), None),
            stuck,
        ]);

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            run_server_with_workers(async { Ok::<(), Infallible>(()) }, workers),
        )
        .await
        .expect("cleanup is bounded")
        .unwrap_err();

        assert!(matches!(error, RuntimeExitError::WorkerShutdown));
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        let rendered = format!("{error:?}");
        assert!(!rendered.contains("sentinel worker panic"));
    }
}
