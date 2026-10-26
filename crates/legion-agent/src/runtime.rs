//! The container runtime calls the agent makes: Podman in production,
//! [`crate::fake::FakeRuntime`] in tests.

use std::future::Future;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use legion_podman::{ContainerInspect, ContainerSpec, ContainerSummary, Event, Podman, Result};

pub trait Runtime: Send + Sync + 'static {
    fn image_exists(&self, reference: &str) -> impl Future<Output = Result<bool>> + Send;
    fn pull(&self, reference: &str) -> impl Future<Output = Result<()>> + Send;
    fn create(&self, spec: &ContainerSpec) -> impl Future<Output = Result<String>> + Send;
    fn start(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    fn stop(&self, id: &str, timeout: Duration) -> impl Future<Output = Result<()>> + Send;
    fn remove(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    fn inspect(&self, id: &str) -> impl Future<Output = Result<ContainerInspect>> + Send;
    fn wait(&self, id: &str) -> impl Future<Output = Result<i32>> + Send;
    fn list(
        &self,
        labels: &[(&str, &str)],
    ) -> impl Future<Output = Result<Vec<ContainerSummary>>> + Send;
    /// Container events from now on, for containers carrying every label.
    fn events(
        &self,
        labels: &[(&str, &str)],
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Event>>>> + Send;
}

impl Runtime for Podman {
    async fn image_exists(&self, reference: &str) -> Result<bool> {
        Podman::image_exists(self, reference).await
    }

    async fn pull(&self, reference: &str) -> Result<()> {
        Podman::pull(self, reference, None).await.map(drop)
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String> {
        Podman::create(self, spec).await
    }

    async fn start(&self, id: &str) -> Result<()> {
        Podman::start(self, id).await
    }

    async fn stop(&self, id: &str, timeout: Duration) -> Result<()> {
        Podman::stop(self, id, timeout).await
    }

    async fn remove(&self, id: &str) -> Result<()> {
        Podman::remove(self, id).await
    }

    async fn inspect(&self, id: &str) -> Result<ContainerInspect> {
        Podman::inspect(self, id).await
    }

    async fn wait(&self, id: &str) -> Result<i32> {
        Podman::wait(self, id).await
    }

    async fn list(&self, labels: &[(&str, &str)]) -> Result<Vec<ContainerSummary>> {
        Podman::list(self, labels).await
    }

    async fn events(&self, labels: &[(&str, &str)]) -> Result<BoxStream<'static, Result<Event>>> {
        Ok(Podman::events(self, labels).await?.boxed())
    }
}
