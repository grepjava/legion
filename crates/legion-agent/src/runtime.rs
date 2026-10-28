//! The container runtime calls the agent makes: Podman in production,
//! [`crate::fake::FakeRuntime`] in tests.

use std::future::Future;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use legion_podman::{
    Attached, Chunk, ContainerInspect, ContainerSpec, ContainerSummary, Event, ExecOutput, Podman,
    Result,
};

pub trait Runtime: Send + Sync + 'static {
    fn image_exists(&self, reference: &str) -> impl Future<Output = Result<bool>> + Send;
    fn pull(&self, reference: &str) -> impl Future<Output = Result<()>> + Send;
    fn create(&self, spec: &ContainerSpec) -> impl Future<Output = Result<String>> + Send;
    fn start(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    fn stop(&self, id: &str, timeout: Duration) -> impl Future<Output = Result<()>> + Send;
    fn remove(&self, id: &str) -> impl Future<Output = Result<()>> + Send;
    fn inspect(&self, id: &str) -> impl Future<Output = Result<ContainerInspect>> + Send;
    fn wait(&self, id: &str) -> impl Future<Output = Result<i32>> + Send;
    /// Runs a command in a running container and collects its output.
    fn exec(&self, id: &str, cmd: &[String]) -> impl Future<Output = Result<ExecOutput>> + Send;
    /// Starts a command in a running container with its output streamed
    /// and, with `stdin`, its input from the caller.
    fn exec_attach(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        tty: bool,
    ) -> impl Future<Output = Result<Attached>> + Send;
    fn exec_resize(
        &self,
        exec_id: &str,
        width: u16,
        height: u16,
    ) -> impl Future<Output = Result<()>> + Send;
    /// The exit code of an attached exec whose output has ended.
    fn exec_exit_code(&self, exec_id: &str) -> impl Future<Output = Result<i32>> + Send;
    /// A container's output; with `follow`, until it stops.
    fn logs(
        &self,
        id: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> impl Future<Output = Result<BoxStream<'static, Result<Chunk>>>> + Send;
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

    async fn exec(&self, id: &str, cmd: &[String]) -> Result<ExecOutput> {
        Podman::exec(self, id, cmd).await
    }

    async fn exec_attach(
        &self,
        id: &str,
        cmd: &[String],
        stdin: bool,
        tty: bool,
    ) -> Result<Attached> {
        Podman::exec_attach(self, id, cmd, stdin, tty).await
    }

    async fn exec_resize(&self, exec_id: &str, width: u16, height: u16) -> Result<()> {
        Podman::exec_resize(self, exec_id, width, height).await
    }

    async fn exec_exit_code(&self, exec_id: &str) -> Result<i32> {
        Podman::exec_exit_code(self, exec_id).await
    }

    async fn logs(
        &self,
        id: &str,
        follow: bool,
        tail: Option<usize>,
    ) -> Result<BoxStream<'static, Result<Chunk>>> {
        Ok(Podman::logs(self, id, follow, tail).await?.boxed())
    }

    async fn list(&self, labels: &[(&str, &str)]) -> Result<Vec<ContainerSummary>> {
        Podman::list(self, labels).await
    }

    async fn events(&self, labels: &[(&str, &str)]) -> Result<BoxStream<'static, Result<Event>>> {
        Ok(Podman::events(self, labels).await?.boxed())
    }
}
