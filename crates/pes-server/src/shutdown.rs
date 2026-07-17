//! SIGTERM handling via `tokio::signal::unix::signal`.

/// Wait for a SIGTERM. Resolves once received; also resolves on Ctrl-C
/// (SIGINT) so local/dev runs (`cargo run`) shut down gracefully too, not
/// just containerized deployments (which receive SIGTERM from Docker/K8s).
pub async fn wait_for_sigterm() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        // No SIGTERM on non-Unix platforms; Ctrl-C is the only signal available.
        tokio::signal::ctrl_c().await
    }
}
