//! Reading a document over HTTPS, as the app catalog is read (TODO.MD
//! item 5): with the process's rustls provider (ring), Legion's user
//! agent, bounded in time and in size.

/// The most a document may be: the catalog is about 140 KiB.
const MOST: usize = 8 << 20;

/// The body of `url`, an `https://` address, if it answers 200.
pub async fn get(url: &str) -> Result<Vec<u8>, String> {
    if !url.starts_with("https://") {
        return Err(format!("{url}: not an https:// address"));
    }
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    let http = reqwest::Client::builder()
        .user_agent(concat!("legion/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|e| format!("making the HTTP client: {e}"))?;
    let r = http.get(url).send().await.map_err(|e| format!("{url}: {e}"))?;
    if !r.status().is_success() {
        return Err(format!("{url}: {}", r.status()));
    }
    let body = r.bytes().await.map_err(|e| format!("{url}: {e}"))?;
    if body.len() > MOST {
        return Err(format!("{url}: more than {} MiB", MOST >> 20));
    }
    Ok(body.to_vec())
}
