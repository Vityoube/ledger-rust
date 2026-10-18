use std::sync::Arc;

use ledger::http;
use ledger::ledger::Ledger;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    println!("Ledger listening on http://localhost:{}", listener.local_addr()?.port());

    axum::serve(listener, http::app(Arc::new(Ledger::default())))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("Ledger stopped");
    Ok(())
}
