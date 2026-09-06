#![cfg(feature = "system-native")]

use std::time::Duration;

use trevrpc_native::{NativeTransport, TransportConfig, TransportState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_create_diagnose_and_close() {
    let transport = NativeTransport::new(TransportConfig::default())
        .await
        .expect("create native transport");

    let diagnostics = transport.diagnostics().await.expect("read diagnostics");
    assert_eq!(diagnostics.state, TransportState::Running);

    tokio::time::timeout(Duration::from_secs(5), transport.close())
        .await
        .expect("native transport close timed out")
        .expect("close native transport");
}

#[test]
fn runtime_teardown_reports_owner_failure() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build first runtime");
    let transport = runtime
        .block_on(NativeTransport::new(TransportConfig::default()))
        .expect("create native transport");
    drop(runtime);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build second runtime");
    let error = runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), transport.close()).await })
        .expect("owner failure propagation timed out")
        .expect_err("runtime teardown must report owner failure");
    assert!(error.to_string().contains("emergency cleanup"));
}
