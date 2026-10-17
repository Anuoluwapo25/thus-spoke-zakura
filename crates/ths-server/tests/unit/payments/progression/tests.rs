use super::*;
use crate::payments::confirmation::tests::lifecycle_claim;
use crate::rpc::RpcError;
use crate::state::tests::started_state_with_local_wallet as state_with_local_wallet;
use crate::{
    db::{PreparedTransaction, Recipient, Store},
    rpc::NodeRpc,
    wallet::RealWallet,
};
use axum::{Json, Router, routing::post};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

async fn journal_height_rpc(slow: Arc<AtomicBool>) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new().route(
            "/",
            post(move |Json(request): Json<Value>| {
                let slow = slow.clone();
                async move {
                    let result = match request["method"].as_str().unwrap() {
                        "getblockchaininfo" => {
                            if false {
                                tokio::time::sleep(PAYMENT_DISPATCH_BUDGET + Duration::from_secs(1))
                                    .await;
                            }
                            json!({"chain":"regtest","blocks":100,"bestblockhash":"ab".repeat(32),"verificationprogress":1.0})
                        }
                        "getrawtransaction" => {
                            let txid = request["params"][0].as_str().unwrap();
                            assert!(matches!(txid, "journal-txid" | "fast-external-txid"));
                        if txid == "journal-txid" && slow.load(Ordering::SeqCst) {
                            tokio::time::sleep(PAYMENT_DISPATCH_BUDGET + Duration::from_secs(1)).await;
                        }
                            if txid == "journal-txid" && slow.load(Ordering::SeqCst) {
                                return Json(json!({"result":null,"error":{"code":-5,"message":"not found"}}));
                            }
                            json!({"txid":txid,"confirmations":1,"blockhash":"confirmed-block"})
                        }
                        "getblock" => json!({"height":100}),
                        "getblockhash" => json!("confirmed-block"),
                    method => panic!("unexpected journal recovery RPC {method}"),
                    };
                    Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result,"error":null}))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (endpoint, server)
}

#[tokio::test]
async fn slow_journal_height_lookup_preserves_recovery_and_external_dispatch_across_passes() {
    let slow = Arc::new(AtomicBool::new(true));
    let (endpoint, server) = journal_height_rpc(slow.clone()).await;
    let (mut state, dir) = state_with_local_wallet();
    Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);
    let internal = lifecycle_claim(&state.0.store, "slow-journal");
    let wallet_db = rusqlite::Connection::open(dir.path().join("wallet.db")).unwrap();
    wallet_db
            .execute(
                "INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,?2,?3,?4)",
                rusqlite::params![internal.id, "journal-txid", b"original signed bytes", 140_u64],
            )
            .unwrap();

    let mut activity_cursor = None;
    let mut address_cursor = None;
    for key in ["fast-external-first-pass", "fast-external-second-pass"] {
        let external = state
            .0
            .store
            .claim_address_faucet("tm-external", 12_000, key)
            .unwrap();
        state
            .0
            .store
            .record_address_prepared(&external.id, "fast-external-txid", b"external bytes", 0)
            .unwrap();
        progress_payment_intents(
            &state,
            Instant::now() + PAYMENT_DISPATCH_BUDGET,
            &mut activity_cursor,
            &mut address_cursor,
        )
        .await
        .unwrap();

        let progressed = state.0.store.address_faucet_for_key(key).unwrap().unwrap();
        assert_eq!(progressed.status, "confirmed");
        assert_eq!(progressed.txid, "fast-external-txid");
        assert_eq!(progressed.block_hash.as_deref(), Some("confirmed-block"));
        let pending = state
            .0
            .store
            .activity_for_key("slow-journal")
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, "prepared");
        assert_eq!(pending.txid, "journal-txid");
        assert!(pending.last_error.is_some());
        assert_eq!(
            state
                .0
                .store
                .prepared_transaction_expiry(&internal.id)
                .unwrap(),
            Some(140)
        );
        let journal = state
            .0
            .wallet
            .prepared(&internal.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(journal.txid, "journal-txid");
        assert_eq!(journal.raw_transaction, b"original signed bytes");
        assert_eq!(journal.expiry_height, 140);
    }

    slow.store(false, Ordering::SeqCst);
    progress_payment_intents(
        &state,
        Instant::now() + PAYMENT_DISPATCH_BUDGET,
        &mut activity_cursor,
        &mut address_cursor,
    )
    .await
    .unwrap();
    let recovered = state
        .0
        .store
        .activity_for_key("slow-journal")
        .unwrap()
        .unwrap();
    assert_eq!(recovered.id, internal.id);
    assert_eq!(recovered.status, "confirmed");
    assert_eq!(recovered.txid, "journal-txid");
    let saved = state.0.store.prepared_transaction(&internal.id).unwrap();
    assert_eq!(saved.raw_transaction, b"original signed bytes");
    assert_eq!(saved.expiry_height, 140);

    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn slow_internal_lookup_does_not_starve_external_faucet_dispatch() {
    let external_lookups = Arc::new(AtomicUsize::new(0));
    let lookup_count = external_lookups.clone();
    let app = Router::new().route(
        "/",
        post(move |Json(request): Json<Value>| {
            let lookup_count = lookup_count.clone();
            async move {
                match request["method"].as_str().unwrap() {
                    "getblock" => return Json(json!({"result":{"height":100},"error":null})),
                    "getblockhash" => {
                        return Json(json!({"result":"external-block","error":null}));
                    }
                    "getrawtransaction" => {}
                    method => panic!("unexpected RPC {method}"),
                }
                let txid = request["params"][0].as_str().unwrap();
                if txid == "slow-internal-txid" {
                    tokio::time::sleep(PAYMENT_LOOKUP_TIMEOUT + Duration::from_secs(1)).await;
                    return Json(json!({
                        "jsonrpc":"2.0",
                        "id":request["id"],
                        "result":null,
                        "error":{"code":-5,"message":"transaction not found"}
                    }));
                }
                lookup_count.fetch_add(1, Ordering::SeqCst);
                Json(json!({
                    "jsonrpc":"2.0",
                    "id":request["id"],
                    "result":{"txid":txid,"confirmations":1,"blockhash":"external-block"},
                    "error":null
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut state, _dir) = state_with_local_wallet();
    Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);

    let internal = lifecycle_claim(&state.0.store, "slow-internal");
    state
        .0
        .store
        .record_prepared(&internal.id, "slow-internal-txid", b"internal bytes", 0)
        .unwrap();
    let external = state
        .0
        .store
        .claim_address_faucet("tm-external", 12_000, "fast-external")
        .unwrap();
    state
        .0
        .store
        .record_address_prepared(&external.id, "fast-external-txid", b"external bytes", 0)
        .unwrap();

    let mut activity_cursor = None;
    let mut address_cursor = None;
    progress_payment_intents(
        &state,
        Instant::now() + PAYMENT_DISPATCH_BUDGET,
        &mut activity_cursor,
        &mut address_cursor,
    )
    .await
    .unwrap();

    let progressed = state
        .0
        .store
        .address_faucet_for_key("fast-external")
        .unwrap()
        .unwrap();
    assert_eq!(progressed.status, "confirmed");
    assert!(external_lookups.load(Ordering::SeqCst) >= 1);
    let pending_internal = state
        .0
        .store
        .activity_for_key("slow-internal")
        .unwrap()
        .unwrap();
    assert_eq!(pending_internal.status, "prepared");
    assert!(pending_internal.last_error.is_some());

    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn insufficient_send_is_terminal_for_same_key_but_a_new_key_can_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.db");
    let store = Store::open(&path).unwrap();
    store.initialize().unwrap();
    let claim = lifecycle_claim(&store, "insufficient-send");
    let wallet = RealWallet::open(dir.path(), &store.seed().unwrap()).unwrap();
    let state = AppState::new(
        store.clone(),
        wallet,
        "http://127.0.0.1:1".into(),
        "test".into(),
    );
    state.mark_started();

    let failure = || {
        std::future::ready(Err(anyhow::Error::new(PaymentError::InsufficientFunds {
            available: 0,
            required: 12_000,
        })))
    };
    assert!(
        execute_payment(&state, claim.clone(), |_, _| failure())
            .await
            .is_err()
    );
    let failed = store
        .activity_for_key("insufficient-send")
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, "failed");
    assert!(
        failed
            .last_error
            .as_deref()
            .unwrap()
            .contains("insufficient")
    );

    drop(state);
    drop(store);
    let reopened = Store::open(&path).unwrap();
    reopened.initialize().unwrap();
    let wallet = RealWallet::open(dir.path(), &reopened.seed().unwrap()).unwrap();
    let state = AppState::new(
        reopened.clone(),
        wallet,
        "http://127.0.0.1:1".into(),
        "test".into(),
    );
    state.mark_started();
    let replay = lifecycle_claim(&reopened, "insufficient-send");
    assert_eq!(replay.id, failed.id);
    assert_eq!(replay.status, "failed");
    let prepare_calls = AtomicUsize::new(0);
    let replayed = execute_payment(&state, replay, |_, _| {
        prepare_calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(PreparedPayment {
            txid: "later-funded-txid".into(),
            raw_transaction: "later-funded-raw".into(),
            expiry_height: 100,
        }))
    })
    .await
    .unwrap();
    assert_eq!(replayed.status, "failed");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);

    let new_key = lifecycle_claim(&reopened, "insufficient-send-retry");
    assert_ne!(new_key.id, failed.id);
    assert_eq!(new_key.status, "preparing");
}

#[tokio::test]
async fn insufficient_send_policy_waits_for_wallet_journal_and_checks_server_transaction() {
    let (state, dir) = state_with_local_wallet();
    let claim = state
        .0
        .store
        .claim_transfer(
            1,
            Recipient::Account(2),
            "ironwood",
            "ironwood",
            12_000,
            "policy-store-transaction",
            None,
        )
        .unwrap();
    assert!(can_fail_insufficient_send(&state, &claim.id).await.unwrap());

    state
        .0
        .store
        .record_prepared(&claim.id, "server-txid", b"signed", 140)
        .unwrap();
    assert!(!can_fail_insufficient_send(&state, &claim.id).await.unwrap());

    let wallet_claim = state
        .0
        .store
        .claim_transfer(
            1,
            Recipient::Account(2),
            "ironwood",
            "ironwood",
            12_000,
            "policy-wallet-transaction",
            None,
        )
        .unwrap();
    rusqlite::Connection::open(dir.path().join("wallet.db"))
            .unwrap()
            .execute(
                "INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,?2,?3,?4)",
                rusqlite::params![wallet_claim.id, "wallet-txid", b"signed", 140_u64],
            )
            .unwrap();
    assert!(
        can_fail_insufficient_send(&state, &wallet_claim.id)
            .await
            .is_err()
    );
}

#[test]
fn progression_errors_are_recorded_against_post_transition_state() {
    let store = Store::open(":memory:").unwrap();
    store.initialize().unwrap();

    let internal = lifecycle_claim(&store, "transition-error-internal");
    let original_internal = store
        .unresolved_activities()
        .unwrap()
        .into_iter()
        .find(|intent| intent.key == "transition-error-internal")
        .unwrap();
    store
        .record_prepared(&internal.id, "current-internal-txid", b"signed", 140)
        .unwrap();
    record_current_activity_error(&store, &original_internal.key, "broadcast unavailable").unwrap();
    let current_internal = store
        .activity_for_key("transition-error-internal")
        .unwrap()
        .unwrap();
    assert_eq!(current_internal.status, "prepared");
    assert_eq!(current_internal.txid, "current-internal-txid");
    assert_eq!(
        current_internal.last_error.as_deref(),
        Some("broadcast unavailable")
    );

    let external = store
        .claim_address_faucet("tm-external", 12_000, "transition-error-external")
        .unwrap();
    let intent = store
        .unresolved_address_faucets()
        .unwrap()
        .into_iter()
        .find(|intent| intent.key == "transition-error-external")
        .unwrap();
    store
        .record_address_prepared(&external.id, "current-external-txid", b"signed", 140)
        .unwrap();
    record_current_address_error(&store, &intent.key, "broadcast unavailable").unwrap();
    let current_external = store
        .address_faucet_for_key("transition-error-external")
        .unwrap()
        .unwrap();
    assert_eq!(current_external.status, "prepared");
    assert_eq!(current_external.txid, "current-external-txid");
    assert_eq!(
        current_external.last_error.as_deref(),
        Some("broadcast unavailable")
    );
}

#[tokio::test]
async fn server_store_repairs_preparation_from_the_wallet_journal() {
    let (mut state, dir) = state_with_local_wallet();
    let claim = state
        .0
        .store
        .claim_transfer(
            1,
            crate::db::Recipient::Account(2),
            "ironwood",
            "ironwood",
            12_000,
            "journal-repair",
            None,
        )
        .unwrap();
    rusqlite::Connection::open(dir.path().join("wallet.db"))
            .unwrap()
            .execute(
                "INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,?2,?3,?4)",
                rusqlite::params![claim.id, "journal-txid", b"journaled signed bytes", 150_u64],
            )
            .unwrap();

    let rpc = Router::new().route(
        "/",
        post(|Json(request): Json<Value>| async move {
            let result = match request["method"].as_str().unwrap() {
                "getblockchaininfo" => json!({"chain":"regtest","blocks":100}),
                "getrawtransaction" => json!({
                    "txid":"journal-txid",
                    "confirmations":1,
                    "blockhash":"journal-block"
                }),
                "getblock" => json!({"height":100}),
                "getblockhash" => json!("journal-block"),
                method => panic!("unexpected RPC method {method}"),
            };
            Json(json!({
                "jsonrpc":"2.0",
                "id":request["id"],
                "result":result,
                "error":null
            }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, rpc).await.unwrap() });
    Arc::get_mut(&mut state.0).unwrap().rpc = NodeRpc::new(endpoint);

    let intent = state
        .0
        .store
        .unresolved_activities()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    progress_activity(&state, &intent, claim.clone())
        .await
        .unwrap();
    let repaired = state
        .0
        .store
        .activity_for_key("journal-repair")
        .unwrap()
        .unwrap();

    assert_eq!(
        (repaired.status.as_str(), repaired.txid.as_str()),
        ("confirmed", "journal-txid")
    );
    assert_eq!(repaired.block_hash.as_deref(), Some("journal-block"));
    assert_eq!(
        state.0.store.prepared_transaction(&claim.id).unwrap(),
        PreparedTransaction {
            raw_transaction: b"journaled signed bytes".to_vec(),
            expiry_height: 150
        }
    );
}

#[tokio::test]
async fn dispatch_deadline_bounds_waiting_for_payment_serialization() {
    let (state, _dir) = state_with_local_wallet();
    lifecycle_claim(&state.0.store, "locked-dispatch");
    let _guard = state.0.payments.lock().await;
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        progress_payment_intents(
            &state,
            Instant::now() + Duration::from_millis(500),
            &mut None,
            &mut None,
        ),
    )
    .await;
    assert!(
        result.is_ok(),
        "payment lock wait exceeded the dispatch budget"
    );
    assert_eq!(
        state
            .0
            .store
            .activity_for_key("locked-dispatch")
            .unwrap()
            .unwrap()
            .status,
        "preparing"
    );
}

#[tokio::test]
async fn payment_lookup_uses_remaining_dispatch_time() {
    let (mut state, _dir) = state_with_local_wallet();
    let pending = lifecycle_claim(&state.0.store, "short-dispatch");
    state
        .0
        .store
        .record_prepared(&pending.id, "pending-txid", b"signed bytes", 0)
        .unwrap();
    let app = Router::new().route("/", post(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Json(json!({"result":null,"error":{"code":-5,"message":"No such mempool or main chain transaction"}}))
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::get_mut(&mut state.0).unwrap().rpc =
        NodeRpc::new(format!("http://{}", listener.local_addr().unwrap()));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        progress_payment_intents(
            &state,
            Instant::now() + Duration::from_millis(500),
            &mut None,
            &mut None,
        ),
    )
    .await;
    assert!(result.is_ok(), "lookup extended the dispatch budget");
    let saved = state
        .0
        .store
        .activity_for_key("short-dispatch")
        .unwrap()
        .unwrap();
    assert_eq!(saved.status, "prepared");
    assert_eq!(saved.txid, "pending-txid");
    assert_eq!(
        state
            .0
            .store
            .prepared_transaction(&pending.id)
            .unwrap()
            .raw_transaction,
        b"signed bytes"
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn expired_wallet_journals_restore_both_server_rows_without_network() {
    let (state, dir) = state_with_local_wallet();
    let internal = lifecycle_claim(&state.0.store, "expired-internal-journal");
    let external = state
        .0
        .store
        .claim_address_faucet("tm-external", 12_000, "expired-external-journal")
        .unwrap();
    let wallet_db = rusqlite::Connection::open(dir.path().join("wallet.db")).unwrap();
    for id in [&internal.id, &external.id] {
        wallet_db.execute("INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,'original-txid',x'010203',140)", [id]).unwrap();
    }
    assert_eq!(
        repair_activity_journal(&state, &internal)
            .await
            .unwrap()
            .unwrap()
            .txid,
        "original-txid"
    );
    assert_eq!(
        repair_address_journal(&state, &external)
            .await
            .unwrap()
            .unwrap()
            .txid,
        "original-txid"
    );
    assert_eq!(
        state
            .0
            .store
            .activity_for_key("expired-internal-journal")
            .unwrap()
            .unwrap()
            .status,
        "prepared"
    );
    assert_eq!(
        state
            .0
            .store
            .address_faucet_for_key("expired-external-journal")
            .unwrap()
            .unwrap()
            .status,
        "prepared"
    );
}

#[tokio::test]
async fn insufficient_send_keeps_the_uncertain_original_lookup_error_pending() {
    let (mut state, dir) = state_with_local_wallet();
    let claim = lifecycle_claim(&state.0.store, "uncertain-insufficient-send");
    rusqlite::Connection::open(dir.path().join("wallet.db")).unwrap().execute("INSERT INTO ext_tsz_prepared_payments(activity_id,txid,raw_transaction,expiry_height) VALUES(?1,'expired-txid',x'010203',140)", [&claim.id]).unwrap();
    let prepared = state
        .0
        .store
        .record_prepared(&claim.id, "expired-txid", &[1, 2, 3], 140)
        .unwrap();
    let pending = state
        .0
        .store
        .reset_for_retry(&prepared.id, &prepared.txid)
        .unwrap();
    let lookups = Arc::new(AtomicUsize::new(0));
    let rpc_lookups = lookups.clone();
    let app = Router::new().route("/", post(move |Json(request): Json<Value>| {
            let lookups = rpc_lookups.clone();
            async move {
                match request["method"].as_str().unwrap() {
                    "getrawtransaction" if lookups.fetch_add(1, Ordering::SeqCst) < 3 => Json(json!({"result":null,"error":{"code":-5,"message":"original absent"}})),
                    "getrawtransaction" => Json(json!({"result":null,"error":{"code":-28,"message":"original lookup uncertain"}})),
                    "getblockchaininfo" => Json(json!({"result":{"chain":"regtest","blocks":140},"error":null})),
                    method => panic!("unexpected RPC {method}"),
                }
            }
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    Arc::get_mut(&mut state.0).unwrap().rpc =
        NodeRpc::new(format!("http://{}", listener.local_addr().unwrap()));
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let prepare_calls = AtomicUsize::new(0);
    let error = execute_payment(&state, pending, |_, _| {
        prepare_calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Err(anyhow::Error::new(PaymentError::InsufficientFunds {
            available: 0,
            required: 12_000,
        })))
    })
    .await
    .expect_err("uncertain original must remain pending");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(lookups.load(Ordering::SeqCst), 4);
    assert_eq!(
        error.downcast_ref::<RpcError>().map(|error| error.code),
        Some(-28),
        "{error:#}"
    );
    let saved = state
        .0
        .store
        .activity_for_key("uncertain-insufficient-send")
        .unwrap()
        .unwrap();
    assert_eq!(saved.status, "preparing");
    assert!(saved.txid.is_empty());
    assert!(
        saved
            .last_error
            .as_deref()
            .unwrap()
            .contains("original lookup uncertain")
    );
    let journal = state.0.wallet.prepared(&claim.id).await.unwrap().unwrap();
    assert_eq!(journal.txid, "expired-txid");
    assert_eq!(journal.raw_transaction, [1, 2, 3]);
    lookups.store(2, Ordering::SeqCst);
    state
        .0
        .store
        .record_error(&claim.id, "preparing", "", "earlier insufficient funds")
        .unwrap();
    let periodic_attempt = PaymentAttempt {
        state: &state,
        dispatch_deadline: Some(Instant::now() + PAYMENT_DISPATCH_BUDGET),
    };
    let periodic_error = can_fail_insufficient_send_with_attempt(&periodic_attempt, &claim.id)
        .await
        .unwrap_err();
    assert_eq!(
        periodic_error
            .downcast_ref::<RpcError>()
            .map(|error| error.code),
        Some(-28)
    );
    assert_eq!(lookups.load(Ordering::SeqCst), 4);
    record_current_activity_error(
        &state.0.store,
        "uncertain-insufficient-send",
        &format!("{periodic_error:#}"),
    )
    .unwrap();
    let pending_periodic = state
        .0
        .store
        .activity_for_key("uncertain-insufficient-send")
        .unwrap()
        .unwrap();
    assert_eq!(pending_periodic.status, "preparing");
    assert!(pending_periodic.txid.is_empty());
    assert!(
        pending_periodic
            .last_error
            .as_deref()
            .unwrap()
            .contains("original lookup uncertain")
    );
    assert_eq!(
        state
            .0
            .wallet
            .prepared(&claim.id)
            .await
            .unwrap()
            .unwrap()
            .txid,
        "expired-txid"
    );
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

const EARLIER_FAUCET_ID: &str = "ffffffff-ffff-4fff-bfff-ffffffffffff";
const DEPENDENT_SEND_ID: &str = "00000000-0000-4000-8000-000000000000";

/// Accepts a faucet into account 2 and then a send from account 2 that needs
/// it, with ids that sort in the opposite order to acceptance.
fn accept_faucet_then_dependent_send(state: &AppState, dir: &std::path::Path) {
    let faucet = state
        .0
        .store
        .claim_faucet(2, "ironwood", 300_000_000, "earlier-faucet")
        .unwrap();
    let send = state
        .0
        .store
        .claim_transfer(
            2,
            Recipient::Account(3),
            "ironwood",
            "ironwood",
            100_000_000,
            "dependent-send",
            None,
        )
        .unwrap();
    let db = rusqlite::Connection::open(dir.join("server.db")).unwrap();
    for (from, to) in [
        (faucet.id.as_str(), EARLIER_FAUCET_ID),
        (send.id.as_str(), DEPENDENT_SEND_ID),
    ] {
        db.execute("UPDATE activity SET id=?2 WHERE id=?1", [from, to])
            .unwrap();
        db.execute(
            "UPDATE idempotency SET activity_id=?2 WHERE activity_id=?1",
            [from, to],
        )
        .unwrap();
    }
}

/// Uses the real insufficient-funds policy, with submission and mining
/// settled directly in the store.
struct StoreSettledLifecycle<'a>(&'a AppState);

#[async_trait::async_trait]
impl crate::payments::execution::PaymentLifecycle for StoreSettledLifecycle<'_> {
    async fn submit_prepared(
        &self,
        activity: &Activity,
    ) -> anyhow::Result<crate::payments::submission::PreparedSubmission> {
        Ok(crate::payments::submission::PreparedSubmission::Broadcast(
            self.0
                .0
                .store
                .mark_broadcast(&activity.id, &activity.txid)?,
        ))
    }

    fn record_preparation_error(&self, id: &str, error: &str) -> anyhow::Result<()> {
        crate::payments::execution::PaymentLifecycle::record_preparation_error(self.0, id, error)
    }

    fn record_prepared(&self, id: &str, prepared: &PreparedPayment) -> anyhow::Result<Activity> {
        crate::payments::execution::PaymentLifecycle::record_prepared(self.0, id, prepared)
    }

    async fn confirm_after_mining(&self, activity: Activity) -> anyhow::Result<Activity> {
        self.0
            .0
            .store
            .confirm(&activity.id, &activity.txid, "dependent-block")
    }

    async fn fail_insufficient_send(
        &self,
        activity: &Activity,
        error: &str,
        replacement_txid: Option<&str>,
    ) -> anyhow::Result<bool> {
        crate::payments::execution::PaymentLifecycle::fail_insufficient_send(
            self.0,
            activity,
            error,
            replacement_txid,
        )
        .await
    }
}

#[tokio::test]
async fn background_dispatch_follows_acceptance_order_not_id_order() {
    let (state, dir) = state_with_local_wallet();
    accept_faucet_then_dependent_send(&state, dir.path());

    let queued = queued_activities(&state.0.store).unwrap();
    let ids: Vec<_> = queued.iter().map(|(_, activity)| activity.id()).collect();
    assert_eq!(ids, [EARLIER_FAUCET_ID, DEPENDENT_SEND_ID]);
    let (faucet_sequence, send_sequence) = (queued[0].0, queued[1].0);

    // Lightwalletd is unreachable, so each dispatch fails fast and stays pending.
    // The cursor is left on the activity dispatched last.
    let mut activity_cursor = None;
    progress_payment_intents(
        &state,
        Instant::now() + PAYMENT_DISPATCH_BUDGET,
        &mut activity_cursor,
        &mut None,
    )
    .await
    .unwrap();
    assert_eq!(activity_cursor, Some(send_sequence));

    // Round robin resumes after the cursor in acceptance order.
    let mut activity_cursor = Some(faucet_sequence);
    progress_payment_intents(
        &state,
        Instant::now() + PAYMENT_DISPATCH_BUDGET,
        &mut activity_cursor,
        &mut None,
    )
    .await
    .unwrap();
    assert_eq!(activity_cursor, Some(faucet_sequence));

    for key in ["earlier-faucet", "dependent-send"] {
        let pending = state.0.store.activity_for_key(key).unwrap().unwrap();
        assert_eq!(pending.status, "preparing");
        assert!(pending.last_error.is_some());
    }
}

#[tokio::test]
async fn dependent_send_waits_for_the_earlier_faucet_and_confirms() {
    let (state, dir) = state_with_local_wallet();
    accept_faucet_then_dependent_send(&state, dir.path());
    let lifecycle = StoreSettledLifecycle(&state);
    let store = &state.0.store;

    // The send reaches preparation before the faucet has funded account 2, as
    // it did whenever its id sorted first.
    let early = execute_payment(
        &lifecycle,
        store.activity(DEPENDENT_SEND_ID).unwrap(),
        |_, _| {
            std::future::ready(Err(anyhow::Error::new(PaymentError::InsufficientFunds {
                available: 0,
                required: 100_010_000,
            })))
        },
    )
    .await;
    assert!(early.is_err());
    let pending = store.activity_for_key("dependent-send").unwrap().unwrap();
    assert_eq!(pending.status, "preparing");
    assert!(
        pending
            .last_error
            .as_deref()
            .unwrap()
            .contains("insufficient")
    );

    let faucet = execute_payment(
        &lifecycle,
        store.activity(EARLIER_FAUCET_ID).unwrap(),
        |_, _| {
            std::future::ready(Ok(PreparedPayment {
                txid: "earlier-faucet-txid".into(),
                raw_transaction: "earlier-faucet-raw".into(),
                expiry_height: 140,
            }))
        },
    )
    .await
    .unwrap();
    assert_eq!(faucet.status, "confirmed");

    let send = execute_payment(&lifecycle, pending, |_, _| {
        std::future::ready(Ok(PreparedPayment {
            txid: "dependent-send-txid".into(),
            raw_transaction: "dependent-send-raw".into(),
            expiry_height: 140,
        }))
    })
    .await
    .unwrap();
    assert_eq!(send.id, DEPENDENT_SEND_ID);
    assert_eq!(send.status, "confirmed");
    assert_eq!(send.block_hash.as_deref(), Some("dependent-block"));
}

#[tokio::test]
async fn insufficient_send_waits_only_for_earlier_unresolved_credits_to_its_source_pool() {
    let (state, _dir) = state_with_local_wallet();
    let store = &state.0.store;
    store
        .claim_faucet(2, "transparent", 12_000, "other-pool-credit")
        .unwrap();
    store
        .claim_faucet(4, "ironwood", 12_000, "other-account-credit")
        .unwrap();
    let credit = store
        .claim_faucet(2, "ironwood", 12_000, "source-pool-credit")
        .unwrap();
    store
        .record_prepared(&credit.id, "source-pool-credit-txid", b"signed", 140)
        .unwrap();
    store
        .mark_broadcast(&credit.id, "source-pool-credit-txid")
        .unwrap();
    let send = store
        .claim_transfer(
            2,
            Recipient::Account(3),
            "ironwood",
            "ironwood",
            12_000,
            "credit-policy-send",
            None,
        )
        .unwrap();
    store
        .claim_faucet(2, "ironwood", 12_000, "later-credit")
        .unwrap();

    // A broadcast credit has not reached the wallet yet, so it still counts.
    assert!(!can_fail_insufficient_send(&state, &send.id).await.unwrap());

    store
        .confirm(&credit.id, "source-pool-credit-txid", "credit-block")
        .unwrap();
    assert!(can_fail_insufficient_send(&state, &send.id).await.unwrap());
}
