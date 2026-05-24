//! Postcard roundtrip and validation rules for supervision wire types.

use spawned_address::{ActorAddress, ActorId, NodeId};
use spawned_cluster::{
    decode_cluster_frame, decode_supervision, encode_supervision, encode_supervision_frame,
    requires_correlation, validate_envelope, validate_reply, ClusterFrame, RemoteSpawnSpec,
    RemoteSpecOverrides, SupervisionEnvelope, SupervisionEvent, SupervisionSignal,
    TransportError, WireExitReason, WireRestartType, MAX_REMOTE_SPAWN_INIT_BYTES,
};

fn sample_address(id: u64) -> ActorAddress {
    ActorAddress::local(ActorId::from_raw(id))
}

fn sample_node(name: &str) -> NodeId {
    NodeId::new(name)
}

fn roundtrip_frame(envelope: &SupervisionEnvelope) {
    let bytes = encode_supervision_frame(envelope).unwrap();
    let decoded = decode_cluster_frame(&bytes).unwrap();
    match decoded {
        ClusterFrame::Supervision(e) => assert_eq!(&e, envelope),
        other => panic!("expected Supervision frame, got {other:?}"),
    }
}

fn roundtrip_reply(envelope: &SupervisionEnvelope) {
    let bytes = encode_supervision(envelope).unwrap();
    let decoded = decode_supervision(&bytes).unwrap();
    assert_eq!(decoded, *envelope);
}

#[test]
fn supervision_event_variants_roundtrip() {
    let parent = sample_address(1);
    let child = sample_address(2);
    let owner = sample_address(3);
    let target = sample_address(4);
    let placement = sample_node("worker@127.0.0.1");

    let events = [
        SupervisionEvent::SpawnRequest {
            parent: parent.clone(),
            placement: placement.clone(),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "counter".into(),
                init: vec![1, 2, 3],
            },
            link: true,
        },
        SupervisionEvent::SpawnRequest {
            parent: parent.clone(),
            placement: placement.clone(),
            spec: RemoteSpawnSpec::NamedSpec {
                name: "my_worker".into(),
                overrides: RemoteSpecOverrides {
                    restart: Some(WireRestartType::Transient),
                    pg_scope: Some("default".into()),
                    pg_group: Some("workers".into()),
                },
            },
            link: false,
        },
        SupervisionEvent::SpawnOk {
            child: child.clone(),
        },
        SupervisionEvent::SpawnErr {
            error: "nope".into(),
        },
        SupervisionEvent::Signal {
            target: target.clone(),
            signal: SupervisionSignal::Stop,
        },
        SupervisionEvent::Signal {
            target: target.clone(),
            signal: SupervisionSignal::Shutdown,
        },
        SupervisionEvent::Signal {
            target: target.clone(),
            signal: SupervisionSignal::Kill,
        },
        SupervisionEvent::ChildExit {
            child: child.clone(),
            parent: parent.clone(),
            reason: WireExitReason::Normal,
        },
        SupervisionEvent::ChildExit {
            child: child.clone(),
            parent: parent.clone(),
            reason: WireExitReason::Shutdown,
        },
        SupervisionEvent::ChildExit {
            child: child.clone(),
            parent: parent.clone(),
            reason: WireExitReason::Panic("boom".into()),
        },
        SupervisionEvent::ChildExit {
            child: child.clone(),
            parent: parent.clone(),
            reason: WireExitReason::Kill,
        },
        SupervisionEvent::Down {
            owner: owner.clone(),
            monitor_ref: 42,
            child: child.clone(),
            reason: WireExitReason::Normal,
        },
        SupervisionEvent::Monitor {
            owner: owner.clone(),
            target: target.clone(),
            monitor_ref: 7,
        },
        SupervisionEvent::Demonitor {
            owner: owner.clone(),
            target: target.clone(),
            monitor_ref: 7,
        },
        SupervisionEvent::Link {
            a: parent.clone(),
            b: child.clone(),
        },
        SupervisionEvent::Unlink {
            a: parent.clone(),
            b: child.clone(),
        },
    ];

    for event in events {
        let cid = if requires_correlation(&event) { 1 } else { 0 };
        let envelope = SupervisionEnvelope {
            correlation_id: cid,
            event,
        };
        roundtrip_frame(&envelope);
        if cid != 0 {
            roundtrip_reply(&envelope);
        }
    }
}

#[test]
fn correlation_id_validation() {
    let spawn = SupervisionEnvelope {
        correlation_id: 0,
        event: SupervisionEvent::SpawnRequest {
            parent: sample_address(1),
            placement: sample_node("n@h"),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "w".into(),
                init: vec![],
            },
            link: false,
        },
    };
    assert!(matches!(
        validate_envelope(&spawn),
        Err(TransportError::Protocol(_))
    ));

    let signal = SupervisionEnvelope {
        correlation_id: 1,
        event: SupervisionEvent::Signal {
            target: sample_address(4),
            signal: SupervisionSignal::Kill,
        },
    };
    assert!(matches!(
        validate_envelope(&signal),
        Err(TransportError::Protocol(_))
    ));

    let ok = SupervisionEnvelope {
        correlation_id: 1,
        event: SupervisionEvent::SpawnRequest {
            parent: sample_address(1),
            placement: sample_node("n@h"),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "w".into(),
                init: vec![],
            },
            link: false,
        },
    };
    validate_envelope(&ok).unwrap();
}

#[test]
fn spawn_init_size_limit() {
    let too_big = SupervisionEnvelope {
        correlation_id: 1,
        event: SupervisionEvent::SpawnRequest {
            parent: sample_address(1),
            placement: sample_node("n@h"),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "w".into(),
                init: vec![0; MAX_REMOTE_SPAWN_INIT_BYTES + 1],
            },
            link: false,
        },
    };
    assert!(matches!(
        validate_envelope(&too_big),
        Err(TransportError::Protocol(_))
    ));
}

#[test]
fn reply_kind_validation() {
    let request = SupervisionEnvelope {
        correlation_id: 99,
        event: SupervisionEvent::SpawnRequest {
            parent: sample_address(1),
            placement: sample_node("n@h"),
            spec: RemoteSpawnSpec::Worker {
                worker_type: "w".into(),
                init: vec![],
            },
            link: false,
        },
    };
    let bad_reply = SupervisionEnvelope {
        correlation_id: 99,
        event: SupervisionEvent::SpawnOk {
            child: sample_address(2),
        },
    };
    validate_reply(&request, &bad_reply).unwrap();

    let wrong_kind = SupervisionEnvelope {
        correlation_id: 99,
        event: SupervisionEvent::Signal {
            target: sample_address(4),
            signal: SupervisionSignal::Stop,
        },
    };
    assert!(matches!(
        validate_reply(&request, &wrong_kind),
        Err(TransportError::Protocol(_))
    ));
}
