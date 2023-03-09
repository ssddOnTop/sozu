use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    net::SocketAddr,
};

use crate::{
    state::ConfigState,
    worker::{
        ActivateListener, AddCertificate, AggregatedMetricsData, Backend, Cluster,
        DeactivateListener, HttpFrontend, HttpListenerConfig, HttpsListenerConfig,
        MetricsConfiguration, ProxyEvent, QueryAnswer, QueryCertificateType, QueryClusterType,
        QueryMetricsOptions, RemoveBackend, RemoveCertificate, RemoveListener, ReplaceCertificate,
        TcpFrontend, TcpListenerConfig,
    },
};

pub const PROTOCOL_VERSION: u8 = 0;

/// A request sent by the CLI (or other) to the main process
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "content", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Order {
    /// save Sōzu's parseable state as a file
    SaveState {
        path: String,
    },
    /// load a state file
    LoadState {
        path: String,
    },
    /// dump the state in JSON
    DumpState,
    /// list the workers and their status
    ListWorkers,
    /// list the frontends, filtered by protocol and/or domain
    ListFrontends(FrontendFilters),
    // list all listeners
    ListListeners,
    /// launche a new worker
    LaunchWorker(String),
    /// upgrade the main process
    UpgradeMain,
    /// upgrade an existing worker
    UpgradeWorker(u32),
    /// subscribe to proxy events
    SubscribeEvents,
    /// reload the configuration from the config file, or a new file
    ReloadConfiguration {
        path: Option<String>,
    },
    /// give status of main process and all workers
    Status,

    AddCluster(Cluster),
    RemoveCluster {
        cluster_id: String,
    },

    AddHttpFrontend(HttpFrontend),
    RemoveHttpFrontend(HttpFrontend),

    AddHttpsFrontend(HttpFrontend),
    RemoveHttpsFrontend(HttpFrontend),

    AddCertificate(AddCertificate),
    ReplaceCertificate(ReplaceCertificate),
    RemoveCertificate(RemoveCertificate),

    AddTcpFrontend(TcpFrontend),
    RemoveTcpFrontend(TcpFrontend),

    AddBackend(Backend),
    RemoveBackend(RemoveBackend),

    AddHttpListener(HttpListenerConfig),
    AddHttpsListener(HttpsListenerConfig),
    AddTcpListener(TcpListenerConfig),

    RemoveListener(RemoveListener),

    ActivateListener(ActivateListener),
    DeactivateListener(DeactivateListener),

    QueryCertificates(QueryCertificateType),
    QueryClusters(QueryClusterType),
    QueryClustersHashes,
    QueryMetrics(QueryMetricsOptions),

    SoftStop,
    HardStop,

    ConfigureMetrics(MetricsConfiguration),
    Logging(String),

    ReturnListenSockets,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FrontendFilters {
    pub http: bool,
    pub https: bool,
    pub tcp: bool,
    pub domain: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandStatus {
    Ok,
    Processing,
    Error,
}

/// details of a response sent by the main process to the client
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandResponseContent {
    /// a list of workers, with ids, pids, statuses
    Workers(Vec<WorkerInfo>),
    /// aggregated metrics of main process and workers
    Metrics(AggregatedMetricsData),
    /// worker responses to a same query: worker_id -> query_answer
    Query(BTreeMap<String, QueryAnswer>),
    /// the state of Sōzu: frontends, backends, listeners, etc.
    State(Box<ConfigState>),
    /// a proxy event
    Event(Event),
    /// a filtered list of frontend
    FrontendList(ListedFrontends),
    // this is new
    Status(Vec<WorkerInfo>),
    /// all listeners
    ListenersList(ListenersList),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListedFrontends {
    pub http_frontends: Vec<HttpFrontend>,
    pub https_frontends: Vec<HttpFrontend>,
    pub tcp_frontends: Vec<TcpFrontend>,
}

/// All listeners, listed for the CLI.
/// the bool indicates if it is active or not
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListenersList {
    pub http_listeners: HashMap<SocketAddr, (HttpListenerConfig, bool)>,
    pub https_listeners: HashMap<SocketAddr, (HttpsListenerConfig, bool)>,
    pub tcp_listeners: HashMap<SocketAddr, (TcpListenerConfig, bool)>,
}

/// Responses of the main process to the CLI (or other client)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandResponse {
    pub id: String,
    pub version: u8,
    pub status: CommandStatus,
    pub message: String,
    pub content: Option<CommandResponseContent>,
}

impl CommandResponse {
    pub fn new(
        // id: String,
        status: CommandStatus,
        message: String,
        content: Option<CommandResponseContent>,
    ) -> CommandResponse {
        CommandResponse {
            version: PROTOCOL_VERSION,
            id: "generic-response-id-to-be-removed".to_string(),
            status,
            message,
            content,
        }
    }
}

/// Runstate of a worker
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunState {
    Running,
    Stopping,
    Stopped,
    NotAnswering,
}

impl fmt::Display for RunState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: u32,
    pub pid: i32,
    pub run_state: RunState,
}

/// a backend event that happened on a proxy
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Event {
    BackendDown(String, SocketAddr),
    BackendUp(String, SocketAddr),
    NoAvailableBackends(String),
    /// indicates a backend that was removed from configuration has no lingering connections
    /// so it can be safely stopped
    RemovedBackendHasNoConnections(String, SocketAddr),
}

impl From<ProxyEvent> for Event {
    fn from(e: ProxyEvent) -> Self {
        match e {
            ProxyEvent::BackendDown(id, addr) => Event::BackendDown(id, addr),
            ProxyEvent::BackendUp(id, addr) => Event::BackendUp(id, addr),
            ProxyEvent::NoAvailableBackends(cluster_id) => Event::NoAvailableBackends(cluster_id),
            ProxyEvent::RemovedBackendHasNoConnections(id, addr) => {
                Event::RemovedBackendHasNoConnections(id, addr)
            }
        }
    }
}

#[derive(Serialize)]
struct StatePath {
    path: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::certificate::split_certificate_chain;
    use crate::config::ProxyProtocolConfig;
    use crate::worker::{
        AddCertificate, Backend, CertificateAndKey, CertificateFingerprint, Cluster,
        ClusterMetricsData, FilteredData, HttpFrontend, LoadBalancingAlgorithms,
        LoadBalancingParams, PathRule, Percentiles, QueryAnswerMetrics, RemoveBackend,
        RemoveCertificate, Route, RulePosition, TlsVersion, WorkerMetrics,
    };
    use hex::FromHex;
    use serde_json;

    #[test]
    fn config_message_test() {
        let raw_json = r#"{ "id": "ID_TEST", "version": 0, "type": "ADD_HTTP_FRONTEND", "content":{"route": {"CLUSTER_ID": "xxx"}, "hostname": "yyy", "path": {"PREFIX": "xxx"}, "address": "0.0.0.0:8080"}}"#;
        let message: Order = serde_json::from_str(raw_json).unwrap();
        println!("{message:?}");
        assert_eq!(
            message,
            Order::AddHttpFrontend(HttpFrontend {
                route: Route::ClusterId(String::from("xxx")),
                hostname: String::from("yyy"),
                path: PathRule::Prefix(String::from("xxx")),
                method: None,
                address: "0.0.0.0:8080".parse().unwrap(),
                position: RulePosition::Tree,
                tags: None,
            })
        );
    }

    macro_rules! test_message (
    ($name: ident, $filename: expr, $expected_message: expr) => (

      #[test]
      fn $name() {
        let data = include_str!($filename);
        let pretty_print = serde_json::to_string_pretty(&$expected_message).expect("should have serialized");
        assert_eq!(&pretty_print, data, "\nserialized message:\n{}\n\nexpected message:\n{}", pretty_print, data);

        let message: Order = serde_json::from_str(data).unwrap();
        assert_eq!(message, $expected_message, "\ndeserialized message:\n{:#?}\n\nexpected message:\n{:#?}", message, $expected_message);

      }

    )
  );

    macro_rules! test_message_answer (
    ($name: ident, $filename: expr, $expected_message: expr) => (

      #[test]
      fn $name() {
        let data = include_str!($filename);
        let pretty_print = serde_json::to_string_pretty(&$expected_message).expect("should have serialized");
        assert_eq!(&pretty_print, data, "\nserialized message:\n{}\n\nexpected message:\n{}", pretty_print, data);

        let message: CommandResponse = serde_json::from_str(data).unwrap();
        assert_eq!(message, $expected_message, "\ndeserialized message:\n{:#?}\n\nexpected message:\n{:#?}", message, $expected_message);

      }

    )
  );

    test_message!(
        add_cluster,
        "../assets/add_cluster.json",
        Order::AddCluster(Cluster {
            cluster_id: String::from("xxx"),
            sticky_session: true,
            https_redirect: true,
            proxy_protocol: Some(ProxyProtocolConfig::ExpectHeader),
            load_balancing: LoadBalancingAlgorithms::RoundRobin,
            load_metric: None,
            answer_503: None,
        })
    );

    test_message!(
        remove_cluster,
        "../assets/remove_cluster.json",
        Order::RemoveCluster {
            cluster_id: String::from("xxx")
        }
    );

    test_message!(
        add_http_front,
        "../assets/add_http_front.json",
        Order::AddHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("xxx")),
            hostname: String::from("yyy"),
            path: PathRule::Prefix(String::from("xxx")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        })
    );

    test_message!(
        remove_http_front,
        "../assets/remove_http_front.json",
        Order::RemoveHttpFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("xxx")),
            hostname: String::from("yyy"),
            path: PathRule::Prefix(String::from("xxx")),
            method: None,
            address: "0.0.0.0:8080".parse().unwrap(),
            position: RulePosition::Tree,
            tags: Some(BTreeMap::from([
                ("owner".to_owned(), "John".to_owned()),
                (
                    "uuid".to_owned(),
                    "0dd8d7b1-a50a-461a-b1f9-5211a5f45a83".to_owned()
                )
            ]))
        })
    );

    test_message!(
        add_https_front,
        "../assets/add_https_front.json",
        Order::AddHttpsFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("xxx")),
            hostname: String::from("yyy"),
            path: PathRule::Prefix(String::from("xxx")),
            method: None,
            address: "0.0.0.0:8443".parse().unwrap(),
            position: RulePosition::Tree,
            tags: None,
        })
    );

    test_message!(
        remove_https_front,
        "../assets/remove_https_front.json",
        Order::RemoveHttpsFrontend(HttpFrontend {
            route: Route::ClusterId(String::from("xxx")),
            hostname: String::from("yyy"),
            path: PathRule::Prefix(String::from("xxx")),
            method: None,
            address: "0.0.0.0:8443".parse().unwrap(),
            position: RulePosition::Tree,
            tags: Some(BTreeMap::from([
                ("owner".to_owned(), "John".to_owned()),
                (
                    "uuid".to_owned(),
                    "0dd8d7b1-a50a-461a-b1f9-5211a5f45a83".to_owned()
                )
            ]))
        })
    );

    const KEY: &str = include_str!("../../lib/assets/key.pem");
    const CERTIFICATE: &str = include_str!("../../lib/assets/certificate.pem");
    const CHAIN: &str = include_str!("../../lib/assets/certificate_chain.pem");

    test_message!(
        add_certificate,
        "../assets/add_certificate.json",
        Order::AddCertificate(AddCertificate {
            address: "0.0.0.0:443".parse().unwrap(),
            certificate: CertificateAndKey {
                certificate: String::from(CERTIFICATE),
                certificate_chain: split_certificate_chain(String::from(CHAIN)),
                key: String::from(KEY),
                versions: vec![TlsVersion::TLSv1_2, TlsVersion::TLSv1_3],
            },
            names: vec![],
            expired_at: None,
        })
    );

    test_message!(
        remove_certificate,
        "../assets/remove_certificate.json",
        Order::RemoveCertificate(RemoveCertificate {
            address: "0.0.0.0:443".parse().unwrap(),
            fingerprint: CertificateFingerprint(
                FromHex::from_hex(
                    "ab2618b674e15243fd02a5618c66509e4840ba60e7d64cebec84cdbfeceee0c5"
                )
                .unwrap()
            ),
        })
    );

    test_message!(
        add_backend,
        "../assets/add_backend.json",
        Order::AddBackend(Backend {
            cluster_id: String::from("xxx"),
            backend_id: String::from("xxx-0"),
            address: "127.0.0.1:8080".parse().unwrap(),
            load_balancing_parameters: Some(LoadBalancingParams { weight: 0 }),
            sticky_id: Some(String::from("xxx-0")),
            backup: Some(false),
        })
    );

    test_message!(
        remove_backend,
        "../assets/remove_backend.json",
        Order::RemoveBackend(RemoveBackend {
            cluster_id: String::from("xxx"),
            backend_id: String::from("xxx-0"),
            address: "127.0.0.1:8080".parse().unwrap(),
        })
    );

    test_message!(soft_stop, "../assets/soft_stop.json", Order::SoftStop);

    test_message!(hard_stop, "../assets/hard_stop.json", Order::HardStop);

    test_message!(status, "../assets/status.json", Order::Status);

    test_message!(
        load_state,
        "../assets/load_state.json",
        Order::LoadState {
            path: String::from("./config_dump.json")
        }
    );

    test_message!(
        save_state,
        "../assets/save_state.json",
        Order::SaveState {
            path: String::from("./config_dump.json")
        }
    );

    test_message!(dump_state, "../assets/dump_state.json", Order::DumpState);

    test_message!(
        list_workers,
        "../assets/list_workers.json",
        Order::ListWorkers
    );

    test_message!(
        upgrade_main,
        "../assets/upgrade_main.json",
        Order::UpgradeMain
    );

    test_message!(
        upgrade_worker,
        "../assets/upgrade_worker.json",
        Order::UpgradeWorker(0)
    );

    test_message_answer!(
        answer_workers_status,
        "../assets/answer_workers_status.json",
        CommandResponse {
            id: "ID_TEST".to_string(),
            version: 0,
            status: CommandStatus::Ok,
            message: String::from(""),
            content: Some(CommandResponseContent::Workers(vec!(
                WorkerInfo {
                    id: 1,
                    pid: 5678,
                    run_state: RunState::Running,
                },
                WorkerInfo {
                    id: 0,
                    pid: 1234,
                    run_state: RunState::Stopping,
                },
            ))),
        }
    );

    test_message_answer!(
        answer_metrics,
        "../assets/answer_metrics.json",
        CommandResponse {
            id: "ID_TEST".to_string(),
            version: 0,
            status: CommandStatus::Ok,
            message: String::from(""),
            content: Some(CommandResponseContent::Metrics(AggregatedMetricsData {
                main: [
                    (String::from("sozu.gauge"), FilteredData::Gauge(1)),
                    (String::from("sozu.count"), FilteredData::Count(-2)),
                    (String::from("sozu.time"), FilteredData::Time(1234)),
                ]
                .iter()
                .cloned()
                .collect(),
                workers: [(
                    String::from("0"),
                    QueryAnswer::Metrics(QueryAnswerMetrics::All(WorkerMetrics {
                        proxy: Some(
                            [
                                (String::from("sozu.gauge"), FilteredData::Gauge(1)),
                                (String::from("sozu.count"), FilteredData::Count(-2)),
                                (String::from("sozu.time"), FilteredData::Time(1234)),
                            ]
                            .iter()
                            .cloned()
                            .collect()
                        ),
                        clusters: Some(
                            [(
                                String::from("cluster_1"),
                                ClusterMetricsData {
                                    cluster: Some(
                                        [(
                                            String::from("request_time"),
                                            FilteredData::Percentiles(Percentiles {
                                                samples: 42,
                                                p_50: 1,
                                                p_90: 2,
                                                p_99: 10,
                                                p_99_9: 12,
                                                p_99_99: 20,
                                                p_99_999: 22,
                                                p_100: 30,
                                            })
                                        )]
                                        .iter()
                                        .cloned()
                                        .collect()
                                    ),
                                    backends: Some(
                                        [(
                                            String::from("cluster_1-0"),
                                            [
                                                (
                                                    String::from("bytes_in"),
                                                    FilteredData::Count(256)
                                                ),
                                                (
                                                    String::from("bytes_out"),
                                                    FilteredData::Count(128)
                                                ),
                                                (
                                                    String::from("percentiles"),
                                                    FilteredData::Percentiles(Percentiles {
                                                        samples: 42,
                                                        p_50: 1,
                                                        p_90: 2,
                                                        p_99: 10,
                                                        p_99_9: 12,
                                                        p_99_99: 20,
                                                        p_99_999: 22,
                                                        p_100: 30,
                                                    })
                                                )
                                            ]
                                            .iter()
                                            .cloned()
                                            .collect()
                                        )]
                                        .iter()
                                        .cloned()
                                        .collect()
                                    ),
                                }
                            )]
                            .iter()
                            .cloned()
                            .collect()
                        )
                    }))
                )]
                .iter()
                .cloned()
                .collect()
            }))
        }
    );
}
