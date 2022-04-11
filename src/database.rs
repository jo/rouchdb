use reqwest::StatusCode;
use serde_json::value::Value;
use std::collections::HashMap;
use std::error::Error;
use url::Url;
use uuid::Uuid;

#[derive(serde::Deserialize, Debug)]
pub struct ServerInfo {
    pub uuid: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct DatabaseInfo {
    pub update_seq: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ReplicationLog {
    pub _id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _rev: Option<String>,
    pub source_last_seq: Option<String>,
    pub session_id: Option<Uuid>
}

#[derive(serde::Deserialize, Debug)]
pub struct Changes {
    pub last_seq: String,
    pub results: Vec<ChangesRow>,
}

#[derive(serde::Deserialize, Debug)]
pub struct ChangesRow {
    pub seq: String,
    pub id: String,
    pub changes: Vec<Change>,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(serde::Deserialize, Debug)]
pub struct Change {
    pub rev: String,
}

#[derive(serde::Serialize, Debug)]
pub struct RevsDiffRequest {
    pub body: HashMap<String, Vec<String>>,
}

#[derive(serde::Deserialize, Debug)]
pub struct RevsDiff {
    pub body: HashMap<String, RevsDiffEntry>,
}

#[derive(serde::Deserialize, Debug)]
pub struct RevsDiffEntry {
    pub missing: Vec<String>,
}

#[derive(serde::Serialize, Debug)]
pub struct DocsRequest {
    pub docs: Vec<DocsRequestEntry>,
}

#[derive(serde::Serialize, Debug)]
pub struct DocsRequestEntry {
    pub id: String,
    pub rev: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct DocsResponse {
    pub results: Vec<DocsResponseEntry>,
}

#[derive(serde::Deserialize, Debug)]
pub struct DocsResponseEntry {
    pub id: String,
    pub docs: Vec<DocsResponseDoc>,
}

#[derive(serde::Deserialize, Debug)]
pub struct DocsResponseDoc {
    pub ok: Value,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Revisions {
    start: usize,
    ids: Vec<String>,
}

#[derive(serde::Serialize, Debug)]
pub struct BulkDocsRequest {
    pub docs: Vec<Value>,
    pub new_edits: bool,
}

pub struct Database {
    url: Url,
}

impl Database {
    pub fn new(url: Url) -> Self {
        Self { url }
    }

    pub async fn get_server_info(&self) -> Result<Option<ServerInfo>, Box<dyn Error>> {
        let mut url = self.url.join("/").unwrap();
        url.set_path("/");

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<ServerInfo>().await?;
                Ok(Some(data))
            }
            _ => panic!("Problem reading server info"),
        }
    }

    pub async fn get_database_info(&self) -> Result<Option<DatabaseInfo>, Box<dyn Error>> {
        let url = self.url.clone();

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<DatabaseInfo>().await?;
                Ok(Some(data))
            }
            _ => panic!("Problem reading database info"),
        }
    }

    pub async fn get_replication_log(
        &self,
        replication_id: &str
    ) -> Result<ReplicationLog, Box<dyn Error>> {
        let id = format!("_local/{}", replication_id);
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(&id);

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<ReplicationLog>().await?;
                Ok(data)
            }
            StatusCode::NOT_FOUND => {
                Ok(ReplicationLog {
                    _id: id,
                    _rev: None,
                    source_last_seq: None,
                    session_id: None
                })
            },
            _ => panic!("Problem reading replication log"),
        }
    }

    pub async fn save_replication_log(
        &self,
        replication_log: &ReplicationLog,
    ) -> Result<(), Box<dyn Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push(&replication_log._id);

        let client = reqwest::Client::new();
        let response = client.put(url).json(&replication_log).send().await?;
        match response.status() {
            StatusCode::CREATED => Ok(()),
            _ => {
                let text = response.text().await?;
                panic!("Problem writing replication log: {}", text)
            },
        }
    }

    pub async fn get_changes(&self, since: &Option<String>) -> Result<Option<Changes>, Box<dyn Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_changes");
        url.query_pairs_mut().append_pair("feed", "normal");
        url.query_pairs_mut().append_pair("style", "all_docs");
        url.query_pairs_mut().append_pair("limit", "1000");
        match since {
            Some(since) => {
                url.query_pairs_mut().append_pair("since", since);
            },
            None => {}
        };

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response.json::<Changes>().await?;
                Ok(Some(data))
            }
            _ => panic!("Problem reading changes feed"),
        }
    }

    pub async fn get_revs_diff(
        &self,
        revs: &RevsDiffRequest,
    ) -> Result<Option<RevsDiff>, Box<dyn Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_revs_diff");

        let client = reqwest::Client::new();
        let response = client.post(url).json(&revs.body).send().await?;
        match response.status() {
            StatusCode::OK => {
                let body = response.json::<HashMap<String, RevsDiffEntry>>().await?;
                let diff = RevsDiff { body };
                Ok(Some(diff))
            }
            _ => panic!("Problem reading revs diff"),
        }
    }

    pub async fn get_docs(
        &self,
        docs: &DocsRequest,
    ) -> Result<Option<DocsResponse>, Box<dyn Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_get");
        url.query_pairs_mut().append_pair("revs", "true");
        url.query_pairs_mut().append_pair("attachments", "true");

        let client = reqwest::Client::new();
        let response = client.post(url).json(&docs).send().await?;
        match response.status() {
            StatusCode::OK => {
                let body = response.json::<DocsResponse>().await?;
                Ok(Some(body))
            }
            _ => panic!("Problem reading docs"),
        }
    }

    pub async fn save_docs(&self, docs: &BulkDocsRequest) -> Result<(), Box<dyn Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_docs");

        let client = reqwest::Client::new();
        let response = client.post(url).json(&docs).send().await?;
        match response.status() {
            StatusCode::CREATED => Ok(()),
            _ => {
                let text = response.text().await?;
                panic!("Problem writing docs: {}", text)
            },
        }
    }
}
