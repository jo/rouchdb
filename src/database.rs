use url::Url;
use reqwest::StatusCode;
use std::collections::HashMap;

#[derive(serde::Deserialize, Debug)]
pub struct ServerInfo {
    pub uuid: String
}

#[derive(serde::Deserialize, Debug)]
pub struct DatabaseInfo {
    pub update_seq: String
}

#[derive(serde::Deserialize, Debug)]
pub struct ReplicationLog {
    pub _id: String,
    pub _rev: String,
    pub source_last_seq: String
}

#[derive(serde::Deserialize, Debug)]
pub struct Changes {
    pub last_seq: String,
    pub results: Vec<ChangesRow>
}

#[derive(serde::Deserialize, Debug)]
pub struct ChangesRow {
    pub seq: String,
    pub id: String,
    pub changes: Vec<Change>,
    #[serde(default)]
    pub deleted: bool
}

#[derive(serde::Deserialize, Debug)]
pub struct Change {
    pub rev: String
}

#[derive(serde::Serialize, Debug)]
pub struct RevsDiffRequest {
    pub body: HashMap<String, Vec<String>>
}

#[derive(serde::Deserialize, Debug)]
pub struct RevsDiff {
    pub body: HashMap<String, RevsDiffEntry>
}

#[derive(serde::Deserialize, Debug)]
pub struct RevsDiffEntry {
    pub missing: Vec<String>
}

#[derive(serde::Serialize, Debug)]
pub struct DocsRequest {
    pub docs: Vec<DocsRequestEntry>
}

#[derive(serde::Serialize, Debug)]
pub struct DocsRequestEntry {
    pub id: String,
    pub rev: String
}

#[derive(serde::Deserialize, Debug)]
pub struct DocsResponse {
    pub results: Vec<DocsResponseEntry>
}

#[derive(serde::Deserialize, Debug)]
pub struct DocsResponseEntry {
    pub id: String,
    pub docs: Vec<DocsResponseDoc>
}

#[derive(serde::Deserialize, Debug)]
pub struct DocsResponseDoc {
    pub ok: Doc
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Doc {
    _id: String,
    _rev: String,
    _revisions: Revisions,
    // TODO: make it generic
    foo: String
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Revisions {
    start: usize,
    ids: Vec<String>
}

#[derive(serde::Serialize, Debug)]
pub struct BulkDocsRequest {
    pub docs: Vec<Doc>,
    pub new_edits: bool
}

pub struct Database {
    url: Url
}

impl Database {
    pub fn new(url: Url) -> Self {
        Self {
            url
        }
    }

    pub async fn get_server_info(&self) -> Result<Option<ServerInfo>, Box<dyn std::error::Error>> {
        let mut url = self.url.join("/").unwrap();
        url.set_path("/");

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response
                    .json::<ServerInfo>()
                    .await?;
                Ok(Some(data))
            },
            // TODO: return error
            _ => {
                Ok(None)
            }
        }
    }

    pub async fn get_database_info(&self) -> Result<Option<DatabaseInfo>, Box<dyn std::error::Error>> {
        let url = self.url.clone();

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response
                    .json::<DatabaseInfo>()
                    .await?;
                Ok(Some(data))
            },
            // TODO: return error
            _ => {
                Ok(None)
            }
        }
    }

    pub async fn get_replication_log(&self, id: &str) -> Result<Option<ReplicationLog>, Box<dyn std::error::Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_local").push(id);

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response
                    .json::<ReplicationLog>()
                    .await?;
                Ok(Some(data))
            },
            StatusCode::NOT_FOUND => Ok(None),
            // TODO: return error here
            _ => Ok(None)
        }
    }

    pub async fn get_changes(&self) -> Result<Option<Changes>, Box<dyn std::error::Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_changes");
        url.query_pairs_mut().append_pair("feed", "normal");
        url.query_pairs_mut().append_pair("style", "all_docs");

        let response = reqwest::get(url).await?;
        match response.status() {
            StatusCode::OK => {
                let data = response
                    .json::<Changes>()
                    .await?;
                Ok(Some(data))
            },
            // TODO: return error here
            _ => Ok(None)
        }
    }

    pub async fn get_revs_diff(&self, revs: &RevsDiffRequest) -> Result<Option<RevsDiff>, Box<dyn std::error::Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_revs_diff");

        let client = reqwest::Client::new();
        let response = client.post(url)
            .json(&revs.body)
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => {
                let body = response
                    .json::<HashMap<String, RevsDiffEntry>>()
                    .await?;
                let diff = RevsDiff {
                    body
                };
                Ok(Some(diff))
            },
            // TODO: return error here
            _ => Ok(None)
        }
    }

    pub async fn get_docs(&self, docs: &DocsRequest) -> Result<Option<DocsResponse>, Box<dyn std::error::Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_get");
        url.query_pairs_mut().append_pair("revs", "true");

        let client = reqwest::Client::new();
        let response = client.post(url)
            .json(&docs)
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => {
                let body = response
                    .json::<DocsResponse>()
                    .await?;
                Ok(Some(body))
            },
            // TODO: return error here
            _ => Ok(None)
        }
    }

    pub async fn save_docs(&self, docs: &BulkDocsRequest) -> Result<(), Box<dyn std::error::Error>> {
        let mut url = self.url.clone();
        url.path_segments_mut().unwrap().push("_bulk_docs");

        let client = reqwest::Client::new();
        let response = client.post(url)
            .json(&docs)
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => {
                Ok(())
            },
            // TODO: return error here
            _ => Ok(())
        }
    }
}
