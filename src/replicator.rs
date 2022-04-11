use serde::ser::StdError;
use std::collections::HashMap;
use tokio::join;
use uuid::Uuid;

use crate::database::{
    BulkDocsRequest, Database, DatabaseInfo, DocsRequest, DocsRequestEntry, ReplicationLog,
    RevsDiffRequest, ServerInfo,
};

pub struct Replicator {
    source: Database,
    target: Database,
}

impl Replicator {
    pub fn new(source: Database, target: Database) -> Self {
        Self { source, target }
    }

    pub async fn pull(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.replicate(&self.source, &self.target).await
    }

    async fn replicate(
        &self,
        source: &Database,
        target: &Database,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let infos = self.get_infos(source, target).await;

        let source_server_info = infos.0?.unwrap();
        let target_server_info = infos.1?.unwrap();
        let source_db_info = infos.2?.unwrap();
        let target_db_info = infos.3?.unwrap();

        println!("source {:?}", source_server_info);
        println!("target {:?}", target_server_info);
        println!("source {:?}", source_db_info);
        println!("target {:?}", target_db_info);

        let replication_id = self.replication_id(source_server_info, target_server_info);
        println!("replication id {}", replication_id);

        Ok(self
            .replicate_batch(source, target, &replication_id)
            .await?)
    }

    // TODO: do we need to use the database names here, too?
    fn replication_id(
        &self,
        source_server_info: ServerInfo,
        target_server_info: ServerInfo,
    ) -> String {
        let replication_id_data = vec![source_server_info.uuid, target_server_info.uuid];
        let replication_digest = md5::compute(replication_id_data.join(""));
        format!("{:x}", replication_digest)
    }

    async fn get_infos(
        &self,
        source: &Database,
        target: &Database,
    ) -> (
        Result<std::option::Option<ServerInfo>, Box<dyn StdError>>,
        Result<std::option::Option<ServerInfo>, Box<dyn StdError>>,
        Result<std::option::Option<DatabaseInfo>, Box<dyn StdError>>,
        Result<std::option::Option<DatabaseInfo>, Box<dyn StdError>>,
    ) {
        let source_server_info = source.get_server_info();
        let target_server_info = target.get_server_info();
        let source_db_info = source.get_database_info();
        let target_db_info = target.get_database_info();
        join!(
            source_server_info,
            target_server_info,
            source_db_info,
            target_db_info
        )
    }

    // TODO: actually batch things. This takes it all
    async fn replicate_batch(
        &self,
        source: &Database,
        target: &Database,
        replication_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let session_id = Uuid::new_v4();

        let logs = self
            .get_replication_logs(source, target, replication_id)
            .await;

        let mut source_replication_log = logs.0?;
        let mut target_replication_log = logs.1?;

        println!("source {:?}", source_replication_log);
        println!("target {:?}", target_replication_log);

        let since = self.common_ancestor(&source_replication_log, &target_replication_log);
        println!("replicating since {:?}", since);

        let changes = source.get_changes(&since).await?.unwrap();
        println!("{:?}", changes);

        let mut revs: HashMap<String, Vec<String>> = HashMap::new();
        for row in changes.results.iter() {
            let id = row.id.clone();
            let r = row.changes.iter().map(|c| c.rev.clone()).collect();
            revs.insert(id, r);
        }
        println!("{:?}", revs);

        let revs = RevsDiffRequest { body: revs };
        let revs_diff = target.get_revs_diff(&revs).await?.unwrap();
        println!("{:?}", revs_diff);

        let mut docs = vec![];
        for (id, entry) in revs_diff.body.iter() {
            for rev in entry.missing.iter() {
                let e = DocsRequestEntry {
                    id: id.to_string(),
                    rev: rev.to_string(),
                };
                docs.push(e);
            }
        }
        let docs_request = DocsRequest { docs };
        println!("{:?}", docs_request);

        let docs_response = source.get_docs(&docs_request).await?.unwrap();
        println!("{:?}", docs_response);

        let mut docs = vec![];
        for entry in docs_response.results.iter() {
            for doc in entry.docs.iter() {
                let doc = doc.ok.clone();
                docs.push(doc);
            }
        }

        // TODO: handle attachments
        let bulk_docs_request = BulkDocsRequest {
            docs,
            new_edits: false,
        };
        println!("{:?}", bulk_docs_request);
        target.save_docs(&bulk_docs_request).await?;

        source_replication_log.session_id = Some(session_id);
        source_replication_log.source_last_seq = Some(changes.last_seq.clone());
        target_replication_log.session_id = Some(session_id);
        target_replication_log.source_last_seq = Some(changes.last_seq.clone());
        
        println!("source {:?}", source_replication_log);
        println!("target {:?}", target_replication_log);

        let save_replication_logs = self.save_replication_logs(&source, &target, &source_replication_log, &target_replication_log).await;
        save_replication_logs.0?;
        save_replication_logs.1?;

        println!("Done.");

        Ok(())
    }

    async fn get_replication_logs(
        &self,
        source: &Database,
        target: &Database,
        replication_id: &str
    ) -> (
        Result<ReplicationLog, Box<dyn StdError>>,
        Result<ReplicationLog, Box<dyn StdError>>,
    ) {
        let source_replication_log = source.get_replication_log(replication_id);
        let target_replication_log = target.get_replication_log(replication_id);
        join!(
            source_replication_log,
            target_replication_log
        )
    }
    
    async fn save_replication_logs(
        &self,
        source: &Database,
        target: &Database,
        source_replication_log: &ReplicationLog,
        target_replication_log: &ReplicationLog
    ) -> (
        Result<(), Box<dyn StdError>>,
        Result<(), Box<dyn StdError>>,
    ) {
        let save_source_replication_log = source.save_replication_log(source_replication_log);
        let save_target_replication_log = target.save_replication_log(target_replication_log);
        join!(
            save_source_replication_log,
            save_target_replication_log
        )
    }
    
    fn common_ancestor(
        &self,
        source_replication_log: &ReplicationLog,
        target_replication_log: &ReplicationLog
    ) -> Option<String> {
        match source_replication_log.session_id == target_replication_log.session_id {
            true => {
                match &source_replication_log.source_last_seq {
                    Some(source_last_seq) => {
                        match &target_replication_log.source_last_seq {
                            Some(target_source_last_seq) => {
                                match source_last_seq == target_source_last_seq {
                                    true => Some(source_last_seq.to_string()),
                                    false => None
                                }
                            },
                            None => None
                        }
                    },
                    None => None
                }
            },
            false => None
        }
    }
}
