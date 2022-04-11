use std::collections::HashMap;

use crate::database::{
    Database,
    ServerInfo,
    RevsDiffRequest,
    DocsRequest,
    DocsRequestEntry,
    BulkDocsRequest
};

pub struct Replicator {
    source: Database,
    target: Database
}

impl Replicator {
    pub fn new(source: Database, target: Database) -> Self {
        Self {
            source,
            target
        }
    }

    pub async fn pull(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.replicate(&self.source, &self.target).await
    }

    async fn replicate(&self, source: &Database, target: &Database) -> Result<(), Box<dyn std::error::Error>> {
        let source_server_info = source.get_server_info().await?.expect("cannot fetch source server info");
        println!("source {:?}", source_server_info);

        let target_server_info = target.get_server_info().await?.expect("cannot fetch target server info");
        println!("target {:?}", target_server_info);

        let source_db_info = source.get_database_info().await?.expect("cannot fetch source db info");
        println!("source {:?}", source_db_info);

        let target_db_info = target.get_database_info().await?.expect("cannot fetch target db info");
        println!("target {:?}", target_db_info);

        let replication_id = self.replication_id(source_server_info, target_server_info);
        println!("replication id {}", replication_id);

        // TODO: use session id
        let source_replication_log = source.get_replication_log(&replication_id).await?;
        println!("source {:?}", source_replication_log);

        let target_replication_log = target.get_replication_log(&replication_id).await?;
        println!("target {:?}", target_replication_log);

        // TODO: Compare Replication Logs


        let changes = source.get_changes().await?.unwrap();
        println!("{:?}", changes);

        let mut revs: HashMap<String, Vec<String>> = HashMap::new();
        for row in changes.results.iter() {
            let id = row.id.clone();
            let r = row.changes.iter().map(|c| c.rev.clone() ).collect();
            revs.insert(id, r);
        }
        println!("{:?}", revs);


        let revs = RevsDiffRequest {
            body: revs
        };
        let revs_diff = target.get_revs_diff(&revs).await?.unwrap();
        println!("{:?}", revs_diff);

        let mut docs = vec![];
        for (id, entry) in revs_diff.body.iter() {
            for rev in entry.missing.iter() {
                let e = DocsRequestEntry {
                    id: id.to_string(),
                    rev: rev.to_string()
                };
                docs.push(e);
            };
        };
        let docs_request = DocsRequest {
            docs
        };
        println!("{:?}", docs_request);

        let docs_response = source.get_docs(&docs_request).await?.unwrap();
        println!("{:?}", docs_response);

        let mut docs = vec![];
        for entry in docs_response.results.iter() {
            for doc in entry.docs.iter() {
                let doc = doc.ok.clone();
                docs.push(doc);
            };
        };

        let bulk_docs_request = BulkDocsRequest {
            docs,
            new_edits: false
        };
        println!("{:?}", bulk_docs_request);
        target.save_docs(&bulk_docs_request).await?;

        println!("Done.");

        Ok(())
    }

    // TODO: do we need to use the database names here, too?
    fn replication_id(&self, source_server_info: ServerInfo, target_server_info: ServerInfo) -> String {
        let replication_id_data = vec![source_server_info.uuid, target_server_info.uuid];
        let replication_digest = md5::compute(replication_id_data.join(""));
        format!("{:x}", replication_digest)
    }
}
