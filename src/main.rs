mod database;
mod replicator;

use crate::database::Database;
use crate::replicator::Replicator;

use clap::Parser;
use url::Url;

#[derive(Parser)]
#[clap(name = "rouchdb")]
#[clap(about = "RouchDB - a CouchDB written in Rust. Currently just a HTTP replicator.", long_about = None)]
pub struct Args {
    /// Source database url
    #[clap(index = 1, required = true, value_name = "SOURCE")]
    source: String,

    /// Target database url
    #[clap(index = 2, required = true, value_name = "TARGET")]
    target: String,
}

pub struct Rouch {
    args: Args,
}

impl Rouch {
    pub fn new(args: Args) -> Self {
        Self { args }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let source_url = Url::parse(&self.args.source).expect("invalid source url");
        let source = Database::new(source_url);

        let target_url = Url::parse(&self.args.target).expect("invalid target url");
        let target = Database::new(target_url);

        let replicator = Replicator::new(source, target);

        replicator.pull().await?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let client = Rouch::new(args);
    client.run().await
}
