//! Cluster metadata shares the shard's WAL and promotion/recovery boundary.
use pg_cache_core::cluster::{Config, Save, Topology};
use pgrx::{bgworkers::BackgroundWorker, prelude::*};

pub fn load(config: Config) -> Topology {
    BackgroundWorker::transaction(|| {
        Spi::run("SET LOCAL synchronous_commit = on").expect("cluster synchronous commit");
        // Opt-in addition for existing 0.1 installations, in the private schema.
        Spi::run("CREATE TABLE IF NOT EXISTS cache_store.cluster_state (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), state text NOT NULL)").expect("cluster metadata table");
        Spi::run("REVOKE ALL ON cache_store.cluster_state FROM PUBLIC").unwrap();
        let stored = Spi::get_one::<String>(
            "SELECT (SELECT state FROM cache_store.cluster_state WHERE singleton)",
        )
        .unwrap();
        if let Some(stored) = stored {
            Topology::restore(config, &stored).expect("restore cluster metadata")
        } else {
            let topology = config.bootstrap().expect("cluster configuration");
            Spi::run_with_args(
                "INSERT INTO cache_store.cluster_state VALUES (true, $1)",
                &[topology.json().into()],
            )
            .expect("initialize cluster metadata");
            topology
        }
    })
}
pub fn save(request: Save) {
    BackgroundWorker::transaction(|| {
        Spi::run("SET LOCAL synchronous_commit = on").expect("cluster synchronous commit");
        Spi::run_with_args(
            "UPDATE cache_store.cluster_state SET state=$1 WHERE singleton",
            &[request.json.into()],
        )
        .expect("persist cluster metadata");
    });
    // Never acknowledge before transaction exit / WAL flush.
    let _ = request.done.send(());
}
