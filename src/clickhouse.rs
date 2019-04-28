use clickhouse_rs::Pool;
use clickhouse_rs::types::Options;
use failure::{Error, format_err};
use futures::{future, Future, Stream};
use log::*;
use std::time::{Duration, Instant};

use crate::backend::Backend;
use crate::dataframe::DataFrame;
use crate::query_ir::QueryIr;

mod df;
mod sql;

use self::df::block_to_df;
use self::sql::clickhouse_sql;

// Ping timeout in millis
const PING_TIMEOUT: u64 = 100_000;

#[derive(Clone)]
pub struct Clickhouse {
    pool: Pool,
}

impl Clickhouse {
    pub fn from_url(url: &str) -> Result<Self, Error> {
        let options = format!("tcp://{}", url).parse::<Options>()?;

        let options = options
            // Ping timeout is necessary, because under heavy load (100 requests
            // simultaneously, each one taking 5s ordinarily) the client will timeout.
            .ping_timeout(Duration::from_millis(PING_TIMEOUT));

        let pool = Pool::new(options);

        Ok(Clickhouse {
            pool,
        })
    }
}

impl Backend for Clickhouse {
    fn exec_sql(&self, sql: String) -> Box<Future<Item=DataFrame, Error=Error>> {
        let time_start = Instant::now();

        let fut = self.pool
            .get_handle()
            .and_then(move |c| c.query(&sql[..]).fetch_all())
            .from_err()
            .and_then(move |(_, block)| {
                let timing = time_start.elapsed();
                info!("Time for sql execution: {}.{:03}", timing.as_secs(), timing.subsec_millis());
                //debug!("Block: {:?}", block);

                Ok(block_to_df(block)?)
            });

        Box::new(fut)
    }

    fn generate_sql(&self, query_ir: QueryIr) -> String {
        clickhouse_sql(query_ir)
    }

    // https://users.rust-lang.org/t/solved-is-it-possible-to-clone-a-boxed-trait-object/1714/4
    fn box_clone(&self) -> Box<dyn Backend + Send + Sync> {
        Box::new((*self).clone())
    }
}
