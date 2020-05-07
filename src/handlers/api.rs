use std::collections::HashMap;

use actix_web::{
    AsyncResponder,
    FutureResponse,
    HttpRequest,
    HttpResponse,
    Path,
};
use failure::{Error, format_err};
use futures::future::{self, *};
use lazy_static::lazy_static;
use log::*;
use serde_qs as qs;
use std::convert::TryInto;

use crate::app::AppState;
use crate::dataframe::{DataFrame, Column, ColumnData};
use crate::error::ServerError;
use crate::format::{FormatType, format_records};
use crate::query::Query;
use crate::schema::{Engine, Endpoint};
use super::api_shared::ApiQueryOpt;
use super::util;


/// Handles default aggregation when a format is not specified.
/// Default format is CSV.
pub fn api_default_handler(
    (req, endpoint): (HttpRequest<AppState>, Path<String>)
    ) -> FutureResponse<HttpResponse>
{
    let endpoint_format = (endpoint.into_inner(), "csv".to_owned());
    do_api(req, endpoint_format)
}


/// Handles aggregation when a format is specified.
pub fn api_handler(
    (req, endpoint_format): (HttpRequest<AppState>, Path<(String, String)>)
    ) -> FutureResponse<HttpResponse>
{
    do_api(req, endpoint_format.into_inner())
}


/// Performs data aggregation.
pub fn do_api(
    req: HttpRequest<AppState>,
    endpoint_format: (String, String),
    ) -> FutureResponse<HttpResponse>
{
    let (endpoint, format) = endpoint_format;

    let format = format.parse::<FormatType>();
    let format = match format {
        Ok(f) => f,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    info!("endpoint: {}, format: {:?}", endpoint, format);

    let schema_endpoint_res = req.state().schema
        .endpoints.iter()
        .find(|ept| ept.name == endpoint)
        .ok_or_else(|| format_err!("Couldn't find endpoint in schema"));

    let schema_endpoint = match schema_endpoint_res {
        Ok(x) => x,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        }
    };

    match &schema_endpoint.engine {
        Some(engine) => {
            match engine {
                Engine::Regular => perform_regular_query(req, &endpoint, format),
                Engine::Similarity => perform_similarity_query(req.clone(), &endpoint, &schema_endpoint, format)
            }
        },
        None => perform_regular_query(req, &endpoint, format)
    }
}


fn get_count(df: &DataFrame) -> u64 {
    match &df.columns[0].column_data {
        ColumnData::UInt64(data)=> {
            data[0]
        },
        _ => {
            // Should never get here as counts will always return UInt64
            0
        }
    }
}

fn perform_regular_query(
    req: HttpRequest<AppState>,
    endpoint: &str,
    format: FormatType,
) -> FutureResponse<HttpResponse> {
    let query = req.query_string();
    lazy_static!{
        static ref QS_NON_STRICT: qs::Config = qs::Config::new(5, false);
    }
    let api_query_res = QS_NON_STRICT.deserialize_str::<ApiQueryOpt>(&query);
    let api_query = match api_query_res {
        Ok(q) => q,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    info!("query opts:{:?}", api_query);

    // Turn ApiQueryOpt into Query
    let query: Result<Query, _> = api_query.try_into();
    let query = match query {
        Ok(q) => q,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    // Turn Query into QueryIr and headers (Vec<String>)
    let query_ir_headers = req
        .state()
        .schema
        .gen_query_ir(&endpoint, &query, &req.state().sql_templates);

    let (query_ir, headers) = match query_ir_headers {
        Ok(x) => x,
        Err(err) => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(err.to_string()))
                )
            );
        },
    };

    let sql_queries = req.state()
        .backend
        .generate_sql(query_ir);

    info!("Headers: {:?}", headers);

    // Joins all the futures for each TsQuery
    let futs: JoinAll<Vec<Box<dyn Future<Item=DataFrame, Error=Error>>>> = join_all(sql_queries
        .iter()
        .map(|sql| {
            info!("Sql query: {}", sql);

            req.state()
                .backend
                .exec_sql(sql.clone())
        })
        .collect()
    );

    // Process data received once all futures are resolved and return response
    futs
        .and_then(move |dfs| {
            let table_count = match dfs.get(0) {
                Some(df) => get_count(df),
                None => return Ok(HttpResponse::NotFound().json("Unable to get table count.".to_string()))
            };

            let filter_count = match dfs.get(1) {
                Some(df) => get_count(df),
                None => return Ok(HttpResponse::NotFound().json("Unable to get filter count.".to_string()))
            };

            let mut metadata = HashMap::new();

            metadata.insert("table_count".to_string(), table_count);
            metadata.insert("filter_count".to_string(), filter_count);

            let df = match dfs.get(2) {
                Some(df) => df.clone(),
                None => return Ok(HttpResponse::NotFound().json("Unable to get data.".to_string()))
            };

            let content_type = util::format_to_content_type(&format);

            match format_records(&headers, df, format, metadata) {
                Ok(res) => Ok(HttpResponse::Ok()
                    .set(content_type)
                    .body(res)),
                Err(err) => Ok(HttpResponse::NotFound().json(err.to_string())),
            }
        })
        .map_err(move |e| {
            error!("{}, {}", e.to_string(), e.as_fail());

            if req.state().debug {
                ServerError::Db { cause: e.to_string() }.into()
            } else {
                ServerError::Db { cause: "Internal Server Error 1010".to_owned() }.into()
            }
        })
        .responder()
}


use rustml::*;
use rustml::knn::scan;
use rustml::Matrix;

use std::{thread, time};


fn perform_similarity_query(
    req: HttpRequest<AppState>,
    endpoint: &str,
    schema_endpoint: &Endpoint,
    format: FormatType,
) -> FutureResponse<HttpResponse> {
    // TODO: Somehow store this into the app state
    // TODO: Build this from the actual data

    // TODO: Get this from the data
    let sql_str = "SELECT company_id, price, sic_code FROM sos_beryl_stock LIMIT 10".to_string();

    println!(" ");
    println!(" ");
    println!("STARTING REQUEST");
    println!(" ");
    println!(" ");

    let res_df = req.state()
        .backend
        .exec_sql(sql_str);

//        .wait()
//        .and_then(move |df| {
//            println!(" ");
//            println!(" ");
//            println!("{:?}", df);
//            println!(" ");
//            println!(" ");
//
//            Ok(df)
//        });
//
//    match res_df {
//        Ok(res_df) => {
//            println!(" ");
//            println!(" ");
//            println!("FINISHED REQUEST");
//            println!(" ");
//            println!(" ");
//        },
//        Err(err) => {
//            return Box::new(
//                future::result(
//                    Ok(HttpResponse::NotFound().json(
//                        err.to_string()
//                    ))
//                )
//            );
//        }
//    }

    let ten_millis = time::Duration::from_millis(10000);

    thread::sleep(ten_millis);

//    return Box::new(
//        future::result(
//            Ok(HttpResponse::NotFound().json(
//                "GOT THIS FAR".to_string()
//            ))
//        )
//    );





    let mut stock_tensor_map: HashMap<String, Vec<f32>> = HashMap::new();
    stock_tensor_map.insert("GOOG".to_string(), vec![1.0, 2.0]);
    stock_tensor_map.insert("AAPL".to_string(), vec![3.0, 4.0]);
    stock_tensor_map.insert("MSFT".to_string(), vec![5.0, 6.0]);

    let mut stock_index_map: HashMap<usize, String> = HashMap::new();
    stock_index_map.insert(0, "GOOG".to_string());
    stock_index_map.insert(1, "AAPL".to_string());
    stock_index_map.insert(2, "MSFT".to_string());

    let stock_tensor: Vec<Vec<f32>> = vec![
        vec![1.0, 2.0],
        vec![3.0, 4.0],
        vec![5.0, 6.0],
    ];

    let num_rows = stock_tensor.len();
    let num_cols = stock_tensor[0].len();

    let flat_stock_tensor = flatten_2d_tensor(&stock_tensor);

    // We need a Matrix object to do the actual KNN
    let stock_matrix = Matrix::<f32>::from_vec(
        flat_stock_tensor, num_rows, num_cols
    );

    /////////////////////////////////
    // WHEN A NEW REQUEST COMES IN //
    /////////////////////////////////

    let mut stock = match stock_tensor_map.get("AAPL") {
        Some(stock) => stock,
        None => {
            return Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(
                        "Unable to recognize provided ID".to_string()
                    ))
                )
            )
        }
    };

    // TODO: Get this from the query?
    let num_results = 2;

    // Perform the KNN search
    // k = num_results + 1, since the first result will always be an exact
    // match with the provided ID.
    let scan_res = scan(
        &stock_matrix,
        stock,
        num_results + 1,
        |x, y| Euclid::compute(x, y).unwrap()
    );

    match scan_res {
        Some(indexes) => {
            println!("RESULTS:");

            // Need to skip the first result, since it will just be the same as
            // the stock provided
            for index in &indexes {
                println!("{:?}: {:?}", index, stock_index_map.get(index));
            }

            Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(
                        indexes.to_string()
                    ))
                )
            )
        },
        None => {
            Box::new(
                future::result(
                    Ok(HttpResponse::NotFound().json(
                        "Error retrieving similar results".to_string()
                    ))
                )
            )
        }
    }
}

fn flatten_2d_tensor(tensor: &Vec<Vec<f32>>) -> Vec<f32> {
    let mut final_tensor = vec![];

    for vector in tensor {
        for entry in vector {
            final_tensor.push(entry.clone());
        }
    }

    final_tensor
}

//fn get_res_df(req: &HttpRequest<AppState>, sql_str: String) -> Result<DataFrame, Error> {
//    println!(" ");
//    println!(" ");
//    println!("MAKING THE REQUEST");
//    println!(" ");
//    println!(" ");
//
//    req.state()
//        .backend
//        .exec_sql(sql_str)
//        .wait()
//        .and_then(move |df| {
//            println!(" ");
//            println!(" ");
//            println!("AND THEN>>>");
//            println!(" ");
//            println!(" ");
//
//            Ok(df)
//        })
//}
