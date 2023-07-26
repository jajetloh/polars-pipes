use wasm_bindgen::prelude::*;

use std::collections::HashMap;
use polars::{prelude::{LazyFrame, col, JoinBuilder, JoinType, DataType, DataFrame, Series, NamedFrom, IntoLazy, PolarsDataType}, lazy::dsl::Expr};

use serde::{Deserialize, Serialize};
use serde_json::from_str;
use chrono::{NaiveDate, NaiveDateTime};

extern crate console_error_panic_hook;
use std::panic;

// pub fn add(left: usize, right: usize) -> usize {
//     left + right
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         let result = add(2, 2);
//         assert_eq!(result, 4);
//     }
// }

#[wasm_bindgen]
extern "C" {
    /// From https://rustwasm.github.io/wasm-bindgen/examples/console-log.html
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);

    // The `console.log` is quite polymorphic, so we can bind it with multiple
    // signatures. Note that we need to use `js_name` to ensure we always call
    // `log` in JS.
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_u64(a: u64);

    // Multiple arguments too!
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_many(a: &str, b: &str);
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceCsvPipeConfig {
    path: String,
    source_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinaryCalculationPipeConfig {
    pipe_id: String,
    new_column: String,
    column_1: String,
    column_2: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupAndReducePipeConfig {
    pipe_id: String,
    group_by: Vec<String>,
    aggs: Vec<AggConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum AggType {
    Sum,
    // Min,
    // Max,
    // First,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggConfig {
    name: String,
    r#type: AggType,
    value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinPipeConfig {
    left_pipe_id: String,
    right_pipe_id: String,
    on: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StringToDatePipeConfig {
    pipe_id: String,
    column_from: String,
    column_to: String,
    format: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag="type")]
pub enum PipeConfig {
    SourceCsv(SourceCsvPipeConfig),
    BinaryCalculation(BinaryCalculationPipeConfig),
    GroupAndReduce(GroupAndReducePipeConfig),
    Join(JoinPipeConfig),
    // StringToDate(StringToDatePipeConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum PipeConfigType {
    SourceCsv,
    BinaryCalculation,
    Join,
    // StringToDate,
}
pub struct LazyFrameFactory {
    lazy_frames: HashMap<String, LazyFrame>,
    pipe_configs: HashMap<String, PipeConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataTable {
    f64: HashMap<String, Vec<Option<f64>>>,
    i64: HashMap<String, Vec<Option<i64>>>,
    str: HashMap<String, Vec<Option<String>>>,
    datetime: HashMap<String, Vec<Option<i64>>>,
}

fn data_table_to_frame(table: &DataTable) -> LazyFrame {
    let mut series_vec: Vec<Series> = Vec::new();
    table.f64.iter().for_each(|(name, values)| {
        series_vec.push(Series::new(name, values))
    });
    table.i64.iter().for_each(|(name, values)| {
        series_vec.push(Series::new(name, values))
    });
    table.str.iter().for_each(|(name, values)| {
        series_vec.push(Series::new(name, values))
    });
    table.datetime.iter().for_each(|(name, values)| {
        series_vec.push(Series::new(name, values))
    });
    DataFrame::new(series_vec).unwrap().lazy()
}

fn data_frame_to_table(lf: LazyFrame) -> Result<DataTable, String> {
    let mut data_table = DataTable {
        f64: HashMap::new(),
        i64: HashMap::new(),
        str: HashMap::new(),
        datetime: HashMap::new(),
    };

    let frame = match lf.collect() {
        Ok(x) => x,
        Err(e) => { log(&format!("{:?}", e)); return Err(e.to_string()) },
    };
    let schema = frame.schema();
    let mut schema_dtype_iter = schema.iter_dtypes();
    let mut column_iters = frame.iter();
    for i in 0..frame.width() {
        let dtype = schema_dtype_iter.next().unwrap();
        let column = column_iters.next().unwrap();
        match dtype {
            DataType::Float64 => {
                let values = column.f64().unwrap().into_iter().collect();
                data_table.f64.insert(column.name().into(), values);
            },
            DataType::Int64 => {
                let values = column.i64().unwrap().into_iter().collect();
                data_table.i64.insert(column.name().into(), values);
            },
            DataType::Utf8 => {
                let values = column.utf8().unwrap().into_iter().map(|x| match x { None => None, Some(y) => Some(y.to_string())}).collect();
                data_table.str.insert(column.name().into(), values);
            },
            // TODO: Handle dates...
            _ => return Err("!!".into()),
        }
    };
    Ok(data_table)
}

#[wasm_bindgen]
pub fn run_data_pipeline(pipe_ids: JsValue, input_data: JsValue, configs: JsValue) -> Result<JsValue, JsValue> {
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    log("start run_data_pipeline");
    let pipe_configs: HashMap<String, PipeConfig> = match serde_wasm_bindgen::from_value(configs) {
        Ok(x) => x,
        Err(e) => { log(&format!("Error parsing pipe_configs: {:?}", e)); return Err(e.into()) }
    };
    let pipes: Vec<String> = match serde_wasm_bindgen::from_value(pipe_ids) {
        Ok(x) => x,
        Err(e) => { log(&format!("Error parsing pipe_ids: {:?}", e)); return Err(e.into()) }
    };
    let inputs: HashMap<String, DataTable> = match serde_wasm_bindgen::from_value(input_data) {
        Ok(x) => x,
        Err(e) => { log(&format!("Error parsing input_data: {:?}", e)); return Err(e.into()) }
    };
    let mut lazy_inputs: HashMap<String, LazyFrame> = HashMap::new();
    for (key, value) in inputs.iter() {
        lazy_inputs.insert(key.to_string(), data_table_to_frame(value));
    }
    log("lazy_inputs ready");

    let lff = LazyFrameFactory {
        pipe_configs,
        lazy_frames: lazy_inputs,
    };
    Ok(serde_wasm_bindgen::to_value(&lff.create_lazy_frame(pipes.iter().next().unwrap()).unwrap()).unwrap())
}

impl LazyFrameFactory {
    pub fn create_lazy_frame(self: &Self, pipe_id: &String) -> Result<DataTable, String> {
        log("Start create_lazy_frame");
        let result_lf = match self.pipe_configs.get(pipe_id) {
            Some(config) => self.recurse(&config),
            None => Err(format!("No pipe with id {} found in pipe_configs", pipe_id)),
        }.unwrap();
        log("Obtained result_lf");
        let result_table = match data_frame_to_table(result_lf) {
            Ok(x) => x,
            Err(e) => { log(&format!("Error converting lazyframe to table formats {:?}", e)); return Err(e.into()) },
        };
        log("End create_lazy_frame");
        return Ok(result_table)
    }

    fn recurse(self: &Self, c: &PipeConfig) -> Result<LazyFrame, String> {
        log("Start recurse");
        match c {
            PipeConfig::SourceCsv(config) => {
                log("Matched to SourceCSV");
                log(&format!("Config is {:?}", config));
                let data_lf = match self.lazy_frames.get(&config.source_id) {
                    Some(x) => x,
                    None => {
                        let msg = format!("No data source with id {:?} was found in inputs array.", config.source_id);
                        log(&msg);
                        return Err(msg)
                    },
                }.clone();
                log("SourceCsv data_lf successful");
                Ok(data_lf)
            },
            PipeConfig::BinaryCalculation(config) => {
                let upstream_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(&upstream_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                Ok(lf.with_column((col(&config.column_1) + col(&config.column_2)).alias(&config.new_column)))
            },
            PipeConfig::GroupAndReduce(config) => {
                let upstream_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(&upstream_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let groupby = match config.group_by.len() {
                    0 => { return Err("Cannot group by zero variables".into()) },
                    1 => lf.groupby([ col(&config.group_by[0]) ]),
                    2 => lf.groupby([ col(&config.group_by[0]), col(&config.group_by[1]) ]),
                    3 => lf.groupby([ col(&config.group_by[0]), col(&config.group_by[1]), col(&config.group_by[2]) ]),
                    4 => lf.groupby([ col(&config.group_by[0]), col(&config.group_by[1]), col(&config.group_by[2]), col(&config.group_by[3]) ]),
                    5 => lf.groupby([ col(&config.group_by[0]), col(&config.group_by[1]), col(&config.group_by[2]), col(&config.group_by[3]), col(&config.group_by[4]) ]),
                    6 => lf.groupby([ col(&config.group_by[0]), col(&config.group_by[1]), col(&config.group_by[2]), col(&config.group_by[3]), col(&config.group_by[4]), col(&config.group_by[5]) ]),
                    _ => { return Err("Too many groupby variables. Need to implement.".into()) },
                };
                let lf_out = groupby.agg(
                    config.clone().aggs.into_iter().map(|c| {
                        match c.r#type {
                            AggType::Sum => {
                                col(&c.value).sum().alias(&c.name)
                            },
                        }
                    }).collect::<Vec<_>>()
                );
                Ok(lf_out)
            },
            PipeConfig::Join(config) => {
                let left_config = match self.pipe_configs.get(&config.left_pipe_id) {
                    Some(c) => c,
                    None => { println!("Could not find pipe_id {} in pipe_configs", config.left_pipe_id); return Err(format!("Could not find pipe_id {} in pipe_configs", config.left_pipe_id)) },
                };
                let right_config = match self.pipe_configs.get(&config.right_pipe_id) {
                    Some(c) => c,
                    None => { println!("Could not find pipe_id {} in pipe_configs", config.right_pipe_id); return Err(format!("Could not find pipe_id {} in pipe_configs", config.right_pipe_id)) },
                };
                let left_lf = match self.recurse(&left_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let right_lf = match self.recurse(&right_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let jb = JoinBuilder::new(left_lf).with(right_lf).how(JoinType::Left);
                let jb_join = match config.on.len() {
                    1 => jb.on(&[col(&config.on[0])]),
                    2 => jb.on(&[col(&config.on[0]), col(&config.on[1])]),
                    3 => jb.on(&[col(&config.on[0]), col(&config.on[1]), col(&config.on[2])]),
                    4 => jb.on(&[col(&config.on[0]), col(&config.on[1]), col(&config.on[2]), col(&config.on[3])]),
                    5 => jb.on(&[col(&config.on[0]), col(&config.on[1]), col(&config.on[2]), col(&config.on[3]), col(&config.on[4])]),
                    6 => jb.on(&[col(&config.on[0]), col(&config.on[1]), col(&config.on[2]), col(&config.on[3]), col(&config.on[4]), col(&config.on[5])]),
                    7 => jb.on(&[col(&config.on[0]), col(&config.on[1]), col(&config.on[2]), col(&config.on[3]), col(&config.on[4]), col(&config.on[5]), col(&config.on[6])]),
                    8 => jb.on(&[col(&config.on[0]), col(&config.on[1]), col(&config.on[2]), col(&config.on[3]), col(&config.on[4]), col(&config.on[5]), col(&config.on[6]), col(&config.on[7])]),
                    _ => return Err(format!("Too many arguments to join on in pipe_id {:?}", config)),
                };
                Ok(jb_join.finish())
            },
            // PipeConfigType::StringToDate => {
            //     let config = from_str::<StringToDatePipeConfig>(&config_str).unwrap();
            //     let (child_config_type, child_config) = match self.pipe_configs.get(&config.pipe_id) {
            //         Some(v) => v,
            //         None => { println!("Unable to find pipe_id {}", config.pipe_id); return Err(format!("Unable to find pipe_id {}", config.pipe_id)) },
            //     };
            //     let lf = match self.recurse(&child_config, &child_config_type) {
            //         Ok(lf) => lf,
            //         Err(e) => return Err(e),
            //     };
            //     let options = StrptimeOptions { format: Some(String::from(config.format)), strict: true, exact: true, cache: false };
            //     return Ok(lf.with_column(col(&config.column_from).str().strptime(DataType::Date, options).alias(&config.column_to)))
            // }
        }
    }
}
