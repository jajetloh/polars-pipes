use wasm_bindgen::prelude::*;

use std::collections::HashMap;
use polars::{prelude::{LazyFrame, LazyCsvReader, col, JoinBuilder, JoinType, DataType, DataFrame, Series, NamedFrom, IntoLazy, PolarsDataType}, df};
// use polars::prelude::{LazyFileListReader, StrptimeOptions};

use serde::{Deserialize, Serialize};
use serde_json::from_str;
use chrono::{NaiveDate, NaiveDateTime};

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

#[wasm_bindgen]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceCsvPipeConfig {
    path: String,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinaryCalculationPipeConfig {
    pipe_id: String,
    new_column: String,
    column_1: String,
    column_2: String,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinPipeConfig {
    left_pipe_id: String,
    right_pipe_id: String,
    on: Vec<String>,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StringToDatePipeConfig {
    pipe_id: String,
    column_from: String,
    column_to: String,
    format: String,
}

#[wasm_bindgen]
#[derive(Debug, Clone, Deserialize, Serialize)]
// #[serde(tag="type")]
// pub enum PipeConfig {
//     SourceCsv(SourceCsvPipeConfig),
//     BinaryCalculation(BinaryCalculationPipeConfig),
//     Join(JoinPipeConfig),
//     StringToDate(StringToDatePipeConfig),
// }
pub enum PipeConfigType {
    SourceCsv,
    BinaryCalculation,
    Join,
    // StringToDate,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum PipeConfig {
    SourceCsv,
    BinaryCalculation,
    Join,
    // StringToDate,
}

// #[wasm_bindgen]
// #[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LazyFrameFactory {
    pipe_configs: HashMap<String, (PipeConfigType, String)>,
    lazy_frames: HashMap<String, LazyFrame>,
    // pipe_configs: HashMap<String, PipeConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataTable {
    f64: Option<HashMap<String, Vec<Option<f64>>>>,
    i64: Option<HashMap<String, Vec<Option<i64>>>>,
    str: Option<HashMap<String, Vec<Option<String>>>>,
    datetime: Option<HashMap<String, Vec<Option<i64>>>>,
}

#[derive(Serialize)]
pub struct DataTable2<T> {
    data: Vec<HashMap<String, T>>
}

// #[wasm_bindgen]
// pub fn do_thing() -> Result<JsValue, JsValue> {
//     let mut lff = LazyFrameFactory {
//         pipe_configs: HashMap::new(),
//         lazy_frames: HashMap::new(),
//     };
//     lff.pipe_configs.insert("SourceOne".into(), (PipeConfigType::SourceCsv, "{\"path\": \"hello.csv\"}".into()));
//     Ok(serde_wasm_bindgen::to_value(&lff.create_lazy_frame(String::from("SourceOne")).unwrap())?)
// }

// fn append_series<T>(map: HashMap<String, Vec<Option<T>>>, mut series_vec: Vec<Series>) {
//     map.iter().for_each(|(name, values)| {
//         series_vec.push(Series::new(name, values))
//     });
// }

fn data_table_to_frame(table: &DataTable) -> LazyFrame {
    let mut series_vec: Vec<Series> = Vec::new();
    match &table.f64 {
        None => (),
        Some(x) => {
            x.iter().for_each(|(name, values)| {
                series_vec.push(Series::new(name, values))
            })
        }
    };
    match &table.i64 {
        None => (),
        Some(x) => {
            x.iter().for_each(|(name, values)| {
                series_vec.push(Series::new(name, values))
            })
        }
    };
    match &table.str {
        None => (),
        Some(x) => {
            x.iter().for_each(|(name, values)| {
                series_vec.push(Series::new(name, values))
            })
        }
    };
    match &table.datetime {
        None => (),
        Some(x) => {
            x.iter().for_each(|(name, values)| {
                series_vec.push(Series::new(name, values))
            })
        }
    };
    DataFrame::new(series_vec).unwrap().lazy()
}

#[wasm_bindgen]
pub fn run_data_pipeline(pipe_ids: JsValue, input_data: JsValue, configs: JsValue) -> Result<JsValue, JsValue> {
    let pipe_configs: HashMap<String, (PipeConfigType, String)> = serde_wasm_bindgen::from_value(configs).unwrap();
    let pipes: Vec<String> = serde_wasm_bindgen::from_value(pipe_ids).unwrap();
    let inputs: HashMap<String, DataTable> = serde_wasm_bindgen::from_value(input_data).unwrap();
    let mut lazy_inputs: HashMap<String, LazyFrame> = HashMap::new();
    for (key, value) in inputs.iter() {
        lazy_inputs.insert(key.to_string(), data_table_to_frame(value));
    }

    // todo!();
    let lff = LazyFrameFactory {
        pipe_configs,
        lazy_frames: lazy_inputs,
    };

    Ok(serde_wasm_bindgen::to_value(&lff.create_lazy_frame(pipes.iter().next().unwrap()).unwrap()).unwrap())
}

#[wasm_bindgen]
pub fn do_thing_2(val: JsValue) -> Result<(), JsValue> {
    log("do_thing_2");
    log(&format!("{:?}", val));
    let example: DataTable = match serde_wasm_bindgen::from_value(val) {
        Ok(x) => x,
        Err(e) => return Err(e.to_string().into()),
    };
    log(&format!("!!! {:?}", example));
    // do_thing()
    Ok(())
}

// pub type DataTable<T> = HashMap<String, Vec<T>>;

impl LazyFrameFactory {
    pub fn create_lazy_frame(self: &Self, pipe_id: &String) -> Result<DataTable, String> {
    // pub fn create_lazy_frame(self: &Self, pipe_id: String) -> Result<LazyFrame, String> {
        log("Start create_lazy_frame");
        let _result_lf = match self.pipe_configs.get(pipe_id) {
            Some((config_type, config_str)) => self.recurse(&config_str, config_type),
            None => Err(format!("No pipe with id {} found in pipe_configs", pipe_id)),
        }.unwrap();
        log("End create_lazy_frame");
        // return Ok(String::from("wow"))

        let c = DataTable {
            f64: Some(HashMap::from([
                ("col1".into(), vec![Some(1.23), Some(2.34), Some(3.45), Some(4.56)]),
                ("col5".into(), vec![Some(0.23), Some(0.34), Some(0.45), Some(0.56)]),
            ])),
            i64: Some(HashMap::from([("col2".into(), vec![Some(5),Some(6),Some(7),Some(8)])])),
            str: Some(HashMap::from([("col3".into(), vec![Some("hello".into()), Some("fwo".into()), None, Some("fe2f".into())])])),
            // datetime: Some(HashMap::from([("col4".into(), vec![Some(NaiveDate::from_ymd_opt(2023,1,1).unwrap().and_hms_opt(1,2,3).unwrap()), None, None, None])])),
            datetime: Some(HashMap::from([("col4".into(), vec![Some(5),Some(6),Some(7),Some(8)])])),
        };
        return Ok(c)

        // let mut c: HashMap<String, Vec<f64 | String>> = HashMap::new();
        // c.insert("column1".into(), vec![Some(0.123), Some(0.234), None]);
        // c.insert("column2".into(), vec![Some(9.876), None, Some(2.1)]);
        // return Ok(DataTable { data: c })

        // let c = vec![
        //     HashMap::<String, f64>::from([ ("a".into(), 1.), ("b".into(), 2.) ]),
        //     HashMap::<String, f64>::from([ ("a".into(), 4.), ("b".into(), 8.) ]),
        //     HashMap::<String, f64>::from([ ("a".into(), 3.), ("b".into(), 9.) ]),
        // ];
        // return Ok(DataTable2 { data: c })

        // return Ok(vec![0.,1.,2.,3.])
    }

    fn recurse(self: &Self, config_str: &String, config_type: &PipeConfigType) -> Result<LazyFrame, String> {
        log("Start recurse");
        match config_type {
            PipeConfigType::SourceCsv => {
                log("Matched to SourceCSV");
                log(&format!("Config is {:?}", config_str));
                let config = match from_str::<SourceCsvPipeConfig>(&config_str) {
                    Ok(x) => x,
                    Err(e) => { log(&format!("ohno! Error when parsing config: {:?}", e)); return Err(e.to_string()) },
                };
                log(&format!("{:?}", config));
                let df_ = DataFrame::new(vec![
                    Series::new("hello", [1,2,3]),
                    Series::new("b", [2,3,4]),
                ]).unwrap(); 
                let lf = Ok(df_.lazy());
                // let lf = match LazyCsvReader::new(config.path.clone()).finish() {
                //     Ok(lf) => Ok(lf),
                //     Err(e) => { println!("Error when scanning csv at path {}: {}", config.path, e); return Err(e.to_string()) },
                // };
                log("Result<LazyFrame> obtained");
                lf
            },
            PipeConfigType::BinaryCalculation => {
                let config = from_str::<BinaryCalculationPipeConfig>(&config_str).unwrap();
                let (upstream_config_type, upstream_config) = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(&upstream_config, &upstream_config_type) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                Ok(lf.with_column((col(&config.column_1) + col(&config.column_2)).alias(&config.new_column)))
            },
            PipeConfigType::Join => {
                let config = from_str::<JoinPipeConfig>(&config_str).unwrap();
                let (left_config_type, left_config) = match self.pipe_configs.get(&config.left_pipe_id) {
                    Some(c) => c,
                    None => { println!("Could not find pipe_id {} in pipe_configs", config.left_pipe_id); return Err(format!("Could not find pipe_id {} in pipe_configs", config.left_pipe_id)) },
                };
                let (right_config_type, right_config) = match self.pipe_configs.get(&config.right_pipe_id) {
                    Some(c) => c,
                    None => { println!("Could not find pipe_id {} in pipe_configs", config.right_pipe_id); return Err(format!("Could not find pipe_id {} in pipe_configs", config.right_pipe_id)) },
                };
                let left_lf = match self.recurse(&left_config, &left_config_type) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let right_lf = match self.recurse(&right_config, &right_config_type) {
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
