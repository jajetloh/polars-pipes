use std::collections::HashMap;
use polars::prelude::{LazyFrame, LazyCsvReader, LazyFileListReader, col, JoinBuilder, JoinType, DataType, StrptimeOptions};
use serde::{Deserialize, Serialize};

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


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceCsvPipeConfig {
    path: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BinaryCalculationPipeConfig {
    pipe_id: String,
    new_column: String,
    column_1: String,
    column_2: String,
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
    Join(JoinPipeConfig),
    StringToDate(StringToDatePipeConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LazyFrameFactory {
    pipe_configs: HashMap<String, PipeConfig>,
}

impl LazyFrameFactory {
    pub fn create_lazy_frame(self: &Self, pipe_id: String) -> Result<LazyFrame, String> {
        match self.pipe_configs.get(&pipe_id) {
            Some(config) => self.recurse(config.clone()),
            None => return Err(format!("No pipe with id {} found in pipe_configs", pipe_id)),
        }
        
    }

    fn recurse(self: &Self, config: PipeConfig) -> Result<LazyFrame, String> {
        match config {
            PipeConfig::SourceCsv(config) => {
                match LazyCsvReader::new(config.path.clone()).finish() {
                    Ok(lf) => Ok(lf),
                    Err(e) => { println!("Error when scanning csv at path {}: {}", config.path, e); return Err(e.to_string()) },
                }
            },
            PipeConfig::BinaryCalculation(config) => {
                let source_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(source_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                Ok(lf.with_column((col(&config.column_1) + col(&config.column_2)).alias(&config.new_column)))
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
                let left_lf = match self.recurse(left_config.clone()) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let right_lf = match self.recurse(right_config.clone()) {
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
            PipeConfig::StringToDate(config) => {
                let child_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(v) => v,
                    None => { println!("Unable to find pipe_id {}", config.pipe_id); return Err(format!("Unable to find pipe_id {}", config.pipe_id)) },
                };
                let lf = match self.recurse(child_config.clone()) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let options = StrptimeOptions { format: Some(String::from(config.format)), strict: true, exact: true, cache: false };
                return Ok(lf.with_column(col(&config.column_from).str().strptime(DataType::Date, options).alias(&config.column_to)))
            }
        }
    }
}
