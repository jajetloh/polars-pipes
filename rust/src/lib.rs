use wasm_bindgen::prelude::*;

use std::collections::HashMap;
use polars::{prelude::{LazyFrame, col, lit, JoinBuilder, JoinType, DataType, DataFrame, Series, NamedFrom, IntoLazy, min_horizontal, max_horizontal, TimeUnit}, lazy::dsl::{Expr, when}};

use serde::{Deserialize, Serialize};
use chrono::Utc;

extern crate console_error_panic_hook;
use std::panic;

#[wasm_bindgen]
extern "C" {
    /// From https://rustwasm.github.io/wasm-bindgen/examples/console-log.html
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SourcePipeConfig {
    source_id: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DerivedValuesPipeConfig {
    pipe_id: String,
    calcs: Vec<DerivedValuesExpressionRoot>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]

pub struct DerivedValuesExpressionRoot {
    name: String,
    expression: DerivedValuesExpression,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum DerivedValuesOperationType {
    Sum,
    Subtract,
    Multiply,
    Divide,
    Min,
    Max,
    Not,
    And,
    Or,
    LessThan,
    LessThanEq,
    GreaterThan,
    GreaterThanEq,
    IfThenElse,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DerivedValuesOperation {
    operation: DerivedValuesOperationType,
    operands: Vec<DerivedValuesExpression>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DerivedValuesProperty {
    property: String
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum DerivedValuesExpression {
    Expression(DerivedValuesOperation),
    WindowAggExpression(DerivedValuesWindowAggExpression),
    Variable(DerivedValuesProperty),
    Literal(f64),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DerivedValuesWindowAggExpression {
    operation: AggType,
    operand: Box<DerivedValuesExpression>,
    over: Vec<String>,
}

fn recurse_derived_expression(expression: DerivedValuesExpression) -> Result<Expr, String> {
    let polars_expr: Result<Expr, String> = match expression {
        DerivedValuesExpression::Expression(e) => {
            let operand_polars_exprs_result = e.operands.iter()
                .map(|operand| recurse_derived_expression(operand.clone()))
                .collect::<Result<Vec<Expr>, String>>();
            let pl_exprs_vec: Vec<Expr> = match operand_polars_exprs_result {
                Ok(x) => x,
                Err(e) => return Err(e),
            };
            match e.operation {
                DerivedValuesOperationType::Sum => {
                    let sum_expr = pl_exprs_vec.iter().fold(lit(0), |acc: Expr, x: &Expr| {
                        acc + x.clone()
                    });
                    return Ok(sum_expr)
                },
                DerivedValuesOperationType::Subtract => {
                    let sub_expr = pl_exprs_vec.iter().skip(1).fold(pl_exprs_vec[0].clone(), |acc: Expr, x: &Expr| {
                        acc - x.clone()
                    });
                    return Ok(sub_expr)
                },
                DerivedValuesOperationType::Multiply => {
                    let sum_expr = pl_exprs_vec.iter().fold(lit(0), |acc: Expr, x: &Expr| {
                        acc * x.clone()
                    });
                    return Ok(sum_expr)
                },
                DerivedValuesOperationType::Divide => {
                    let sub_expr = pl_exprs_vec.iter().skip(1).fold(pl_exprs_vec[0].clone(), |acc: Expr, x: &Expr| {
                        acc / x.clone()
                    });
                    return Ok(sub_expr)
                },
                DerivedValuesOperationType::Min => {
                    let min_expr = match pl_exprs_vec.len() {
                        0 => { return Err("'Min' requires at least one operand.".into()) },
                        1 => min_horizontal([pl_exprs_vec[0].clone()]),
                        2 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone()]),
                        3 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone()]),
                        4 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone()]),
                        5 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone()]),
                        6 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone(), pl_exprs_vec[5].clone()]),
                        7 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone(), pl_exprs_vec[5].clone(), pl_exprs_vec[6].clone()]),
                        8 => min_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone(), pl_exprs_vec[5].clone(), pl_exprs_vec[6].clone(), pl_exprs_vec[7].clone()]),
                        _ => { return Err("Too many operands to min by. Need to implement.".into()) },
                    };
                    Ok(min_expr)
                },
                DerivedValuesOperationType::Max => {
                    let max_expr = match pl_exprs_vec.len() {
                        0 => { return Err("'Max' requires at least one operand.".into()) },
                        1 => max_horizontal([pl_exprs_vec[0].clone()]),
                        2 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone()]),
                        3 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone()]),
                        4 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone()]),
                        5 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone()]),
                        6 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone(), pl_exprs_vec[5].clone()]),
                        7 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone(), pl_exprs_vec[5].clone(), pl_exprs_vec[6].clone()]),
                        8 => max_horizontal([pl_exprs_vec[0].clone(), pl_exprs_vec[1].clone(), pl_exprs_vec[2].clone(), pl_exprs_vec[3].clone(), pl_exprs_vec[4].clone(), pl_exprs_vec[5].clone(), pl_exprs_vec[6].clone(), pl_exprs_vec[7].clone()]),
                        _ => { return Err("Too many operands to min by. Need to implement.".into()) },
                    };
                    Ok(max_expr)
                },
                DerivedValuesOperationType::Not => {
                    match pl_exprs_vec.len() {
                        1 => Ok(pl_exprs_vec[0].clone().not()),
                        _ => return Err(format!("'Not' must have exactly 1 operand ({} found)", pl_exprs_vec.len()).into())
                    }
                },
                DerivedValuesOperationType::And => {
                    let expr = pl_exprs_vec.iter().fold(lit(0), |acc: Expr, x: &Expr| {
                        acc.and(x.clone())
                    });
                    return Ok(expr)
                },
                DerivedValuesOperationType::Or => {
                    let expr = pl_exprs_vec.iter().fold(lit(0), |acc: Expr, x: &Expr| {
                        acc.or(x.clone())
                    });
                    return Ok(expr)
                },
                DerivedValuesOperationType::LessThan => {
                    match pl_exprs_vec.len() {
                        2 => Ok(pl_exprs_vec[0].clone().lt(pl_exprs_vec[1].clone())),
                        _ => return Err(format!("'LessThan' must have exactly 2 operands ({} found)", pl_exprs_vec.len()).into())
                    }
                },
                DerivedValuesOperationType::LessThanEq => {
                    match pl_exprs_vec.len() {
                        2 => Ok(pl_exprs_vec[0].clone().lt_eq(pl_exprs_vec[1].clone())),
                        _ => return Err(format!("'LessThanEq' must have exactly 2 operands ({} found)", pl_exprs_vec.len()).into())
                    }
                },
                DerivedValuesOperationType::GreaterThan => {
                    match pl_exprs_vec.len() {
                        2 => Ok(pl_exprs_vec[0].clone().gt(pl_exprs_vec[1].clone())),
                        _ => return Err(format!("'GreaterThan' must have exactly 2 operands ({} found)", pl_exprs_vec.len()).into())
                    }
                },
                DerivedValuesOperationType::GreaterThanEq => {
                    match pl_exprs_vec.len() {
                        2 => Ok(pl_exprs_vec[0].clone().gt_eq(pl_exprs_vec[1].clone())),
                        _ => return Err(format!("'GreaterThanEq' must have exactly 2 operands ({} found)", pl_exprs_vec.len()).into())
                    }
                },
                DerivedValuesOperationType::IfThenElse => {
                    if pl_exprs_vec.len() % 2 == 0 {
                        return Err("Must have an odd number of operands".into());
                    } else if pl_exprs_vec.len() < 3 {
                        return Err("Not enough operands".into());
                    }
                    // Next line is a workaround to get an initial WhenThenThen struct
                    let mut expr = when(lit(false)).then(lit(false)).when(lit(false)).then(lit(false));
                    for i in 0..(pl_exprs_vec.len() / 2) {
                        log(&format!("i={}, doing {} {}, total length {}", i, i, i+1, pl_exprs_vec.len()));
                        expr = expr.when(pl_exprs_vec[2*i].clone()).then(pl_exprs_vec[2*i+1].clone());
                    }
                    let final_expr = expr.otherwise(pl_exprs_vec[pl_exprs_vec.len() - 1].clone());
                    return Ok(final_expr)
                },
            }
        },
        DerivedValuesExpression::WindowAggExpression(expr) => {
            let operand_expr = match recurse_derived_expression(*expr.operand.clone()) {
                Ok(x) => x,
                Err(e) => return Err(e)
            };
            if expr.over.len() > 1 {
                return Err("Window expression over more than one variable not supported yet".into())
            }
            let over_as_boxed_slice = expr.over.iter().map(|x| lit(x.clone())).collect::<Vec<_>>().into_boxed_slice();
            match expr.operation {
                AggType::Sum => {
                    let new_expr = operand_expr.sum().over(over_as_boxed_slice);
                    return Ok(new_expr)
                },
                AggType::Max => {
                    let new_expr = operand_expr.max().over(over_as_boxed_slice);
                    return Ok(new_expr)
                },
                AggType::Min => {
                    let new_expr = operand_expr.min().over(over_as_boxed_slice);
                    return Ok(new_expr)
                }
            }
        },
        DerivedValuesExpression::Literal(x) => Ok(lit(x)),
        DerivedValuesExpression::Variable(y) => Ok(col(&y.property))
    };
    return polars_expr
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupAndReducePipeConfig {
    pipe_id: String,
    group_by: Vec<String>,
    aggs: Vec<AggConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
enum AggType {
    Sum,
    Max,
    Min,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AggConfig {
    name: String,
    r#type: AggType,
    agg_property: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterPipeConfig {
    pipe_id: String,
    filters: Vec<DerivedValuesExpression>
}


#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinPipeConfig {
    left_pipe_id: String,
    right_pipe_id: String,
    on: Vec<String>,
    how: JoinPipeType,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum JoinPipeType {
    Left,
    Right,
    Inner,
    Outer,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RenamePipeConfig {
    pipe_id: String,
    properties: Vec<RenamePropertyConfig>
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RenamePropertyConfig {
    from: String,
    to: String
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StringToDatePipeConfig {
    pipe_id: String,
    column_from: String,
    column_to: String,
    format: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag="type")]
pub enum PipeConfig {
    Source(SourcePipeConfig),
    DerivedValues(DerivedValuesPipeConfig),
    GroupAndReduce(GroupAndReducePipeConfig),
    Filter(FilterPipeConfig),
    Join(JoinPipeConfig),
    Rename(RenamePipeConfig),
    // StringToDate(StringToDatePipeConfig),
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
    bool: HashMap<String, Vec<Option<bool>>>,
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
        bool: HashMap::new(),
    };

    let frame = match lf.collect() {
        Ok(x) => x,
        Err(e) => { log(&format!("{:?}", e)); return Err(e.to_string()) },
    };
    let schema = frame.schema();
    let mut schema_dtype_iter = schema.iter_dtypes();
    let mut column_iters = frame.iter();
    for _ in 0..frame.width() {
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
            DataType::Boolean => {
                let values = column.bool().unwrap().into_iter().collect();
                data_table.bool.insert(column.name().into(), values);
            },
            DataType::Datetime(unit, tz) => {
                todo!();
                // let values: Vec<_> = match unit {
                //     TimeUnit::Milliseconds => column.datetime().unwrap().into_iter().collect(),
                //     TimeUnit::Microseconds => column.datetime().unwrap().into_iter().collect(),
                //     TimeUnit::Nanoseconds => column.datetime().unwrap().into_iter().collect(),
                // };
                // data_table.datetime.insert(column.name().into(), values);
            },
            _ => return Err("!!".into()),
        }
    };
    Ok(data_table)
}

#[wasm_bindgen]
pub fn runDataPipeline(pipe_ids: JsValue, input_data: JsValue, configs: JsValue) -> Result<JsValue, JsValue> {
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
            PipeConfig::Source(config) => {
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
            PipeConfig::DerivedValues(config) => {
                let upstream_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(&upstream_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let pl_exprs_vec: Vec<Expr> = match config.calcs.iter().map(|calc| {
                    match recurse_derived_expression(calc.expression.clone()) {
                        Ok(y) => Ok(y.alias(&calc.name)),
                        Err(e) => Err(e),
                    }
                }).collect::<Result<Vec<Expr>, String>>() {
                    Ok(x) => x,
                    Err(e) => return Err(e),
                };
                let final_lf = pl_exprs_vec.iter().fold(lf.clone(), |acc_lf, expr| acc_lf.with_column(expr.clone()) );
                Ok(final_lf)
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
                                col(&c.agg_property).sum().alias(&c.name)
                            },
                            AggType::Max => {
                                col(&c.agg_property).max().alias(&c.name)
                            },
                            AggType::Min => {
                                col(&c.agg_property).min().alias(&c.name)
                            },
                        }
                    }).collect::<Vec<_>>()
                );
                Ok(lf_out)
            },
            PipeConfig::Filter(config) => {
                let upstream_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(&upstream_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                let pl_exprs_vec: Vec<Expr> = match config.filters.iter().map(|calc| {
                    match recurse_derived_expression(calc.clone()) {
                        Ok(y) => Ok(y),
                        Err(e) => Err(e),
                    }
                }).collect::<Result<Vec<Expr>, String>>() {
                    Ok(x) => x,
                    Err(e) => return Err(e),
                };
                let final_lf = pl_exprs_vec.iter().fold(lf.clone(), |acc_lf, expr| acc_lf.filter(expr.clone()) );
                Ok(final_lf)
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
                let jb: JoinBuilder = match config.how {
                    JoinPipeType::Left => JoinBuilder::new(left_lf).with(right_lf).how(JoinType::Left),
                    JoinPipeType::Right => JoinBuilder::new(right_lf).with(left_lf).how(JoinType::Left),
                    JoinPipeType::Inner => JoinBuilder::new(left_lf).with(right_lf).how(JoinType::Inner),
                    JoinPipeType::Outer => JoinBuilder::new(left_lf).with(right_lf).how(JoinType::Outer),
                };
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
            PipeConfig::Rename(config) => {
                let upstream_config = match self.pipe_configs.get(&config.pipe_id) {
                    Some(c) => c.clone(),
                    None => { println!("Pipe id {} not found", config.pipe_id); return Err(format!("Pipe id {} not found", config.pipe_id)) },
                };
                let lf = match self.recurse(&upstream_config) {
                    Ok(lf) => lf,
                    Err(e) => return Err(e),
                };
                Ok(lf.rename(config.properties.iter().map(|x| x.from.clone()), config.properties.iter().map(|x| x.to.clone())))
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

#[wasm_bindgen]
pub fn getSourcePipes(configs: JsValue) -> Result<JsValue, String> {
    panic::set_hook(Box::new(console_error_panic_hook::hook));

    let pipe_configs: Vec<PipeConfig> = match serde_wasm_bindgen::from_value(configs) {
        Ok(c) => c,
        Err(e) => { log(&format!("Error parsing pipe_configs: {:?}", e)); return Err(e.to_string()) }
    };
    let result = pipe_configs.iter().map(get_source_pipes_for_single).collect::<Vec<_>>();
    let result_jsvalue = match serde_wasm_bindgen::to_value(&result) {
        Ok(x) => x,
        Err(e) => { log(&format!("Error converting result to JsValue: {:?}", e)); return Err(e.to_string()) }
    };
    Ok(result_jsvalue)
}

fn get_source_pipes_for_single(config: &PipeConfig) -> Vec<String> {
    match config {
        PipeConfig::Source(c) => vec![],
        PipeConfig::DerivedValues(c) => vec![c.pipe_id.clone()],
        PipeConfig::GroupAndReduce(c) => vec![c.pipe_id.clone()],
        PipeConfig::Filter(c) => vec![c.pipe_id.clone()],
        PipeConfig::Join(c) => vec![c.left_pipe_id.clone(), c.right_pipe_id.clone()],
        PipeConfig::Rename(c) => vec![c.pipe_id.clone()],
    }
}