export function getSourcePipes(configs: PipeConfig[]): string[][]

export function runDataPipeline(pipe_ids: string[], input_data: {[k: string]: DataTable}, configs: {[k: string]: PipeConfig}): DataTable

export interface DataTable {
    f64: Map<string, (number | null)[]>,
    i64: Map<string, (number | null)[]>,
    str: Map<string, (number | null)[]>,
    datetime: Map<string, (number | null)[]>,
    bool: Map<string, (number | null)[]>,
}

export type PipeConfig = SourcePipeConfig
    | DerivedValuesPipeConfig
    | GroupAndReducePipeConfig
    | FilterPipeConfig
    | JoinPipeConfig
    | RenamePipeConfig

export interface SourcePipeConfig {
    type: 'Source',
    sourceId: string,
}

export interface DerivedValuesPipeConfig {
    type: 'DerivedValues',
    pipeId: string,
    calcs: DerivedValuesExpressionRoot[],
}

export interface DerivedValuesExpressionRoot {
    name: string,
    expression: DerivedValuesExpression,
}

export type DerivedValuesExpression = DerivedValuesOperation
    | DerivedValuesWindowAggExpression
    | DerivedValuesProperty
    | number

export interface DerivedValuesWindowAggExpression {
    operation: AggType,
    operand: DerivedValuesExpression,
    over: string[],
}

export interface DerivedValuesProperty {
    property: string
}

export interface DerivedValuesOperation {
    operation: DerivedValuesOperationType,
    operands: DerivedValuesExpression[],
}

export type DerivedValuesOperationType = 'Sum'
    | 'Subtract'
    | 'Multiply'
    | 'Divide'
    | 'Min'
    | 'Max'
    | 'Not'
    | 'And'
    | 'Or'
    | 'LessThan'
    | 'LessThanEq'
    | 'GreaterThan'
    | 'GreaterThanEq'
    | 'IfThenElse'

export interface GroupAndReducePipeConfig {
    type: 'GroupAndReduce',
    pipeId: string,
    groupBy: string[],
    aggs: AggConfig[],
}

export interface AggConfig {
    name: string,
    type: AggType,
    aggProperty: string
}

export type AggType = 'Sum'
    | 'Max'
    | 'Min'

export interface FilterPipeConfig {
    type: 'Filter',
    pipeId: string,
    filters: DerivedValuesExpression[],
}

export interface JoinPipeConfig {
    type: 'Join',
    leftPipeId: string,
    rightPipeId: string,
    on: string[],
    how: JoinPipeType,
}

export type JoinPipeType = 'Left'
    | 'Right'
    | 'Inner'
    | 'Outer'

export interface RenamePipeConfig {
    type: 'Rename',
    pipeId: string,
    properties: RenamePropertyConfig,
}

export interface RenamePropertyConfig {
    from: string,
    to: string,
}


