import { PipeConfig } from "polars-pipes/polars_pipes.original"
import { DataTable } from "polars-pipes"

export function toDataTypeArrays(inputData: any[], columnSchema: {[k: string]: keyof DataTable}): DataTable {
    // TODO: Handle case when some records are missing keys...
    const initialMap: DataTable = { f64: new Map(), i64: new Map(), str: new Map(), datetime: new Map(), bool: new Map() }
    Object.entries(columnSchema).forEach(([k,v]) => {
        initialMap[v]!.set(k, [])
    })
    return inputData.reduce((acc, obj) => {
        Object.entries(obj).forEach(([k, v]) => {
            acc[columnSchema[k]].get(k).push(v)
        })
        return acc
    }, initialMap)
}
export function fromDataTypeArrays(input: DataTable): any[] {
    const columnIters: { name: string, iterRef: any[] }[] = []
    let maxRows = 0
    Object.entries(input)
        .filter(([k,v]) => v !== undefined)
        .forEach(([k,v]) => {
            for (let [k, v2] of v) {
                columnIters.push({ name: k, iterRef: v2 })
                maxRows = Math.max(maxRows, v2.length)
            }
        })
    const records: any[] = []
    for (let i = 0; i < maxRows; i++) {
        records.push(columnIters.reduce((acc, { name, iterRef }) => {
            acc[name] = iterRef[i]
            return acc
        }, {} as any))
    }
    return records
}


type TableSchema = {[j: string]: keyof DataTable}

const INPUT_DATA: {[k: string]: [TableSchema, any[]]} = {
    sourceId1: [
        { year: "i64", month: "i64", revenue: "f64", cost: "f64" },
        [
            { year: 2021, month: 1, revenue: 100, cost: 50 },
            { year: 2021, month: 2, revenue: 200, cost: 100 },
            { year: 2022, month: 2, revenue: 300, cost: 150 },
            { year: 2022, month: 3, revenue: 400, cost: 200 },
            { year: 2023, month: 3, revenue: 500, cost: 250 },
            { year: 2023, month: 4, revenue: 600, cost: 300 },
        ],
    ],
    sourceId2: [
        { year: 'i64', taxRate: 'f64' },
        [
            { year: 2020, taxRate: 0.05 },
            { year: 2021, taxRate: 0.15 },
            { year: 2022, taxRate: 0.25 },
        ],
    ],
}

const INPUT_DATA_TABLES: [string, DataTable][] = Object.entries(INPUT_DATA).map(([key, [schema, data]]) => {
    return [key, toDataTypeArrays(data, schema)]
})
const INPUT_TABLE_MAP = new Map<string, DataTable>(INPUT_DATA_TABLES)

const PIPE_CONFIGS: {[k: string]: PipeConfig} = {
    source1: {
        type: 'Source',
        sourceId: 'sourceId1',
    },
    source2: {
        type: 'Source',
        sourceId: 'sourceId2',
    },
    joinLeft1: {
        type: 'Join',
        leftPipeId: 'source1',
        rightPipeId: 'source2',
        on: ['year'],
        how: 'Left',
    },
    joinInner1: {
        type: 'Join',
        leftPipeId: 'source1',
        rightPipeId: 'source2',
        on: ['year'],
        how: 'Inner',
    },
    joinRight1: {
        type: 'Join',
        leftPipeId: 'source1',
        rightPipeId: 'source2',
        on: ['year'],
        how: 'Right',
    },
    joinOuter1: {
        type: 'Join',
        leftPipeId: 'source1',
        rightPipeId: 'source2',
        on: ['year'],
        how: 'Outer',
    },
    addPipe1: {
        type: 'DerivedValues',
        pipeId: 'source1',
        calcs: [{
            name: 'newValue',
            expression: {
                operation: 'Sum',
                operands: [{ property: 'revenue' }, { property: 'cost' }, 1]
            }
        }]
    },
    multiplyPipe1: {
        type: 'DerivedValues',
        pipeId: 'source1',
        calcs: [{
            name: 'newValue',
            expression: {
                operation: 'Multiply',
                operands: [{ property: 'revenue' }, { property: 'cost' }, -1]
            }
        }]
    }
}
const PIPE_CONFIGS_MAP = new Map<string, PipeConfig>(Object.entries(PIPE_CONFIGS))

let runDataPipeline: any = null // Set in beforeAll as wasm must be loaded asynchronously - otherwise module won't load
describe('Data Pipe Testing', () => {
    beforeAll((done) => {
        import('polars-pipes').then(module => {
            runDataPipeline = module.runDataPipeline
            done()
        })
    })

    describe('Join Pipe', () => {
        describe('Left Join', () => {
            it('should return correct result when valid', () => {
                const tableResult = runDataPipeline(['joinLeft1'], INPUT_TABLE_MAP, PIPE_CONFIGS_MAP)
                const arrayResult = fromDataTypeArrays(tableResult)

                const expectedResult = [
                    { year: 2021, month: 1, revenue: 100, cost: 50, taxRate: 0.15 },
                    { year: 2021, month: 2, revenue: 200, cost: 100, taxRate: 0.15 },
                    { year: 2022, month: 2, revenue: 300, cost: 150, taxRate: 0.25 },
                    { year: 2022, month: 3, revenue: 400, cost: 200, taxRate: 0.25 },
                    { year: 2023, month: 3, revenue: 500, cost: 250, taxRate: undefined },
                    { year: 2023, month: 4, revenue: 600, cost: 300, taxRate: undefined },
                ]
                expect(arrayResult).toEqual(expectedResult)
            })
        })
        describe('Inner Join', () => {
            it('should return correct result when valid', () => {
                const tableResult = runDataPipeline(['joinInner1'], INPUT_TABLE_MAP, PIPE_CONFIGS_MAP)
                const arrayResult = fromDataTypeArrays(tableResult)

                const expectedResult = [
                    { year: 2021, month: 1, revenue: 100, cost: 50, taxRate: 0.15 },
                    { year: 2021, month: 2, revenue: 200, cost: 100, taxRate: 0.15 },
                    { year: 2022, month: 2, revenue: 300, cost: 150, taxRate: 0.25 },
                    { year: 2022, month: 3, revenue: 400, cost: 200, taxRate: 0.25 },
                ]
                expect(arrayResult).toEqual(expectedResult)
            })
        })
        describe('Right Join', () => {
            it('should return correct result when valid', () => {
                const tableResult = runDataPipeline(['joinRight1'], INPUT_TABLE_MAP, PIPE_CONFIGS_MAP)
                const arrayResult = fromDataTypeArrays(tableResult)

                const expectedResult = [
                    { year: 2020, month: undefined, revenue: undefined, cost: undefined, taxRate: 0.05 },
                    { year: 2021, month: 1, revenue: 100, cost: 50, taxRate: 0.15 },
                    { year: 2021, month: 2, revenue: 200, cost: 100, taxRate: 0.15 },
                    { year: 2022, month: 2, revenue: 300, cost: 150, taxRate: 0.25 },
                    { year: 2022, month: 3, revenue: 400, cost: 200, taxRate: 0.25 },
                ]
                expect(arrayResult).toEqual(expectedResult)
            })
        })
        describe('Outer Join', () => {
            it('should return correct result when valid', () => {
                const tableResult = runDataPipeline(['joinOuter1'], INPUT_TABLE_MAP, PIPE_CONFIGS_MAP)
                const arrayResult = fromDataTypeArrays(tableResult)

                const expectedResult = [
                    { year: 2021, month: 1, revenue: 100, cost: 50, taxRate: 0.15 },
                    { year: 2021, month: 2, revenue: 200, cost: 100, taxRate: 0.15 },
                    { year: 2022, month: 2, revenue: 300, cost: 150, taxRate: 0.25 },
                    { year: 2022, month: 3, revenue: 400, cost: 200, taxRate: 0.25 },
                    { year: 2023, month: 3, revenue: 500, cost: 250, taxRate: undefined },
                    { year: 2023, month: 4, revenue: 600, cost: 300, taxRate: undefined },
                    { year: 2020, month: undefined, revenue: undefined, cost: undefined, taxRate: 0.05 },
                ]
                expect(arrayResult).toEqual(expectedResult)
            })
        })
    })

    describe('Derived Values Pipe', () => {
        describe('Addition', () => {
            it('should add correctly', () => {
                const tableResult = runDataPipeline(['addPipe1'], INPUT_TABLE_MAP, PIPE_CONFIGS_MAP)
                const arrayResult = fromDataTypeArrays(tableResult)

                const expectedResult = [
                    { year: 2021, month: 1, revenue: 100, cost: 50, newValue: 151 },
                    { year: 2021, month: 2, revenue: 200, cost: 100, newValue: 301 },
                    { year: 2022, month: 2, revenue: 300, cost: 150, newValue: 451 },
                    { year: 2022, month: 3, revenue: 400, cost: 200, newValue: 601 },
                    { year: 2023, month: 3, revenue: 500, cost: 250, newValue: 751 },
                    { year: 2023, month: 4, revenue: 600, cost: 300, newValue: 901 },
                ]
                expect(arrayResult).toEqual(expectedResult)
            })
        })

        describe('Multiplication', () => {
            it('should multiply correctly', () => {
                const tableResult = runDataPipeline(['multiplyPipe1'], INPUT_TABLE_MAP, PIPE_CONFIGS_MAP)
                const arrayResult = fromDataTypeArrays(tableResult)

                const expectedResult = [
                    { year: 2021, month: 1, revenue: 100, cost: 50, newValue: -5_000 },
                    { year: 2021, month: 2, revenue: 200, cost: 100, newValue: -20_000 },
                    { year: 2022, month: 2, revenue: 300, cost: 150, newValue: -45_000 },
                    { year: 2022, month: 3, revenue: 400, cost: 200, newValue: -80_000 },
                    { year: 2023, month: 3, revenue: 500, cost: 250, newValue: -125_000 },
                    { year: 2023, month: 4, revenue: 600, cost: 300, newValue: -180_000 },
                ]
                expect(arrayResult).toEqual(expectedResult)
            })
        })

        xdescribe('Subtract', () => {
            xit('should subtract correctly', () => {
                // TODO
            })
        })
    })
})
