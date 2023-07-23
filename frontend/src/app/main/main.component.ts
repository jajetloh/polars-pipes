import { Component } from '@angular/core'
import * as polarsPipes from 'polars-pipes'

type Option<T> = (T | undefined)

interface DataTypeArrays {
    f64: Map<string, Option<number>[]>,
    i64: Map<string, Option<number>[]>,
    str: Map<string, Option<string>[]>,
    datetime: Map<string, Option<number>[]>,
}

function toDataTypeArrays(inputData: any[], columnSchema: {[k: string]: keyof DataTypeArrays}): DataTypeArrays {
    // TODO: Handle case when some records are missing keys...
    const initialMap: DataTypeArrays = { f64: new Map(), i64: new Map(), str: new Map(), datetime: new Map() }
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
function fromDataTypeArrays(input: DataTypeArrays): any[] {
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

enum ColumnType {
    F64 = 'f64',
    I64 = 'i64',
    Str = 'str',
    Datetime = 'datetime',
}

@Component({
    selector: 'app-main',
    templateUrl: './main.component.html',
    styleUrls: ['./main.component.css']
})
export class MainComponent {
    constructor() {
    }

    onClick() {
        const pipeConfigs = {
            source1: { type: 'SourceCsv', path: '.', source_id: 'myFirstSource' },
            source2: { type: 'SourceCsv', path: '.', source_id: 'mySecondSource' },
            join1: { type: 'Join', left_pipe_id: 'source1', right_pipe_id: 'source2', on: ['name'] },
        }
        const inputData = {
             myFirstSource: toDataTypeArrays(
                 [{ name: 'Andrew', score: 1.23 }, { name: 'Beth', score: undefined }, { name: 'Connor', score: 2.34 }],
                 { name: 'str', score: 'f64' },
             ),
            mySecondSource: toDataTypeArrays(
                [{ name: 'Andrew', grade: 5 }, { name: 'Beth', grade: 4 }, { name: 'David', grade: 3 }],
                { name: 'str', grade: 'i64' }
            )
        }
        const result = polarsPipes.run_data_pipeline(['join1'], inputData, pipeConfigs)
        console.log('RESULT IS', fromDataTypeArrays(result))
    }
}
