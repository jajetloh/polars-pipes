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

    onClick3() {
        // const pipeConfigs = {
        //     source1: ['SourceCsv', { path: '.', source_id: 'myFirstSource' }],
        //     source2: ['SourceCsv', { path: '.', source_id: 'myFirstSource' }],
        // }
        const csvConfig = {
            type: 'SourceCsv',
            path: 'f',
            source_id: 'g',
        }
        const testObject = {
            source1: [1,2, 'woo', 'SourceCsv', csvConfig],
            source2: [2,3, 'yow', 'SourceCsv', csvConfig] ,
        }
        // const testObject = new Map<string, any>()
        // testObject.set('source1', 'hello there')
        // testObject.set('source2', 'www there')

        const pipeConfigs = new Map<string, any>()
        pipeConfigs.set('source1', ['SourceCsv', { path: '.', source_id: 'myFirstSource' }])
        pipeConfigs.set('source2', ['SourceCsv', { path: '..', source_id: 'mySecondSource' }])
        const inputData = {
             myFirstSource: [{ name: 'Andrew', score: 1.23 }, { name: 'Beth', score: 2.34 }, { name: 'Connor', score: undefined }]
        }
        polarsPipes.run_data_pipeline('source1', inputData, pipeConfigs, testObject)
    }

    onClick2() {
        const p = polarsPipes

        const columnSchema = { a: ColumnType.I64, b: ColumnType.Str }
        const inputData: any[] = [
            {a: 1, b: '123'},
            {a: 2, b: '1243'},
            {a: 3, b: '12fgg_3'},
        ]
        const x = toDataTypeArrays(inputData, columnSchema)
        p.do_thing_2(x)
    }
    onClick() {
        const p = polarsPipes

        // const columnSchema = { a: ColumnType.I64, b: ColumnType.Str }
        // const inputData: any[] = [
        //     {a: 1, b: '123'},
        //     {a: 2, b: '1243'},
        //     {a: 3, b: '12fgg_3'},
        // ]
        // const x = toDataTypeArrays(inputData, columnSchema)
        // const r = p.do_thing_2(x)
        // console.log(r)

        // console.log(p.do_thing(in))
        // const raw: DataTypeArrays = p.do_thing()
        // console.log(fromDataTypeArrays(raw))

        // const columnIters: { name: string, iterRef: any[] }[] = []
        // // Object.entries(raw)
        // //     .filter(([k, v]) => v !== undefined)
        // //     .map(([k, v]) => v.entries())
        // //     .forEach((x: [string, Map<string, any>]) => {
        // //     columnIters.push({ name: x[0], iterRef: x[1].values() })
        // // })
        // let maxRows = 0
        // Object.entries(raw)
        //     .filter(([k,v]) => v !== undefined)
        //     .forEach(([k,v]) => {
        //         for (let [k, v2] of v) {
        //             columnIters.push({ name: k, iterRef: v2 })
        //             maxRows = Math.max(maxRows, v2.length)
        //         }
        //     })
        // const records: any[] = []
        // for (let i = 0; i < maxRows; i++) {
        //     records.push(columnIters.reduce((acc, { name, iterRef }) => {
        //         acc[name] = iterRef[i]
        //         return acc
        //     }, {} as any))
        // }
        // console.log(records)
    }
}
