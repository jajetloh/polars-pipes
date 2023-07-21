import { Component } from '@angular/core'
import * as polarsPipes from 'polars-pipes'
import { max } from "rxjs"

type Option<T> = (T | undefined)

interface DataFrame {
    f64: Option<Map<string, Option<number>[]>>,
    i64: Option<Map<string, Option<number>[]>>,
    str: Option<Map<string, Option<string>[]>>,
    datetime: Option<Map<string, Option<number>[]>>,
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
        const p = polarsPipes
        console.log(p.do_thing())
        const raw: DataFrame = p.do_thing()

        const columnIters: { name: string, iterRef: any[] }[] = []
        // Object.entries(raw)
        //     .filter(([k, v]) => v !== undefined)
        //     .map(([k, v]) => v.entries())
        //     .forEach((x: [string, Map<string, any>]) => {
        //     columnIters.push({ name: x[0], iterRef: x[1].values() })
        // })
        let maxRows = 0
        Object.entries(raw)
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
        console.log(records)
    }
}
