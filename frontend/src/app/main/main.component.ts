import { Component, OnInit } from '@angular/core'
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
export class MainComponent implements OnInit {

    allNames = ['Alex', 'Beth', 'Carter', 'Diane', 'Eddy', 'Fiona', 'Gary', 'Hayley']
    allSubjects = ['English', 'Maths', 'Sport', 'Politics', 'Drama', 'Music', 'Chemistry', 'Physics', 'Biology', 'Geography', 'Computer Science']
    allSemesters = [1, 2, 3, 4, 5, 6, 7, 8]
    studentScores: any[] = []
    subjectMultipliers: any[] = []
    constructor() {
    }

    ngOnInit() {
        this.allNames.forEach((name, i) => {
            this.allSubjects.forEach((subject, j) => {
                this.studentScores.push({ name, subject, score: i + j })
            })
        })
        this.allSubjects.forEach((subject, i) => {
            this.allSemesters.forEach((semester, j) => {
                this.subjectMultipliers.push({ subject, semester, multiplier: 1 + (i * j / 100)})
            })
        })
    }

    onClick() {
        const pipeConfigs = {
            source1: { type: 'SourceCsv', path: '.', source_id: 'myFirstSource' },
            source2: { type: 'SourceCsv', path: '.', source_id: 'mySecondSource' },
            join1: { type: 'Join', left_pipe_id: 'source1', right_pipe_id: 'source2', on: ['name'] },
            studentScoresSource: { type: 'SourceCsv', path: '', source_id: 'studentScores' },
            subjectMultipliersSource: { type: 'SourceCsv', path: '', source_id: 'subjectMultipliers' },
            scoresJoinMultipliers: { type: 'Join', left_pipe_id: 'studentScoresSource', right_pipe_id: 'subjectMultipliersSource', on: ['subject'] },
            adjustedScores: { type: 'BinaryCalculation', pipe_id: 'scoresJoinMultipliers', new_column: 'adjustedScore', column_1: 'score', column_2: 'multiplier' },
        }
        const inputData = {
             myFirstSource: toDataTypeArrays(
                 [{ name: 'Andrew', score: 1.23 }, { name: 'Beth', score: undefined }, { name: 'Connor', score: 2.34 }],
                 { name: 'str', score: 'f64' },
             ),
            mySecondSource: toDataTypeArrays(
                [{ name: 'Andrew', grade: 5 }, { name: 'Beth', grade: 4 }, { name: 'David', grade: 3 }],
                { name: 'str', grade: 'i64' }
            ),
            studentScores: toDataTypeArrays(this.studentScores, { name: 'str', subject: 'str', score: 'f64' }),
            subjectMultipliers: toDataTypeArrays(this.subjectMultipliers, { subject: 'str', semester: 'i64', multiplier: 'f64' }),
        }
        const result = polarsPipes.run_data_pipeline(['adjustedScores'], inputData, pipeConfigs)
        console.log('RESULT IS', fromDataTypeArrays(result))
    }
}
