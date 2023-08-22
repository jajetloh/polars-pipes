import { Component, OnInit } from '@angular/core'
import { DataTable, getSourcePipes, PipeConfig, runDataPipeline } from 'polars-pipes'

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

    allNames = ['Alex', 'Beth', 'Carter', 'Diane', 'Eddy', 'Fiona', 'Gary', 'Hayley', 'Ingrid', 'James', 'Karl', 'Leah', 'Mahwish', 'Niamh', 'Otto', 'Pyotr', 'Quill', 'Rue', 'Soleil', 'Tammy', 'Ulrich', 'Ville', 'Wahlberg', 'Xena', 'Yumi', 'Zet']
    allSubjects = ['English', 'Maths', 'Sport', 'Politics', 'Drama', 'Music', 'Chemistry', 'Physics', 'Biology', 'Geography', 'Computer Science', 'Law', 'Ancient History', 'Modern History', 'Economics', 'Literature', 'Film', 'Media', 'Health Sciences', 'Medicine', 'Business', 'Management', 'Leadership']
    allSemesters = Object.keys([...new Array(1000)]).map(x => Number(x))
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
        const pipeConfigs: { [k: string]: PipeConfig } = {
            source1: { type: 'Source', sourceId: 'myFirstSource' },
            source2: { type: 'Source', sourceId: 'mySecondSource' },
            join1: { type: 'Join', leftPipeId: 'source1', rightPipeId: 'source2', on: ['name'], how: 'Left' },
            studentScoresSource: { type: 'Source',  sourceId: 'studentScores' },
            subjectMultipliersSource: { type: 'Source', sourceId: 'subjectMultipliers' },
            scoresJoinMultipliers: { type: 'Join', leftPipeId: 'studentScoresSource', rightPipeId: 'subjectMultipliersSource', on: ['subject'], how: 'Left' },
            adjustedScores: { type: 'DerivedValues', pipeId: 'scoresJoinMultipliers', calcs: [{ name: 'adjustedScore', expression: { operation: 'Sum', operands: [{ property: 'score' }, { property: 'multiplier' }]} }]},
            // adjustedScores: { type: 'BinaryCalculation', pipeId: 'scoresJoinMultipliers', newColumn: 'adjustedScore', column1: 'score', column2: 'multiplier' },
            adjustedScoresSum: {
                type: 'GroupAndReduce',
                pipeId: 'adjustedScores',
                groupBy: ['name', 'subject'],
                aggs: [
                    { name: 'total', type: 'Sum', aggProperty: 'adjustedScore' }
                ]
            },
            adjustedScoresDerivedValues: {
                type: 'DerivedValues',
                pipeId: 'adjustedScores',
                calcs: [
                    {
                        name: 'hello',
                        expression: {
                            operation: 'Subtract',
                            operands: [
                                100,
                                {
                                    operation: 'Sum',
                                    operands: [
                                        { property: 'adjustedScore' },
                                        { property: 'semester' },
                                    ]
                                }
                            ]
                        }
                    },
                    {
                        name: 'wowzers',
                        expression: {
                            operation: 'IfThenElse',
                            operands: [
                                { operation: 'LessThan', operands: [{ property: 'hello' }, 80] },
                                0,
                                { operation: 'LessThan', operands: [{ property: 'hello' }, 90] },
                                1000,
                                2000,
                            ]
                        }
                    },
                    {
                        name: 'boundedScore',
                        expression: { operation: 'Min', operands: [{ property: 'adjustedScore' }, 200] },
                    }
                ]
            },
            hoorayFiltered: {
                type: 'Filter',
                pipeId: 'adjustedScoresDerivedValues',
                filters: [
                    {
                        operation: 'GreaterThan',
                        operands: [{ property: 'semester' }, 900]
                    },
                    {
                        operation: 'LessThan',
                        operands: [{ property: 'semester' }, 910]
                    }
                ]
            },
            overPipe: {
                type: 'DerivedValues',
                pipeId: 'hoorayFiltered',
                calcs: [{
                    name: 'maxAdjustedScoreOver',
                    expression: {
                        operation: 'Max',
                        operand: { property: 'adjustedScore' },
                        over: ['semester'] /// NEXT: Figure out why it errors when 2+ over arguments
                        // over: ['semester', 'name'] /// NEXT: Figure out why it errors when 2+ over arguments
                    }
                }]
            }
        }
        const inputData: {[k: string]: DataTable} = {
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
        const result = runDataPipeline(['overPipe'], inputData, pipeConfigs)
        // const result = polarsPipes.run_data_pipeline(['adjustedScoresDerivedValues'], inputData, pipeConfigs)
        // const result = polarsPipes.run_data_pipeline(['adjustedScores'], inputData, pipeConfigs)
        console.log('RESULT IS', fromDataTypeArrays(result))
        const sourcePipes = getSourcePipes(Object.values(pipeConfigs))
        console.log(sourcePipes)
    }
}
