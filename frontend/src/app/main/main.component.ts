import { Component } from '@angular/core'
import * as polarsPipes from 'polars-pipes'

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
        console.log(p)
    }
}
