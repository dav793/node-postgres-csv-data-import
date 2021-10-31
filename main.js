const { Pool, Client } = require('pg');
const { Buffer } = require('buffer');
const { Observable, of } = require('rxjs');
const { switchMap, concatMap, reduce } = require('rxjs/operators');
const csv = require('csv-parser');
const fs = require('fs');

const READ_LIMIT = 100000;
const CSV_PATHS = [
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2021.csv'
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2020.csv'
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2019.csv'
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2018.csv'
];

function connectDB() {
    return new Observable(obs => {
        const client = new Client();
        client.connect((err, res) => {
            if (err) {
                console.error(err);
                obs.error(err);
            }
            else {
                console.log(`Conexion con BD establecida.`);
                obs.next(res);
            }
        });
    });
}

function parseCsv(path) {
    return new Observable(obs => {
        const results = [];
        fs.createReadStream(path)
            .pipe(csv())
            .on('data', data => results.push(data))
            .on('error', err => obs.error(err))
            .on('end', () => obs.next(results));
    });
}

function testStream(path) {

    return new Observable(obs => {
        const stream = fs.createReadStream(path);
        stream
            .on('data', data => {

                obs.next( Buffer.from(data).toString('utf8') );

                if (stream.bytesRead > READ_LIMIT)
                    stream.destroy();

            })
            .on('error', err => obs.error(err))
            .on('end', () => console.log('Stream reached end of data'))
            .on('close', () => {
                console.log(`Stream was closed after ${stream.bytesRead} bytes read`);
                obs.complete();
            });
    });
    
}

connectDB()
    .pipe(
        switchMap(conn => {

            return testStream(CSV_PATHS[0]).pipe(
                reduce((acc, cur) => acc + cur, ''),
                concatMap(str => {

                    let lines = str.split('\n');
                    const lastLine = lines.splice(lines.length - 1, 1)[0];
                    lines = str.split('\n').map(line => line.split(','));

                    console.log(`line count: ${lines.length}`);

                    console.log(`first line:`);
                    console.log(lines[0]);

                    console.log(`second line:`);
                    console.log(lines[1]);

                    console.log(`last line:`);
                    console.log(lastLine.split(','));

                    return of('');

                })
            );
            // return parseCsv(CSV_PATHS[0]);
    
        })
    )
    .subscribe({
        next: res => {
            // console.log(res);
            console.log(`Stream completed`);
            console.log(res);
        },
        error: err => {
            console.error(err);
            process.exit(1);
        },
        complete: () => {
            console.log(`Ejecucion terminada con exito.`);
            process.exit(0);
        }
    });
