const { Client } = require('pg');
const { Buffer } = require('buffer');
const { Observable, of } = require('rxjs');
const { switchMap, concatMap, reduce, map } = require('rxjs/operators');
const fs = require('fs');

const READ_LIMIT = 100000;
const CSV_PATHS = [
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2021.csv',
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2020.csv',
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2019.csv',
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
                obs.next(client);
                obs.complete();
            }
        });
    });
}

function streamCsv(path) {

    return new Observable(obs => {
        const stream = fs.createReadStream(path);
        stream
            .on('data', data => {

                obs.next( Buffer.from(data).toString('utf8') );

                if (stream.bytesRead > READ_LIMIT)
                    stream.destroy();

            })
            .on('error', err => obs.error(err))
            .on('end', () => console.log('El stream ha leido todos datos'))
            .on('close', () => {
                console.log(`El stream fue cerrado despues de ${stream.bytesRead} bytes leidos`);
                obs.complete();
            });
    });
    
}

function processLines(lines, headers, pgClient) {

    return consulta(pgClient);

    // return of(`se procesaron ${lines.length} lineas`);

}

function consulta(client) {

    return new Observable(obs => {
        client.query('SELECT NOW()', (err, res) => {
            if (err)
                obs.error(err);
            else {
                obs.next(res);
                obs.complete();
            }
        })
    });

}

connectDB()
    .pipe(
        switchMap(pgClient => {

            let headers = [];
            let lastLine = '';

            return streamCsv(CSV_PATHS[0]).pipe(
                map(chunk => {

                    // parsear lineas + extraer headers + pasar sobrante al siguiente chunk
                    if (lastLine && lastLine !== '')
                        chunk = lastLine + chunk;
                    let lines = chunk.split('\n');

                    if (headers.length === 0)
                        headers = lines.splice(0, 1)[0].split(',');

                    if (!lines[lines.length - 1].endsWith('\n'))
                        lastLine = lines.splice(lines.length - 1, 1)[0];

                    lines = lines.map(line => line.split(','));

                    return lines;

                }),
                concatMap(lines => processLines(lines, headers, pgClient))
            );
        })
    )
    .subscribe({
        next: res => {
            // console.log(res);
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
