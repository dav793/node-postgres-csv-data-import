const { Client } = require('pg');
const { Buffer } = require('buffer');
const { Observable, Subject, of, forkJoin, concat, EMPTY } = require('rxjs');
const { switchMap, concatMap, reduce, map, tap, expand } = require('rxjs/operators');
const fs = require('fs');
const moment = require('moment');

const READ_LIMIT = 100000;
const CHUNK_SIZE = 100000;
const CSV_PATHS = [
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2021.csv',
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2020.csv',
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2019.csv',
    '/home/vol/dumps/Parking_Violations_Issued_-_Fiscal_Year_2018.csv'
];

const IDX_SUMMONS_NUMBER = 0;
const IDX_PLATE_ID = 1;
const IDX_PLATE_TYPE = 3;
const IDX_VIOLATION_CODE = 5;
const IDX_VEHICLE_MAKE = 7;
const IDX_VEHICLE_COLOR = 33;
const IDX_DESCRIPTION = 39;
const IDX_YEAR = 4;     // 35 o 4?

const continueReading$ = new Subject();

const delayUntil = (until$, timeout = 30000) => {
    return (obs) => {
        return new Observable(obsIn => {

            obs.subscribe({
                next: r => {

                    const timer = setTimeout(() => {
                        obsIn.next(r);
                        obsIn.complete();
                    }, timeout);

                    until$.subscribe(x => {
                        clearTimeout(timer);
                        obsIn.next(r);
                        obsIn.complete();
                    });

                },
                error: e => obsIn.error(e),
                complete: () => { }
            })

        });
    };
};

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

function getFileDescriptor(path) {
    return new Observable(obs => {
        fs.open(path, 'r', (status, fd) => {
            if (status)
                obs.error(status.message);
            else {
                obs.next(fd);
                obs.complete();
            }
        });
    });
}

function readChunk(chunkNumber, fileDescriptor) {
    return new Observable(obs => {
        // console.log(`reading chunk ${chunkNumber}`);
        const buffer = Buffer.alloc(CHUNK_SIZE);
        fs.read(fileDescriptor, buffer, 0, CHUNK_SIZE, chunkNumber * CHUNK_SIZE, (err, num) => {
            if (err)
                obs.error(err);
            else {
                obs.next(buffer.toString('utf8', 0, num));
                obs.complete();
            }
        });
    });
}

function streamFileContents(path) {

    return new Observable(obs => {
        getFileDescriptor(path).pipe(
            concatMap(fd => {

                const stats = fs.statSync(path);
                const fileSize = stats.size;
                const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);
                let chunkNumber = 0;

                return readChunk(0, fd).pipe(
                    tap(chunk => {
                        chunkNumber++;
                        obs.next(chunk);
                    }),
                    expand(() => {
                        if (chunkNumber < totalChunks) {
                            return readChunk(chunkNumber, fd).pipe(
                                delayUntil(continueReading$),
                                tap(chunk => {
                                    chunkNumber++;
                                    obs.next(chunk);
                                })
                            );
                        }
                        obs.complete();
                        return EMPTY;
                    })
                );

            })
        ).subscribe(() => { });
    });
}

//function streamCsv(path) {

//    return new Observable(obs => {
//        const stream = fs.createReadStream(path);
//        stream
//            .on('data', data => {

//                of(data).pipe(
//                    delayWhen(continueReading$)
//                ).subscribe({
//                    next: data => {
//                        obs.next(Buffer.from(data).toString('utf8'));
//                    }
//                });

//                // if (stream.bytesRead > READ_LIMIT)
//                //     stream.destroy();

//            })
//            .on('error', err => obs.error(err))
//            .on('end', () => console.log('El stream ha leido todos datos'))
//            .on('close', () => {
//                console.log(`El stream fue cerrado despues de ${stream.bytesRead} bytes leidos`);
//                obs.complete();
//            });
//    });
    
//}

function createLineObj(line) {

    const year = moment(line[IDX_YEAR], 'D/M/YYYY');

    return {
        SUMMONS_NUMBER: line[IDX_SUMMONS_NUMBER],
        PLATE_ID: line[IDX_PLATE_ID],
        PLATE_TYPE: line[IDX_PLATE_TYPE],
        VIOLATION_CODE: line[IDX_VIOLATION_CODE],
        VEHICLE_MAKE: line[IDX_VEHICLE_MAKE],
        VEHICLE_COLOR: line[IDX_VEHICLE_COLOR],
        DESCRIPTION: line[IDX_DESCRIPTION],
        YEAR: year.isValid() ? year.format('YYYY') : ''
    };
}

function processLines(lines, pgClient) {

    const tasks = lines.map(l => {

        return of(createLineObj(l)).pipe(
            concatMap(line => {

                return forkJoin([
                    getVehicleDescriptionByPlateID(line.PLATE_ID, pgClient),
                    getViolationByViolationCode(line.VIOLATION_CODE, pgClient),
                    getTicketBySummonsNumberAndYear(line.SUMMONS_NUMBER, line.YEAR, pgClient)
                ]).pipe(
                    concatMap(([vehicleDescription, violation, ticket]) => {
                        return forkJoin([
                            vehicleDescription ? of(false) : insertVehicleDescription(line, pgClient),
                            violation ? of(false) : insertViolation(line, pgClient),
                            ticket ? of(false) : insertTicket(line, pgClient)
                        ]);
                    })
                );

            })
        );
    });

    return concat(...tasks).pipe(
        reduce((acc, cur) => acc.concat(cur), []),
        tap(res => {
            console.log(`Se procesaron ${res.length} entradas...`);
            continueReading$.next();
        })
    );

}

function getVehicleDescriptionByPlateID(plateId, client) {
    return new Observable(obs => {
        client.query(`SELECT * FROM Vehicle_Description WHERE PlateID = '${plateId}'`, (err, res) => {
            if (err)
                obs.error(err);
            else {
                if (res.rowCount === 0)
                    obs.next(null);
                obs.next(res.rows[0]);
                obs.complete();
            }
        })
    });
}

function insertVehicleDescription(data, client) {
    return new Observable(obs => {
        client.query(`
            INSERT INTO Vehicle_Description(PlateID, VehicleColor, VehicleMake, PlateType)
            VALUES ('${data.PLATE_ID}', '${data.VEHICLE_COLOR}', '${data.VEHICLE_MAKE}', '${data.PLATE_TYPE}')
        `, (err, res) => {
            if (err)
                obs.error(err);
            else {
                if (res.rowCount === 0)
                    obs.next(false);
                obs.next(true);
                obs.complete();
            }
        })
    });
}

function getViolationByViolationCode(violationCode, client) {
    return new Observable(obs => {
        client.query(`SELECT * FROM Violation WHERE ViolationCode = '${violationCode}'`, (err, res) => {
            if (err)
                obs.error(err);
            else {
                if (res.rowCount === 0)
                    obs.next(null);
                obs.next(res.rows[0]);
                obs.complete();
            }
        })
    });
}

function insertViolation(data, client) {
    return new Observable(obs => {
        client.query(`
            INSERT INTO Violation(ViolationCode, Description)
            VALUES ('${data.VIOLATION_CODE}', '${data.DESCRIPTION}')
        `, (err, res) => {
            if (err)
                obs.error(err);
            else {
                if (res.rowCount === 0)
                    obs.next(false);
                obs.next(true);
                obs.complete();
            }
        })
    });
}

function getTicketBySummonsNumberAndYear(summonsNumber, year, client) {
    return new Observable(obs => {
        client.query(`SELECT * FROM Ticket WHERE SummonsNumber = '${summonsNumber}' AND Year = '${year}'`, (err, res) => {
            if (err)
                obs.error(err);
            else {
                if (res.rowCount === 0)
                    obs.next(null);
                obs.next(res.rows[0]);
                obs.complete();
            }
        })
    });
}

function insertTicket(data, client) {
    return new Observable(obs => {
        client.query(`
            INSERT INTO Ticket(SummonsNumber, PlateID, ViolationCode, Year)
            VALUES ('${data.SUMMONS_NUMBER}', '${data.PLATE_ID}', '${data.VIOLATION_CODE}', '${data.YEAR}')
        `, (err, res) => {
            if (err)
                obs.error(err);
            else {
                if (res.rowCount === 0)
                    obs.next(false);
                obs.next(true);
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

            return streamFileContents(CSV_PATHS[0]).pipe(
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
                concatMap(lines => processLines(lines, pgClient))
            );
        })
    )
    .subscribe({
        next: res => {
            // console.log(res);
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
