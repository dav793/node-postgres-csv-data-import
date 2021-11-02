const { Client } = require('pg');
const { Buffer } = require('buffer');
const { Observable, Subject, of, forkJoin, concat, timer, EMPTY } = require('rxjs');
const { switchMap, concatMap, reduce, map, tap, expand } = require('rxjs/operators');
const fs = require('fs');
const moment = require('moment');

const CHUNK_SIZE = 10;
const CSV_PATHS = [
    '/home/vol/dumps/TEST.txt'
];

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
        console.log(`reading chunk ${chunkNumber}`);
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

function processChunk(chunk) {
    return timer(1000).pipe(
        tap(() => {
            continueReading$.next();
        }),
        map(() => chunk)
    );
}

function streamCsv(path) {

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
        ).subscribe(() => {});
    });
}

streamCsv(CSV_PATHS[0]).pipe(
    concatMap(chunk => processChunk(chunk))
).subscribe({
    next: res => {
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
