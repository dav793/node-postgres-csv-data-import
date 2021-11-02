const { Observable, of, timer } = require('rxjs');

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
                complete: () => {}
            })

        });
    };
}

of('hola').pipe(
    delayUntil(timer(2000))
).subscribe({
    next: r => console.log(r),
    error: e => console.log(e),
    complete: () => console.log('terminado')
});
