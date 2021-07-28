# fitbod-test

repo for correction and performance test harness for `fitbod-server`.

```console
$ cargo build --bin fitbod-test --release
```

## `--help`

```console
$ ./target/release/fitbod-test -h
fitbod-test 0.1.0
Jonathan Strong <jonathan.strong@gmail.com>
tools for testing fitbod api server

note: this program does not handle *any* errors. that is "on purpose," because this is supposed to test the api server,
so problems should cause a screeching halt to the program.

however, as this is being completed for the purpose of evaluation, please note that this style of code is (constant use
of `.unwrap()`) is not idiomatic and would be bad practice for code that is intended to be robust and reliable.

USAGE:
    fitbod-test <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    help                     Prints this message or the help of the given subcommand(s)
    insert-workouts-test     inserts workout.csv example data provided by fitbot in random order, and checks
                             correctness of /api/v1/workouts/list output following each insert
    list-workouts-request    print example http request for /api/v1/workouts/list endpoint to stdout
    new-workouts-request     print example http request for /api/v1/workouts/new endpoint to stdout
    setup-example-users      insert user.csv example data provided by fitbot to postgres. this does *not* load the
                             data in workout.csv
    setup-random-users       generate random users, insert into db, and save csv with private keys
    stress-test              try our best to make the api server melt
```

## `stress-test` subcommand

```console
$ ./target/release/fitbod-test stress-test -h
fitbod-test-stress-test 0.1.0
try our best to make the api server melt

note: this requires random users to have been generated, inserted, and saved to csv (see setup-random-users subcommand).

this will perform a barage of requests meant to simulate real-world usage.

to begin with, workouts table will be truncated (unless in --read-only mode).

users will be selected randomly, and a job will be chosen: 80% probability read, 20% probability write.

- read job will fetch last 25 workouts

- write job will insert 15 workouts, which will include 10 previously inserted workouts (except on first write) and 5
new workouts.

the workouts in --workouts-csv-path will be used as templates. a given randomly generated user will be assigned one of
the templates from that file and his writes will be identical, other than having a different user_id.

state of each user (meaning how many workouts have been written) will be tracked, and on read jobs, the results will be
chacked against the expected state.

among universe of all users, each user will be given an engagement score, which is randomly sampled from normal
distribution.

stress test will progress in rounds of `--batch-size`. for each round, `--batch-size` users will be chosen randomly,
using their engagement scores as the weight or likilihood of being chosen. this is to mimic a user base with differing
rates of participation. then (read or write) jobs will be assigned for each of the sampled users.

a user may be sampled more than once in a given batch. to prevent race conditions between threads in terms of the read
jobs validating the results, the state of which workouts has been inserted for a given user is behind a mutex (rw lock
actually).

in --read-only mode, everything is the same except that all jobs are read jobs, and there is no validating the results
against what workout rows are known to have been written.

tip: to create even moar stress run one machine in normal mode and multiple additional machines in --read-only mode.

the --n-threads param controls the level of concurrency, meaning how many requests this will generate simultaneously.
each thread proceeds synchronosly. a manager thread is in charge of assigning jobs to the worker threads, and keeps
track of the state of each user.

program will continue until ctrl-c (kill signal) prompts exit. at that time, there will be a final check between the
state of users on db vs. what we expect based on writes executed against db server. this check will be skipped in
--read-only mode.

USAGE:
    fitbod-test stress-test [FLAGS] [OPTIONS]

FLAGS:
    -h, --help         Prints help information
        --read-only    don't insert any data, only read it
    -V, --version      Prints version information

OPTIONS:
        --batch-size <batch-size>
            number of users to assign jobs for in between shuffling [default: 1024]

    -c, --connect <connect>                        api server address [default: 127.0.0.1:3030]
    -j, --n-threads <n-threads>
            number of threads that will simultaneously be inserting data via api [default: 4]

    -u, --users-csv-path <users-csv-path>           [default: var/random-users.csv]
    -w, --workouts-csv-path <workouts-csv-path>
            path of csv file provided by fitbot with example workout data [default: var/workout.csv]

```
