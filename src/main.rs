use std::time::*;
use std::sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}, Mutex, RwLock};
use std::path::*;
use std::io::{self, prelude::*};
use std::convert::TryInto;
use std::net::SocketAddr;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};
use hashbrown::{HashMap, HashSet};
use chrono::prelude::*;
use rand::prelude::*;
use pretty_toa::ThousandsSep;
use tokio::runtime::Runtime;
use uuid::Uuid;
use itertools::Itertools;
use chrono_tz::US::Pacific;
use structopt::StructOpt;
use influx_writer::{InfluxWriter, measure};

const API_REQUEST: &str = include_str!("../templates/api-request.tera");

/// tools for testing fitbod api server
///
/// note: this program does not handle *any* errors. that is "on purpose," because this is supposed
/// to test the api server, so problems should cause a screeching halt to the program. 
///
/// however, as this is being completed for the purpose of evaluation, please note that this style of
/// code is (constant use of `.unwrap()`) is not idiomatic and would be bad practice for code
/// that is intended to be robust and reliable.
///
#[derive(StructOpt)]
enum Opt {
    /// insert user.csv example data provided by fitbot to postgres. this does
    /// *not* load the data in workout.csv.
    SetupExampleUsers {
        #[structopt(short = "f", default_value = "var/user.csv")]
        input_path: PathBuf,
        /// where to save csv with generated user data (user_id, private keys) corresponding
        /// to user.csv emails
        #[structopt(short, long, default_value = "var/example-users.csv")]
        output_path: PathBuf,
    },

    /// inserts workout.csv example data provided by fitbot in random order, and checks
    /// correctness of /api/v1/workouts/list output following each insert
    ///
    /// warning: running this twice in a row will not work (and is not expected to work).
    ///
    /// in that case, the program will generate new `workout_id`s for the same set of workouts,
    /// prompting errors (ultimately unique key violations) that are not handled.
    ///
    /// that should never happen in practice, based on a couple assumptions of the api server code:
    ///
    /// 1) regarding the (user_id, start_time) unique key on workouts, the assumption is
    ///    there will not be multiple workouts with identical start times for a given
    ///    user. to get identical start times, user would have to start two or more workouts
    ///    at same exact moment on multiple devices
    /// 2) api server assumes the same workout will not be inserted more than once with multiple
    ///    `workout_id` values. my rationale is, the mobile app will save a fixed workout_id locally
    ///    and that id consistently when sending writes to api server. note: it is perfectly fine
    ///    for the app to write the same workout twice, so long as it uses the same `workout_id`
    ///    in both instances. duplicate writes with consistent `workout_ids` is handled by api
    ///    server just fine.
    ///
    InsertWorkoutsTest {
        /// path of csv file provided by fitbot with example workout data
        #[structopt(short = "w", long, default_value = "var/workout.csv")]
        workouts_csv_path: PathBuf,

        #[structopt(short = "u", long, default_value = "var/example-users.csv")]
        users_csv_path: PathBuf,

        /// number of threads that will simultaneously be inserting data via api
        #[structopt(short = "j", long, default_value = "4")]
        n_threads: usize,

        /// api server address
        #[structopt(short, long, default_value = "127.0.0.1:3030")]
        connect: SocketAddr,
    },


    /// generate random users, insert into db, and save csv with private keys
    ///
    /// note: after running this command, restart the api server to enable it to
    /// cache the newly inserted keys on startup
    SetupRandomUsers {
        /// where to save csv with generated user data (user_id, private keys) corresponding
        /// to the randomly generated users
        #[structopt(short, long, default_value = "var/random-users.csv")]
        output_path: PathBuf,

        /// how many users to insert
        #[structopt(short, long, default_value = "1000000")]
        n_users: usize,

        /// how many user rows to insert in each tx
        #[structopt(long, default_value = "8192")]
        chunk_size: usize,
    },

    /// try our best to make the api server melt
    ///
    /// note: this requires random users to have been generated, inserted, and saved
    /// to csv (see setup-random-users subcommand).
    ///
    /// this will perform a barage of requests meant to simulate real-world usage.
    ///
    /// to begin with, workouts table will be truncated (unless in --read-only mode).
    ///
    /// users will be selected randomly, and a job will be chosen: 80% probability read,
    /// 20% probability write.
    ///
    /// - read job will fetch last 25 workouts
    ///
    /// - write job will insert 15 workouts, which will include 10 previously inserted
    ///   workouts (except on first write) and 5 new workouts.
    ///
    /// the workouts in --workouts-csv-path will be used as templates. a given randomly
    /// generated user will be assigned one of the templates from that file and his writes
    /// will be identical, other than having a different user_id.
    ///
    /// state of each user (meaning how many workouts have been written) will be tracked,
    /// and on read jobs, the results will be chacked against the expected state.
    ///
    /// among universe of all users, each user will be given an engagement score, which is
    /// randomly sampled from normal distribution.
    ///
    /// stress test will progress in rounds of `--batch-size`. for each round, `--batch-size`
    /// users will be chosen randomly, using their engagement scores as the weight or likilihood
    /// of being chosen. this is to mimic a user base with differing rates of participation.
    /// then (read or write) jobs will be assigned for each of the sampled users.
    ///
    /// a user may be sampled more than once in a given batch. to prevent race conditions
    /// between threads in terms of the read jobs validating the results, the state of which
    /// workouts has been inserted for a given user is behind a mutex (rw lock actually).
    ///
    /// in --read-only mode, everything is the same except that all jobs are read jobs, and there
    /// is no validating the results against what workout rows are known to have been written.
    ///
    /// tip: to create even moar stress run one machine in normal mode and multiple additional machines
    /// in --read-only mode.
    ///
    /// the --n-threads param controls the level of concurrency, meaning how many requests this
    /// will generate simultaneously. each thread proceeds synchronosly. a manager thread is in
    /// charge of assigning jobs to the worker threads, and keeps track of the state of each user.
    ///
    /// program will continue until ctrl-c (kill signal) prompts exit. at that time, there will be
    /// a final check between the state of users on db vs. what we expect based on writes
    /// executed against db server. this check will be skipped in --read-only mode.
    ///
    StressTest {
        /// path of csv file provided by fitbot with example workout data
        #[structopt(short = "w", long, default_value = "var/workout.csv")]
        workouts_csv_path: PathBuf,

        #[structopt(short = "u", long, default_value = "var/random-users.csv")]
        users_csv_path: PathBuf,

        /// number of threads that will simultaneously be inserting data via api
        #[structopt(short = "j", long, default_value = "4")]
        n_threads: usize,

        /// number of users to assign jobs for in between shuffling
        #[structopt(long, default_value = "1024")]
        batch_size: usize,

        /// api server address
        #[structopt(short, long, default_value = "127.0.0.1:3030")]
        connect: SocketAddr,

        /// don't insert any data, only read it.
        ///
        /// in other words, no requests to /api/v1/workouts/new, only requests
        /// to /api/v1/workouts/list
        #[structopt(long)]
        read_only: bool,
    },

    /// print example http request for /api/v1/workouts/list endpoint to stdout
    ListWorkoutsRequest {
        #[structopt(short = "u", long, default_value = "var/example-users.csv")]
        users_csv_path: PathBuf,

        /// defaults to a user id randomly chosen from the file
        #[structopt(long)]
        user_id: Option<Uuid>,

        /// pick user by email instead of user_id
        #[structopt(long, conflicts_with = "user_id")]
        email: Option<String>,

        /// filter results by end (YYYY-MM-DD)
        #[structopt(long)]
        start: Option<NaiveDate>,

        /// filter results by end (YYYY-MM-DD)
        #[structopt(long)]
        end: Option<NaiveDate>,

        /// specify limit to request
        #[structopt(long)]
        limit: Option<usize>,

        /// output curl command instead of http request text
        #[structopt(long)]
        curl: bool,
    },

    /// print example http request for /api/v1/workouts/new endpoint to stdout
    NewWorkoutsRequest {
        #[structopt(short = "u", long, default_value = "var/example-users.csv")]
        users_csv_path: PathBuf,

        /// defaults to a user id randomly chosen from the file
        #[structopt(long)]
        user_id: Option<Uuid>,

        /// pick user by email instead of user_id
        #[structopt(long, conflicts_with = "user_id")]
        email: Option<String>,

        /// date of workout
        date: NaiveDate,

        /// workout duration in minutes
        duration: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExampleUsersCsvRow {
    #[serde(alias = "Email")]
    email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExampleWorkoutsCsvRow {
    #[serde(alias = "Email Address")]
    email: String,

    #[serde(alias = "Workout Date")]
    dt: NaiveDate,

    #[serde(alias = "Workout Duration")]
    duration_minutes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserPrivate {
    pub user_id: Uuid,
    pub key: fitbod::auth::PrivateKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserPrivateEncoded {
    pub user_id: Uuid,
    pub email: String,
    pub private_key: String,
    pub public_key: String,
}

struct UserState {
    user_id: Uuid,
    key: fitbod::auth::PrivateKey,
    /// note: the `workout_id` values in these is wrong for this user. the `inserted_workout_ids`
    /// contains a set of correct `workout_id` values for this user.
    workouts: Arc<Vec<fitbod::Workout>>,
    workout_ids: Vec<Uuid>,
    inserted: Arc<RwLock<HashSet<Uuid>>>,
    pos: usize,
}

enum StressTestJob {
    Read {
        user_id: Uuid,
        key: fitbod::auth::PrivateKey,
        inserted: Arc<RwLock<HashSet<Uuid>>>,
    },

    Write {
        user_id: Uuid,
        key: fitbod::auth::PrivateKey,
        workouts: Vec<fitbod::Workout>,
        inserted: Arc<RwLock<HashSet<Uuid>>>,
    },

    Exit,
}

fn load_csv<T, P>(input_path: P) -> Vec<T>
    where T: for<'de> Deserialize<'de>,
          P: AsRef<Path>
{
    assert!(input_path.as_ref().exists(), "path does not exist: {}", input_path.as_ref().display());
    let bytes = std::fs::read(input_path).unwrap();
    let mut rdr = csv::Reader::from_reader(&bytes[..]);
    let mut out = Vec::new();
    for row in rdr.deserialize() {
        let row = row.unwrap();
        out.push(row);
    }
    out
}

fn load_example_users<P: AsRef<Path>>(input_path: P) -> Vec<ExampleUsersCsvRow> {
    load_csv(input_path)
}

fn load_example_workouts<P: AsRef<Path>>(input_path: P) -> Vec<ExampleWorkoutsCsvRow> {
    load_csv(input_path)
}

fn get_workout(user_id: Uuid, dt: NaiveDate, duration_minutes: u32) -> fitbod::Workout {
    let workout_id = Uuid::new_v4();
    let start_time: DateTime<Utc> = Pacific.from_local_date(&dt)
        .unwrap()
        .and_hms(6, 30, 0)
        .with_timezone(&Utc);
    let end_time = start_time + chrono::Duration::minutes(duration_minutes as i64);
    fitbod::Workout { user_id, workout_id, start_time, end_time }
}

fn as_priv_key<T: AsRef<[u8]>>(bytes: T) -> fitbod::auth::PrivateKey {
    bytes.as_ref().try_into().unwrap()
}

// impl<'a> From<&'a UserPrivate> for UserPrivateEncoded {
//     fn from(user: &'a UserPrivate) -> Self {
//         let private_key = base64::encode(&user.key[..]);
//         Self {
//             user_id: user.user_id,
//             private_key,
//         }
//     }
// }

fn generate_users_from_emails<T: ToString>(emails: &[T]) -> (Vec<UserPrivate>, Vec<fitbod::User>) {
    let mut out_priv = Vec::with_capacity(emails.len());
    let mut out_pub = Vec::with_capacity(emails.len());
    for email in emails {
        let email = email.to_string();
        let user_id = Uuid::new_v4();
        let (priv_key, pub_key) = fitbod::auth::gen_keypair();
        out_priv.push(UserPrivate { user_id, key: priv_key });
        out_pub.push(fitbod::User { user_id, email, key: pub_key, created: Utc::now() });
    }
    (out_priv, out_pub)
}

fn generate_random_emails(n: usize) -> Vec<String> {
    const ASCII: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut ascii: Vec<&'static str> = (0..ASCII.len()).map(|i| &ASCII[i..(i+1)]).collect();
    let mut rng = thread_rng();
    (&mut ascii[..]).shuffle(&mut rng);
    ascii.into_iter()
        .combinations(8)
        .map(|letters| format!("{}@fitbod.me", letters.join("")))
        .take(n)
        .collect()
}

fn setup_example_users(input_path: &Path, output_path: &Path) {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let users = load_example_users(input_path);
    let emails: Vec<String> = users.into_iter().map(|ExampleUsersCsvRow { email }| email).collect();
    let (user_priv, user_pub) = generate_users_from_emails(&emails);
    let n = emails.len();
    assert_eq!(user_priv.len(), n);
    assert_eq!(user_pub.len(), n);
    let rt  = Runtime::new().unwrap();
    rt.block_on(async {
        let db = fitbod::db::DataBase::new(&db_url).await.unwrap();
        db.insert_users(&user_pub[..]).await.unwrap();
    });
    let user_priv_encoded: Vec<UserPrivateEncoded> = (0..n).map(|i| {
        assert_eq!(&emails[i], &user_pub[i].email);
        UserPrivateEncoded {
            user_id: user_pub[i].user_id,
            email: emails[i].clone(),
            private_key: base64::encode(user_priv[i].key),
            public_key: base64::encode(user_pub[i].key),
        }
    }).collect();
    let mut file = std::fs::File::create(output_path).unwrap();
    writeln!(&mut file, "user_id,email,public_key,private_key").unwrap();
    assert_eq!(emails.len(), n);
    for i in 0..n {
        let pub_key = base64::encode(&user_pub[i].key[..]);
        assert_eq!(user_priv_encoded[i].user_id, user_pub[i].user_id);
        writeln!(&mut file, "{},{},{},{}", user_pub[i].user_id, user_pub[i].email, pub_key, user_priv_encoded[i].private_key).unwrap();
    }
}

fn load_private_keys<P: AsRef<Path>>(input_path: P) -> Vec<UserPrivateEncoded> {
    load_csv(input_path)
}

fn list_request(
    users_csv_path: &Path,
    user_id: Option<Uuid>,
    email: Option<String>,
    start: Option<NaiveDate>,
    end: Option<NaiveDate>,
    limit: Option<usize>,
    curl: bool,
) {
    let mut keys = load_private_keys(users_csv_path);
    let user_id = user_id.unwrap_or_else(|| {
        if let Some(email) = email {
            keys.iter().find(|x| x.email == email).unwrap().user_id
        } else {
            let mut rng = thread_rng();
            keys.shuffle(&mut rng);
            keys[0].user_id
        }
    });
    let encoded_key = keys.iter().find(|x| x.user_id == user_id)
        .expect("key not found in users_csv_path")
        .private_key
        .as_str();
    let key = as_priv_key(base64::decode(encoded_key).unwrap());
    let req = fitbod::api::ListWorkoutsRequest {
        user_id,
        start: start.map(|dt| Utc.from_local_date(&dt).unwrap().and_hms(0, 0, 0)),
        end: end.map(|dt| Utc.from_local_date(&dt).unwrap().and_hms(0, 0, 0)),
        limit,
    };
    let req_json = serde_json::to_string(&req).unwrap();
    let timestamp = Utc::now().timestamp();
    let sig = fitbod::auth::sign_request(timestamp, &req_json, &key);
    let timestamp_str = timestamp.to_string();

    if curl {
        let addr = "127.0.0.1:3030";
        let path = "api/v1/workouts/list";
        println!("curl -H 'x-fitbod-access-signature: {sig}' -H 'x-fitbod-access-timestamp: {ts}' --data '{d}' {a}/{p}",
            sig = sig,
            ts = timestamp_str,
            d = req_json,
            a = addr,
            p = path,
        );
    } else {
        let mut tera = tera::Tera::default();
        tera.add_raw_template("api-request", API_REQUEST).unwrap();
        let mut ctx = tera::Context::new();
        ctx.insert("path", "/api/v1/workouts/list");
        ctx.insert("body", &req_json);
        ctx.insert("sig", &sig);
        ctx.insert("timestamp", &timestamp_str);

        let http_req = tera.render("api-request", &ctx).unwrap(); //.replace("\n", "\r\n");
        print!("{}", http_req);
    }
}

fn new_workouts_request(users_csv_path: &Path, user_id: Option<Uuid>, email: Option<String>, dt: NaiveDate, duration: u32) {
    let mut keys = load_private_keys(users_csv_path);
    let user_id = user_id.unwrap_or_else(|| {
        if let Some(email) = email {
            keys.iter().find(|x| x.email == email).unwrap().user_id
        } else {
            let mut rng = thread_rng();
            keys.shuffle(&mut rng);
            keys[0].user_id
        }
    });
    let encoded_key = keys.iter().find(|x| x.user_id == user_id)
        .expect("key not found in users_csv_path")
        .private_key
        .as_str();
    let key = as_priv_key(base64::decode(encoded_key).unwrap());
    let start_time: DateTime<Utc> = Pacific.from_local_date(&dt)
        .unwrap()
        .and_hms(6, 30, 0)
        .with_timezone(&Utc);
    let end_time = start_time + chrono::Duration::minutes(duration as i64);
    let workout = fitbod::Workout {
        user_id,
        workout_id: Uuid::new_v4(),
        start_time,
        end_time,
    };
    let req = fitbod::api::NewWorkoutsRequest {
        user_id,
        items: vec![workout],
    };
    let req_json = serde_json::to_string(&req).unwrap();
    let timestamp = Utc::now().timestamp();
    let sig = fitbod::auth::sign_request(timestamp, &req_json, &key);
    let timestamp_str = timestamp.to_string();

    let mut tera = tera::Tera::default();
    tera.add_raw_template("api-request", API_REQUEST).unwrap();
    let mut ctx = tera::Context::new();
    ctx.insert("path", "/api/v1/workouts/new");
    ctx.insert("body", &req_json);
    ctx.insert("sig", &sig);
    ctx.insert("timestamp", &timestamp_str);

    let http_req = tera.render("api-request", &ctx).unwrap(); //.replace("\n", "\r\n");
    print!("{}", http_req);
}

/// returns request body
fn api_request<T>(addr: &SocketAddr, path: &str, req: &T, key: &fitbod::auth::PrivateKey, tera: &mut tera::Tera, influx: &InfluxWriter) -> Option<Vec<u8>>
    where T: Serialize
{
    let req_json = serde_json::to_string(&req).unwrap();
    let timestamp = Utc::now().timestamp();
    let sig = fitbod::auth::sign_request(timestamp, &req_json, key);
    let timestamp_str = timestamp.to_string();

    let mut ctx = tera::Context::new();
    ctx.insert("path", path);
    ctx.insert("body", &req_json);
    ctx.insert("sig", &sig);
    ctx.insert("timestamp", &timestamp_str);
    let http_req_str = tera.render("api-request", &ctx).unwrap();
    let http_req = http_req_str.as_bytes();

    let req_start = Instant::now();
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream.set_nonblocking(true).expect("send nonblocking");
    stream.set_nodelay(true).expect("send nodelay");
    let mut n_bytes_written = 0;
    while n_bytes_written < http_req.len() {
        match stream.write(&http_req[n_bytes_written..]) {
            Ok(n) => n_bytes_written += n,
            Err(e) if would_block(&e) => {}
            Err(e) => return None,
        }
    }

    let mut buf = [0u8; 16384];
    let mut n_bytes_read = 0;

    loop {
        match stream.read(&mut buf[n_bytes_read..]) {
            Ok(n) =>  n_bytes_read += n,
            Err(e) if would_block(&e) => {},
            Err(e) => return None,
        }

        let mut headers = Vec::<thhp::HeaderField>::with_capacity(8);
        if let Ok(thhp::Complete((ref resp, body_offset))) = thhp::Response::parse(&buf[..n_bytes_read], &mut headers) {
            let req_done = Instant::now();
            let took = req_done.saturating_duration_since(req_start).as_nanos() as i64;
            let endpoint = path;
            let status = resp.status.to_string();

            measure!(influx, api_req, t(endpoint), t(status), i(took), tm(Utc::now().timestamp_nanos()));

            if ! (resp.status == 200 || resp.status == 204) {
                eprintln!("***\nREQUEST:\n\n{}\n\nRESPONSE:\n\n{}\nbody.len()={}",
                    http_req_str,
                    std::str::from_utf8(&buf[..n_bytes_read]).unwrap(),
                    req_json.len(),
                );
                return None
            }
            assert!(body_offset <= n_bytes_read);
            return Some((&buf[body_offset..n_bytes_read]).to_vec())
        } // TODO: add timeout here
    }
}

fn stress_test(
    workouts_csv_path: &Path,
    users_csv_path: &Path,
    n_threads: usize,
    batch_size: usize,
    addr: SocketAddr,
    read_only: bool,
) {
    let begin = Instant::now();
    println!("beginning - make sure to restart the api server prior to this to re-cache user keys");

    let mut rng = thread_rng();

    let mut users = load_private_keys(users_csv_path);
    let n = users.len();
    println!("loaded private keys from --users-csv-path");
    users.shuffle(&mut rng);

    let example_workouts = load_example_workouts(workouts_csv_path);
    println!("loaded example workouts");

    let mut workouts_by_user: HashMap<String, Vec<fitbod::Workout>> = Default::default();
    for ExampleWorkoutsCsvRow { email, dt, duration_minutes } in example_workouts {
        let user_id = Uuid::nil();
        let workout_id = Uuid::nil();
        let start_time: DateTime<Utc> = Pacific.from_local_date(&dt)
            .unwrap()
            .and_hms(6, 30, 0)
            .with_timezone(&Utc);
        let end_time = start_time + chrono::Duration::minutes(duration_minutes as i64);
        workouts_by_user.entry(email)
            .or_default()
            .push(fitbod::Workout { user_id, workout_id, start_time, end_time });
    }


    let workout_templates: Vec<Arc<Vec<fitbod::Workout>>> = workouts_by_user.drain()
        .map(|(_, mut workouts)| {
            workouts.sort_unstable_by_key(|x| x.start_time);
            Arc::new(workouts)
        }).collect();


    let mut user_states: Vec<UserState> = users.into_par_iter()
        .enumerate()
        .map(|(i, UserPrivateEncoded { user_id, private_key, .. })| {
            let key = as_priv_key(base64::decode(&private_key).unwrap());
            let workouts = workout_templates[i % workout_templates.len()].clone();
            let workout_ids = (0..workouts.len()).map(|_| Uuid::new_v4()).collect();
            let inserted = Default::default();
            UserState {
                user_id,
                key,
                workouts,
                workout_ids,
                inserted,
                pos: 0,
            }
        }).collect();
    assert_eq!(user_states.len(), n);
    println!("assembled initial user states");

    let user_engagement_scores: Vec<f32> = rand::distributions::Standard.sample_iter(&mut rng).take(n).collect();
    let ix: Vec<usize> = (0..n).collect(); // sample indices instead of directly to access &mut user_states[i]
    println!("generated user engagement scores");

    let mut txs = Vec::new();
    let mut threads = Vec::new();

    let influx = InfluxWriter::new("localhost", "fitbod");
    let n_inserted = Arc::new(AtomicUsize::new(0));
    let mut tera = tera::Tera::default();
    tera.add_raw_template("api-request", API_REQUEST).unwrap();

    for i in 0..n_threads {
        let (tx, rx) = crossbeam_channel::bounded(8);
        txs.push(tx);
        let mut tera = tera.clone();
        let influx = influx.clone();
        let addr = addr.clone();
        let n_inserted = n_inserted.clone();
        threads.push(std::thread::spawn(move || {
            'event: loop {
                match rx.recv() {
                    Ok(StressTestJob::Exit) => break 'event,

                    Ok(StressTestJob::Read { user_id, key, inserted }) if ! read_only => {
                        let write_lock = inserted.write().unwrap();
                        let req = fitbod::api::ListWorkoutsRequest::from(user_id);
                        let resp = api_request(&addr, "/api/v1/workouts/list", &req, &key, &mut tera, &influx).unwrap();
                        let resp: fitbod::api::ListWorkoutsResponse = serde_json::from_slice(&resp[..]).unwrap();
                        let resp_wids: HashSet<Uuid> = resp.items.iter().map(|x| x.workout_id).collect();
                        assert_eq!(write_lock.symmetric_difference(&resp_wids).count(), 0,
                            "read check failed for user id {}\n{:#?}\n{:#?}", user_id, resp_wids, &*write_lock,
                        );
                        drop(write_lock);
                    }

                    Ok(StressTestJob::Read { user_id, key, .. }) if read_only => {
                        let req = fitbod::api::ListWorkoutsRequest::from(user_id);
                        let _resp = api_request(&addr, "/api/v1/workouts/list", &req, &key, &mut tera, &influx).unwrap();
                    }

                    Ok(StressTestJob::Write { user_id, key, workouts, inserted }) => {
                        assert!( ! read_only );
                        let req = fitbod::api::NewWorkoutsRequest {
                            user_id,
                            items: workouts,
                        };
                        let mut write_lock = inserted.write().unwrap();
                        let _resp = api_request(&addr, "/api/v1/workouts/new", &req, &key, &mut tera, &influx).unwrap();
                        let n_before = write_lock.len();
                        write_lock.extend(req.items.iter().map(|x| x.workout_id));
                        let n_after = write_lock.len();
                        drop(write_lock);
                        n_inserted.fetch_add(n_after.saturating_sub(n_before), Ordering::Relaxed);
                    }

                    _ => todo!(),
                }
            }
        }));
    }

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGINT, Arc::clone(&term)).unwrap();
    signal_hook::flag::register(signal_hook::SIGTERM, Arc::clone(&term)).unwrap();
    signal_hook::flag::register(signal_hook::SIGQUIT, Arc::clone(&term)).unwrap();

    let uniform = rand::distributions::Uniform::new(0.0f64, 1.0f64);
    let mut next_thread = 0;
    let mut n_pending_inserts = 0;

    let mut last_disp = Instant::now();
    let mut n_jobs_sent = 0;
    let mut n_read = 0;
    let mut n_write = 0;

    loop {
        let loop_time = Instant::now();
        for &i in ix.choose_multiple_weighted(&mut rng, batch_size, |&i| { user_engagement_scores[i] }).unwrap() {
            let state = &mut user_states[i];
            let is_write = ! read_only && state.pos < state.workouts.len() && uniform.sample(&mut rng) > 0.80;
            let job = match is_write {
                true => {
                    n_write += 1;
                    let workouts: Vec<fitbod::Workout> = state.workouts.iter()
                        .enumerate()
                        .skip(state.pos.saturating_sub(10))
                        .take(15)
                        .map(|(i, w)| {
                            let mut w = w.clone();
                            w.user_id = state.user_id;
                            w.workout_id = state.workout_ids[i];
                            w
                        }).collect();
                    let n_new = {
                        let read_lock = state.inserted.read().unwrap();
                        workouts.iter().filter(|x| ! read_lock.contains(&x.workout_id)).count()
                    };
                    state.pos += n_new;
                    n_pending_inserts += n_new;
                    StressTestJob::Write {
                        user_id: state.user_id,
                        workouts,
                        key: state.key.clone(),
                        inserted: state.inserted.clone(),
                    }
                }

                false => {
                    n_read += 1;
                    StressTestJob::Read {
                        user_id: state.user_id,
                        key: state.key.clone(),
                        inserted: state.inserted.clone(),
                    }
                }
            };
            // send to first available thread - each has small queue size so can fill up
            let mut job = Some(job);
            while job.is_some() {
                job = {
                    let job = job.take().unwrap();
                    match txs[next_thread % n_threads].try_send(job) {
                        Ok(_) => None,
                        Err(crossbeam_channel::TrySendError::Full(j)) => Some(j),
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => panic!("thread rx disconnected"),
                    }
                };
                next_thread += 1;
            }
            n_jobs_sent += 1;
        }

        let loop_end = Instant::now();
        if loop_end.saturating_duration_since(last_disp) > Duration::from_secs(1) {
            let elapsed = loop_end.saturating_duration_since(last_disp);
            println!("{} jobs ({} read / {} write) in last {:?} - {} inserted (incl pending) vs. {} inserted (confirmed)",
                n_jobs_sent.thousands_sep(),
                n_read.thousands_sep(),
                n_write.thousands_sep(),
                elapsed,
                n_pending_inserts.thousands_sep(),
                n_inserted.load(Ordering::Relaxed).thousands_sep(),
            );
            last_disp = loop_end;
            n_jobs_sent = 0;
            n_read = 0;
            n_write = 0;
        }

        if term.load(Ordering::Relaxed) { break }
    }
    println!("exit signal received");

    for tx in txs.iter() {
        tx.send(StressTestJob::Exit).unwrap();
    }

    for join_handle in threads {
        let _ = join_handle.join().unwrap();
    }
    println!("joined threads");

    if ! read_only {
        let failed_verifications: Vec<Uuid> = user_states.par_iter().filter(|x| x.pos > 0).map_init(
            || {
                let mut tera = tera::Tera::default();
                tera.add_raw_template("api-request", API_REQUEST).unwrap();
                tera
            },

            |tera, UserState { user_id, inserted, key, .. }| {
                let req = fitbod::api::ListWorkoutsRequest::from(*user_id);
                let resp = api_request(&addr, "/api/v1/workouts/list", &req, &key, tera, &influx).unwrap();
                let resp: fitbod::api::ListWorkoutsResponse = serde_json::from_slice(&resp[..]).unwrap();
                let resp_wids: HashSet<Uuid> = resp.items.iter().map(|x| x.workout_id).collect();
                let expected_wids = inserted.read().unwrap();
                if expected_wids.symmetric_difference(&resp_wids).count() > 0 {
                    Some(*user_id)
                } else {
                    None
                }
            }
        ).filter_map(|x| x).collect();
        println!("finished checking user list results");

        if ! failed_verifications.is_empty() {
            dbg!(&failed_verifications);
            panic!("final check failed for {} users", failed_verifications.len());
        }
    }
    println!("all done in {:?}", Instant::now().saturating_duration_since(begin));
}

fn insert_workouts_test(
    workouts_csv_path: &Path,
    users_csv_path: &Path,
    n_threads: usize,
    addr: SocketAddr,
) {
    let mut keys = load_private_keys(users_csv_path);
    let email_uid: HashMap<String, Uuid> = keys.iter().map(|x| (x.email.clone(), x.user_id)).collect();
    let uid_key: HashMap<Uuid, fitbod::auth::PrivateKey> = keys.iter()
        .map(|x| (x.user_id, as_priv_key(base64::decode(&x.private_key).unwrap())))
        .collect();
    let mut example_workouts = load_example_workouts(workouts_csv_path);
    let mut rng = thread_rng();
    example_workouts.shuffle(&mut rng);
    //let mut workouts: HashMap<Uuid, Vec<fitbod::Workout>> = Default::default();
    let mut workouts: Vec<fitbod::Workout> = Default::default();
    for ExampleWorkoutsCsvRow { email, dt, duration_minutes } in example_workouts {
        let user_id = email_uid[&email];
        let start_time: DateTime<Utc> = Pacific.from_local_date(&dt)
            .unwrap()
            .and_hms(6, 30, 0)
            .with_timezone(&Utc);
        let end_time = start_time + chrono::Duration::minutes(duration_minutes as i64);
        //workouts.entry(user_id)
        //    .or_default()
        //    .push(fitbod::Workout { user_id, workout_id: Uuid::new_v4(), start_time, end_time });
        workouts.push(fitbod::Workout { user_id, workout_id: Uuid::new_v4(), start_time, end_time });
    }

    let uid_wid: HashMap<Uuid, Arc<Mutex<HashSet<Uuid>>>> = email_uid.values()
        .map(|&uid| (uid, Default::default()))
        .collect();
    let uid_wid = Arc::new(uid_wid);

    let mut thread_jobs: Vec<Vec<fitbod::Workout>> = (0..n_threads).map(|_| Vec::new()).collect();
    for (i, workout) in workouts.into_iter().enumerate() {
        thread_jobs[i % n_threads].push(workout);
    }

    let influx = InfluxWriter::new("localhost", "fitbod");

    let threads: Vec<std::thread::JoinHandle<()>> = (0..n_threads).map(|i| {
        let mut jobs = Vec::new();
        std::mem::swap(&mut jobs, &mut thread_jobs[i]);
        let uid_wid = Arc::clone(&uid_wid);
        let uid_key = uid_key.clone();
        let mut tera = tera::Tera::default();
        tera.add_raw_template("api-request", API_REQUEST).unwrap();
        let influx = influx.clone();
        std::thread::spawn(move || {
            while let Some(workout) = jobs.pop() {
                let user_id = workout.user_id;
                let key = &uid_key[&user_id];
                let req = fitbod::api::NewWorkoutsRequest {
                    user_id,
                    items: vec![workout.clone()],
                };

                let mut write_lock = uid_wid[&workout.user_id].lock().unwrap();

                let _resp = api_request(&addr, "/api/v1/workouts/new", &req, key, &mut tera, &influx).unwrap();

                assert!( write_lock.insert(workout.workout_id) ); // assert! is verifying that workout_id did not exist in set

                let local_copy: HashSet<_> = (&*write_lock).clone();

                // now check results of /api/v1/workouts/list

                let req = fitbod::api::ListWorkoutsRequest::from(user_id);
                let resp = api_request(&addr, "/api/v1/workouts/list", &req, key, &mut tera, &influx).unwrap();

                drop(write_lock);

                let resp_str = std::str::from_utf8(&resp[..]).unwrap();
                let resp: fitbod::api::ListWorkoutsResponse = serde_json::from_str(resp_str.trim()).map_err(|e| {
                    eprintln!("parsing response failed: {}\n\n***begin***{}***end***\nresp_str.trim().len()={}",
                        e, resp_str.trim(), resp_str.trim().len(),
                    );
                    e
                }).unwrap();
                let resp_wids: HashSet<Uuid> = resp.items.iter().map(|x| x.workout_id).collect();

                assert_eq!( resp_wids.symmetric_difference(&local_copy).count(), 0,
                    "ERROR!\n\nresp:\n{:#?}\\nnwrite_lock:\n{:#?}\n\nworkout:\n{:#?}", resp, local_copy, workout,
                );
            }
        })
    }).collect();

    for join_handle in threads {
        let res = join_handle.join().unwrap();
    }
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn setup_random_users(output_path: &Path, n_users: usize, chunk_size: usize) {
    let setup_start = Instant::now();
    let db_url = std::env::var("DATABASE_URL").unwrap();
    assert_ne!(&db_url[..], "");
    let emails = generate_random_emails(n_users);
    let n = emails.len();
    println!("generated {} random email addresses", emails.len().thousands_sep());
    let n_cores = num_cpus::get_physical();
    let keypair_chunks: Vec<(_, _)> = emails.par_iter()
        .chunks(n / (n_cores * 2))
        .map(|email_chunk| generate_users_from_emails(&email_chunk))
        .collect();
    let mut user_priv = Vec::with_capacity(n);
    let mut user_pub = Vec::with_capacity(n);
    for (user_priv_chunk, user_pub_chunk) in keypair_chunks {
        user_priv.extend(user_priv_chunk.into_iter());
        user_pub.extend(user_pub_chunk.into_iter());
    }
    println!("generated {} priv/public key pairs", user_priv.len().thousands_sep());
    assert_eq!(user_priv.len(), n);
    assert_eq!(user_pub.len(), n);
    let rt  = Runtime::new().unwrap();
    rt.block_on(async {
        let db = fitbod::db::DataBase::new(&db_url).await.unwrap();
        println!("connected to db");
        sqlx::query("truncate users cascade").execute(db.pool()).await.unwrap();
        println!("truncated users table");
        let mut n_inserted = 0;
        for chunk in user_pub.chunks(chunk_size) {
            db.insert_users(chunk).await.unwrap();
            println!("inserted users[{}..{}]", n_inserted, n_inserted + chunk.len());
            n_inserted += chunk.len();
        }
        println!("finished inserting {} user rows", n_inserted.thousands_sep());
    });

    println!("writing csv output file");
    let mut file = std::fs::File::create(output_path).unwrap();
    writeln!(&mut file, "user_id,email,public_key,private_key").unwrap();
    let user_priv_encoded: Vec<UserPrivateEncoded> = (0..n).map(|i| {
        assert_eq!(&emails[i], &user_pub[i].email);
        UserPrivateEncoded {
            user_id: user_pub[i].user_id,
            email: emails[i].clone(),
            private_key: base64::encode(user_priv[i].key),
            public_key: base64::encode(user_pub[i].key),
        }
    }).collect();
    assert_eq!(user_priv_encoded.len(), n);
    for i in 0..n {
        let pub_key = base64::encode(&user_pub[i].key[..]);
        assert_eq!(user_priv_encoded[i].user_id, user_pub[i].user_id);
        writeln!(&mut file, "{},{},{},{}", user_pub[i].user_id, user_pub[i].email, pub_key, user_priv_encoded[i].private_key).unwrap();
    }
    println!("finished writing csv output file");
    rt.shutdown_timeout(Duration::from_secs(30));
    println!("dropped rt");
    println!("all done in {:?}", Instant::now().saturating_duration_since(setup_start));
}

fn main() {
    dotenv::dotenv().unwrap();
    pretty_env_logger::init();
    match Opt::from_args() {
        Opt::SetupExampleUsers { input_path, output_path } => {
            assert!(input_path.exists(), "path does not exist: {}", input_path.display());
            setup_example_users(&input_path, &output_path);
        }

        Opt::InsertWorkoutsTest { workouts_csv_path, users_csv_path, n_threads, connect } => {
            assert!(workouts_csv_path.exists(), "path does not exist: {}", workouts_csv_path.display());
            assert!(users_csv_path.exists(), "path does not exist: {}", users_csv_path.display());
            insert_workouts_test(&workouts_csv_path, &users_csv_path, n_threads, connect);
        }

        Opt::ListWorkoutsRequest { users_csv_path, user_id, start, end, limit, email, curl } => {
            assert!(users_csv_path.exists(), "path does not exist: {}", users_csv_path.display());
            list_request(&users_csv_path, user_id, email, start, end, limit, curl);
        }

        Opt::NewWorkoutsRequest { users_csv_path, user_id, email, date, duration } => {
            assert!(users_csv_path.exists(), "path does not exist: {}", users_csv_path.display());
            new_workouts_request(&users_csv_path, user_id, email, date, duration);
        }

        Opt::SetupRandomUsers { output_path, n_users, chunk_size } => {
            setup_random_users(&output_path, n_users, chunk_size);
        }

        Opt::StressTest {
            workouts_csv_path, users_csv_path, n_threads, connect,
            batch_size, read_only,
        } => {
            stress_test(
                &workouts_csv_path, &users_csv_path, n_threads, batch_size,
                connect, read_only,
            );
        }
    }
}
