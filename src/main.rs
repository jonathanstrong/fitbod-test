use std::time::*;
use std::sync::{Arc, Mutex};
use std::path::*;
use std::io::{self, prelude::*};
use std::convert::TryInto;
use std::net::SocketAddr;
use serde::{Serialize, Deserialize};
use hashbrown::{HashMap, HashSet};
use chrono::prelude::*;
use rand::prelude::*;
use tokio::runtime::Runtime;
use uuid::Uuid;
use itertools::Itertools;
use chrono_tz::US::Pacific;
use structopt::StructOpt;
use influx_writer::{InfluxWriter, measure};

const API_REQUEST: &str = include_str!("../templates/api-request.tera");

#[derive(StructOpt)]
enum Opt {
    /// insert user.csv example data provided by fitbot to postgres. this does
    /// *not* load the data in workout.csv.
    SetupExampleUsers {
        #[structopt(short = "f", default_value = "var/user.csv")]
        input_path: PathBuf,
        /// where to save csv with generated user data (user_id, private keys) corresponding
        /// to user.csv emails
        output_path: PathBuf,
    },

    /// inserts workout.csv example data provided by fitbot in random order, and checks
    /// correctness of /api/v1/workouts/list output following each insert
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

    /// print example http request for /api/v1/workouts/list endpoint to stdout
    ListRequest {
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

fn list_request(users_csv_path: &Path, user_id: Option<Uuid>, email: Option<String>, start: Option<NaiveDate>, end: Option<NaiveDate>, limit: Option<usize>) {
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
    let req_json = serde_json::to_string_pretty(&req).unwrap();
    let timestamp = Utc::now().timestamp();
    let sig = fitbod::auth::sign_request(timestamp, &req_json, &key);
    let timestamp_str = timestamp.to_string();

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
    let req_json = serde_json::to_string_pretty(&req).unwrap();
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
        //    .push(fitbod:::Workout { user_id, workout_id: Uuid::new_v4(), start_time, end_time });
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
                let key = uid_key[&user_id];
                let req = fitbod::api::NewWorkoutsRequest {
                    user_id,
                    items: vec![workout.clone()],
                };
                let req_json = serde_json::to_string(&req).unwrap();
                let timestamp = Utc::now().timestamp();
                let sig = fitbod::auth::sign_request(timestamp, &req_json, &key);
                let timestamp_str = timestamp.to_string();

                let mut ctx = tera::Context::new();
                ctx.insert("path", "/api/v1/workouts/new");
                ctx.insert("body", &req_json);
                ctx.insert("sig", &sig);
                ctx.insert("timestamp", &timestamp_str);
                let http_req_str = tera.render("api-request", &ctx).unwrap();
                let http_req = http_req_str.as_bytes();

                let mut write_lock = uid_wid[&workout.user_id].lock().unwrap();

                let insert_start = Instant::now();
                let mut stream = std::net::TcpStream::connect(&addr).unwrap();
                stream.set_nonblocking(true).expect("send nonblocking");
                stream.set_nodelay(true).expect("send nodelay");
                let mut n_bytes_written = 0;
                while n_bytes_written < http_req.len() {
                    match stream.write(&http_req[n_bytes_written..]) {
                        Ok(n) => n_bytes_written += n,
                        Err(e) if would_block(&e) => {}
                        Err(e) => panic!("stream.write failed: {:?}", e),
                    }
                }

                let mut buf = [0u8; 1024];
                let mut n_bytes_read = 0;

                'insert_resp: loop {
                    match stream.read(&mut buf[n_bytes_read..]) {
                        Ok(n) =>  n_bytes_read += n,
                        Err(e) if would_block(&e) => {},
                        Err(e) => panic!("stream read failed: {:?}", e),
                    }

                    let mut headers = Vec::<thhp::HeaderField>::with_capacity(16);
                    if let Ok(thhp::Complete((ref resp, body_offset))) = thhp::Response::parse(&buf[..n_bytes_read], &mut headers) {
                        let status = resp.status;
                        let insert_done = Instant::now();
                        let took = insert_done.saturating_duration_since(insert_start).as_nanos() as i64;
                        let endpoint = "new";
                        let status = resp.status.to_string();
                        measure!(influx, insert_workouts_test, t(endpoint), t(status), i(took), tm(Utc::now().timestamp_nanos()));

                        if resp.status != 204 {
                            eprintln!("***\nREQUEST:\n\n{}\n\nRESPONSE:\n\n{}",
                                http_req_str,
                                std::str::from_utf8(&buf[..n_bytes_read]).unwrap(),
                            );
                        }
                        break 'insert_resp
                    }
                }

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

fn main() {
    dotenv::dotenv().unwrap();
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

        Opt::ListRequest { users_csv_path, user_id, start, end, limit, email } => {
            assert!(users_csv_path.exists(), "path does not exist: {}", users_csv_path.display());
            list_request(&users_csv_path, user_id, email, start, end, limit);
        }

        Opt::NewWorkoutsRequest { users_csv_path, user_id, email, date, duration } => {
            assert!(users_csv_path.exists(), "path does not exist: {}", users_csv_path.display());
            new_workouts_request(&users_csv_path, user_id, email, date, duration);
        }
    }
}
