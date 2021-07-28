use std::path::*;
use std::io::prelude::*;
use std::convert::TryInto;
use serde::{Serialize, Deserialize};
use chrono::prelude::*;
use rand::prelude::*;
use tokio::runtime::Runtime;
use uuid::Uuid;
use itertools::Itertools;
use chrono_tz::US::Pacific;
use structopt::StructOpt;

#[derive(StructOpt)]
enum Opt {
    /// insert user.csv example data provided by fitbot to postgres. this does
    /// *not* load the data in workout.csv.
    SetupExampleUsers {
        /// where to save csv with generated user data (user_id, private keys) corresponding
        /// to user.csv emails
        output_path: PathBuf,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsersCsvRow {
    #[serde(alias = "Email")]
    email: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkoutsCsvRow {
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
    pub encoded_key: String,
}

fn load_users() -> Vec<UsersCsvRow> {
    let bytes = std::fs::read("var/user.csv").unwrap();
    let mut rdr = csv::Reader::from_reader(&bytes[..]);
    let mut out = Vec::new();
    for row in rdr.deserialize() {
        let row = row.unwrap();
        out.push(row);
    }
    out
}

fn load_workouts() -> Vec<WorkoutsCsvRow> {
    let bytes = std::fs::read("var/workout.csv").unwrap();
    let mut rdr = csv::Reader::from_reader(&bytes[..]);
    let mut out = Vec::new();
    for row in rdr.deserialize() {
        let row = row.unwrap();
        out.push(row);
    }
    out
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

impl<'a> From<&'a UserPrivate> for UserPrivateEncoded {
    fn from(user: &'a UserPrivate) -> Self {
        let encoded_key = base64::encode(&user.key[..]);
        Self {
            user_id: user.user_id,
            encoded_key,
        }
    }
}

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

fn setup_example_users(output_path: &Path) {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let users = load_users();
    let emails: Vec<String> = users.into_iter().map(|UsersCsvRow { email }| email).collect();
    let (user_priv, user_pub) = generate_users_from_emails(&emails);
    let rt  = Runtime::new().unwrap();
    rt.block_on(async {
        let db = fitbod::db::DataBase::new(&db_url).await.unwrap();
        db.insert_users(&user_pub[..]).await.unwrap();
    });
    let user_priv_encoded: Vec<UserPrivateEncoded> = user_priv.into_iter().map(|x| UserPrivateEncoded::from(&x)).collect();
    let mut file = std::fs::File::create(output_path).unwrap();
    writeln!(&mut file, "user_id,email,private_key,public_key").unwrap();
    let n = user_priv_encoded.len();
    assert_eq!(user_pub.len(), n);
    assert_eq!(emails.len(), n);
    for i in 0..n {
        let pub_key = base64::encode(&user_pub[i].key[..]);
        assert_eq!(user_priv_encoded[i].user_id, user_pub[i].user_id);
        writeln!(&mut file, "{},{},{},{}", user_pub[i].user_id, user_pub[i].email, pub_key, user_priv_encoded[i].encoded_key).unwrap();
    }
}

fn main() {
    dotenv::dotenv().unwrap();
    match Opt::from_args() {
        Opt::SetupExampleUsers { output_path } => {
            setup_example_users(&output_path);
        }
    }
}
