use ciborium;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

mod libfcc_config;
mod libfcc_data;
mod libfcc_get_uls;
mod libfcc_parse;
mod libfcc_sql;
mod libfcc_unzip_uls;

#[derive(Serialize, Deserialize, Clone)]
pub struct FccDB {
    amateur: Vec<libfcc_data::Amateur>,
    entity: Vec<libfcc_data::Entity>,
    application_license_header: Vec<libfcc_data::ApplicationLicenseHeader>,
}

fn parse_am_file(filename: &str) -> Vec<libfcc_data::Amateur> {
    let mut am_records: Vec<libfcc_data::Amateur> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
        for line in lines {
            if let Ok(line) = line {
                let am_record = libfcc_parse::parse_am_line(line);
                am_records.push(am_record);
            }
        }
    }

    am_records
}

fn parse_en_file(filename: &str) -> Vec<libfcc_data::Entity> {
    let mut en_records: Vec<libfcc_data::Entity> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
        for line in lines {
            if let Ok(line) = line {
                let en_record = libfcc_parse::parse_en_line(line);
                en_records.push(en_record);
            }
        }
    }

    en_records
}

fn parse_hd_file(filename: &str) -> Vec<libfcc_data::ApplicationLicenseHeader> {
    let mut hd_records: Vec<libfcc_data::ApplicationLicenseHeader> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
        for line in lines {
            if let Ok(line) = line {
                let hd_record = libfcc_parse::parse_hd_line(line);
                hd_records.push(hd_record);
            }
        }
    }
    hd_records
}

fn build_fcc_db() -> FccDB {
    let amateur = parse_am_file("data/AM.dat");
    let entity = parse_en_file("data/EN.dat");
    let application_license_header = parse_hd_file("data/HD.dat");

    FccDB {
        amateur: amateur,
        entity: entity,
        application_license_header: application_license_header,
    }
}

#[allow(unused_must_use)]
#[tokio::main]
async fn main() {
    let config_file = std::fs::File::open("config.yaml").expect("create failed");
    let main_config: libfcc_config::Configuration =
        serde_yaml::from_reader(config_file).expect("Could not read values.");
    if main_config.download_db {
        match libfcc_get_uls::download_ham_db().await {
            Err(why) => panic!("download_ham_db failed: {}", why),
            Ok(res) => res,
        };
    }
    libfcc_unzip_uls::unzip_uls();
    println!("Begin reading FCC Database");

    if main_config.write_sql {
        // ham_AM
        println!("Beginning writing ham_AM MySQL");
        /*
         * Wrap this in a closure to save memory.
         *
         * This is probably not necessary, since we move `amateur' when we do insert_am_rows.
         */
        {
            let amateur: Vec<libfcc_data::Amateur> = parse_am_file("data/AM.dat");
            libfcc_sql::insert_am_rows(&main_config.mysql_config.sql_url, amateur);
        }
        println!("Done writing ham_AM MySQL");

        // ham_EN
        println!("Beginning writing ham_EN MySQL");
        {
            let entity: Vec<libfcc_data::Entity> = parse_en_file("data/EN.dat");
            libfcc_sql::insert_en_rows(&main_config.mysql_config.sql_url, entity);
        }
        println!("Done writing ham_EN MySQL");

        // ham_HD
        println!("Beginning writing ham_HD MySQL");
        {
            let application_license_header: Vec<libfcc_data::ApplicationLicenseHeader> =
                parse_hd_file("data/HD.dat");
            libfcc_sql::insert_hd_rows(
                &main_config.mysql_config.sql_url,
                application_license_header,
            );
        }
        println!("Done writing ham_HD MySQL");
    }

    // fccdb.json
    let fcc_db: Option<FccDB>;
    if main_config.write_json | main_config.write_dat {
        fcc_db = Some(build_fcc_db());
    } else {
        fcc_db = None;
    }
    if main_config.write_json {
        let serialized = serde_json::to_string_pretty(&fcc_db).unwrap();
        let json_filename = main_config.json_filename;
        let mut file = std::fs::File::create(json_filename).expect("create failed");
        file.write_all(serialized.as_bytes()).expect("write failed");
        println!("Data written to file");
    }

    if main_config.write_dat {
        let cibor_filename = "fccdb.dat";
        let file = std::fs::File::create(cibor_filename).expect("create failed");
        ciborium::ser::into_writer(&fcc_db, file).unwrap();
        println!("Data written to file");
    }
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
