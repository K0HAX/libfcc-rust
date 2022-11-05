use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;
use serde::{Serialize, Deserialize};
use ciborium;

mod libfcc_data;
mod libfcc_sql;
mod libfcc_parse;
mod libfcc_config;
mod libfcc_get_uls;
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
    let main_config: libfcc_config::Configuration = serde_yaml::from_reader(config_file).expect("Could not read values.");
    if main_config.download_db {
	match libfcc_get_uls::download_ham_db().await {
	    Err(why) => panic!("download_ham_db failed: {}", why),
	    Ok(res) => res,
	};
    }
    libfcc_unzip_uls::unzip_uls();
    println!("Begin reading FCC Database");
    let fcc_db = build_fcc_db();

    if main_config.write_sql {
	// ham_AM
	println!("Beginning writing ham_AM MySQL");
	libfcc_sql::insert_am_rows(&main_config.mysql_config.sql_url, fcc_db.amateur.clone());
	println!("Done writing ham_AM MySQL");

	// ham_EN
	println!("Beginning writing ham_EN MySQL");
	libfcc_sql::insert_en_rows(&main_config.mysql_config.sql_url, fcc_db.entity.clone());
	println!("Done writing ham_EN MySQL");

	// ham_HD
	println!("Beginning writing ham_HD MySQL");
	libfcc_sql::insert_hd_rows(&main_config.mysql_config.sql_url, fcc_db.application_license_header.clone());
	println!("Done writing ham_HD MySQL");
    }

    // fccdb.json
    if main_config.write_json {
	let serialized = serde_json::to_string_pretty(&fcc_db).unwrap();
	let json_filename = main_config.json_filename;
	let mut file = std::fs::File::create(json_filename).expect("create failed");
	file.write_all(serialized.as_bytes()).expect("write failed");
	println!("Data written to file");
    }

    {
	let cibor_filename = "fccdb.dat";
	let file = std::fs::File::create(cibor_filename).expect("create failed");
	ciborium::ser::into_writer(&fcc_db, file).unwrap();
	println!("Data written to file");
    }

    /*
    let mut file_am = std::fs::File::create("am.json").expect("create failed");
    let mut file_en = std::fs::File::create("en.json").expect("create failed");
    let mut file_hd = std::fs::File::create("hd.json").expect("create failed");
    let am_serialized = serde_json::to_string_pretty(&fcc_db.amateur).unwrap();
    let en_serialized = serde_json::to_string_pretty(&fcc_db.entity).unwrap();
    let hd_serialized = serde_json::to_string_pretty(&fcc_db.application_license_header).unwrap();
    file_am.write_all(am_serialized.as_bytes()).expect("write failed");
    file_en.write_all(en_serialized.as_bytes()).expect("write failed");
    file_hd.write_all(hd_serialized.as_bytes()).expect("write failed");
    */
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
