use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct FccDB {
    amateur: Vec<Amateur>,
    entity: Vec<Entity>,
}

// Begin Enums
#[derive(Serialize, Deserialize)]
enum OperatorClass {
    Advanced,
    AmateurExtra,
    General,
    Novice,
    TechnicianPlus,
    Technician,
    Unknown,
}

fn operator_class_map(operator_class: &str) -> OperatorClass {
    match operator_class {
	"A" => OperatorClass::Advanced,
	"E" => OperatorClass::AmateurExtra,
	"G" => OperatorClass::General,
	"N" => OperatorClass::Novice,
	"P" => OperatorClass::TechnicianPlus,
	"T" => OperatorClass::Technician,
	_ => OperatorClass::Unknown,
    }
}
// End Enums

// AM.dat
#[derive(Serialize, Deserialize)]
struct Amateur {
    record_type: String,
    unique_system_identifier: u32,
    uls_file_num: String,
    ebf_number: String,
    callsign: String,
    operator_class: OperatorClass,
    group_code: String,
    region_code: String,
    trustee_callsign: String,
    trustee_indicator: String,
    physician_certification: String,
    ve_signature: String,
    systematic_callsign_change: String,
    vanity_callsign_change: String,
    vanity_relationship: String,
    previous_callsign: String,
    previous_operator_class: String,
    trustee_name: String,
}

// EN.dat
#[derive(Serialize, Deserialize)]
struct Entity {
    record_type: String,
    unique_system_identifier: u32,
    uls_file_num: String,
    ebf_number: String,
    call_sign: String,
    entity_type: String,
    licensee_id: String,
    entity_name: String,
    first_name: String,
    mi: String,
    last_name: String,
    suffix: String,
    phone: String,
    fax: String,
    email: String,
    street_address: String,
    city: String,
    state: String,
    zip_code: String,
    po_box: String,
    attention_line: String,
    sgin: String,
    frn: String,
    applicant_type_code: String,
    status_code: String,
    status_date: String,
    lic_category_code: String,
    linked_license_id: String,
    linked_callsign: String,
}

fn parse_am_line(line: String) -> Amateur {
    let split: Vec<&str> = line.split("|").collect();
    let unique_system_identifier: u32 = split[1].trim().parse().expect("Unique System Identifier is not a number!");
    let operator_class = operator_class_map(split[5]);
    Amateur {
	record_type: String::from(split[0]),
	unique_system_identifier: unique_system_identifier,
	uls_file_num: String::from(split[2]),
	ebf_number: String::from(split[3]),
	callsign: String::from(split[4]),
	operator_class: operator_class,
	group_code: String::from(split[6]),
	region_code: String::from(split[7]),
	trustee_callsign: String::from(split[8]),
	trustee_indicator: String::from(split[9]),
	physician_certification: String::from(split[10]),
	ve_signature: String::from(split[11]),
	systematic_callsign_change: String::from(split[12]),
	vanity_callsign_change: String::from(split[13]),
	vanity_relationship: String::from(split[14]),
	previous_callsign: String::from(split[15]),
	previous_operator_class: String::from(split[16]),
	trustee_name: String::from(split[17]),
    }
}

fn parse_en_line(line: String) -> Entity {
    let split: Vec<&str> = line.split("|").collect();
    let unique_system_identifier: u32 = split[1].trim().parse().expect("Unique System Identifier is not a number!");
    Entity {
	record_type: String::from(split[0]),
	unique_system_identifier: unique_system_identifier,
	uls_file_num: String::from(split[2]),
	ebf_number: String::from(split[3]),
	call_sign: String::from(split[4]),
	entity_type: String::from(split[5]),
	licensee_id: String::from(split[6]),
	entity_name: String::from(split[7]),
	first_name: String::from(split[8]),
	mi: String::from(split[9]),
	last_name: String::from(split[10]),
	suffix: String::from(split[11]),
	phone: String::from(split[12]),
	fax: String::from(split[13]),
	email: String::from(split[14]),
	street_address: String::from(split[15]),
	city: String::from(split[16]),
	state: String::from(split[17]),
	zip_code: String::from(split[18]),
	po_box: String::from(split[19]),
	attention_line: String::from(split[20]),
	sgin: String::from(split[21]),
	frn: String::from(split[22]),
	applicant_type_code: String::from(split[23]),
	status_code: String::from(split[24]),
	status_date: String::from(split[25]),
	lic_category_code: String::from(split[26]),
	linked_license_id: String::from(split[27]),
	linked_callsign: String::from(split[28]),
    }
}

fn parse_am_file(filename: &str) -> Vec<Amateur> {
    let mut am_records: Vec<Amateur> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
	for line in lines {
	    if let Ok(line) = line {
		let am_record = parse_am_line(line);
		am_records.push(am_record);
	    }
	}
    }

    am_records
}

fn parse_en_file(filename: &str) -> Vec<Entity> {
    let mut en_records: Vec<Entity> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
	for line in lines {
	    if let Ok(line) = line {
		let en_record = parse_en_line(line);
		en_records.push(en_record);
	    }
	}
    }

    en_records
}

fn build_fcc_db() -> FccDB {
    let amateur = parse_am_file("data/AM.dat");
    let entity = parse_en_file("data/EN.dat");

    FccDB {
	amateur: amateur,
	entity: entity,
    }
}

fn main() {
    let fcc_db = build_fcc_db();
    let serialized = serde_json::to_string_pretty(&fcc_db).unwrap();
    let mut file = std::fs::File::create("fccdb.json").expect("create failed");
    file.write_all(serialized.as_bytes()).expect("write failed");
    println!("Data written to file");
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
