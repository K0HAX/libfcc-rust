use ciborium;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::Path;

mod config;
mod data;
mod get_uls;
mod parse;
mod sql;
mod unzip_uls;

#[derive(Serialize, Deserialize, Clone)]
pub struct FccDB {
    amateur: Vec<data::Amateur>,
    entity: Vec<data::Entity>,
    application_license_header: Vec<data::ApplicationLicenseHeader>,
}

#[derive(Serialize, Clone)]
pub struct FccDB2<'a> {
    pub unique_system_identifier: u32,
    pub uls_file_num: &'a str,
    pub ebf_number: &'a str,
    pub callsign: &'a str,
    pub operator_class: &'a crate::data::OperatorClass,
    pub group_code: &'a str,
    pub region_code: &'a crate::data::U64Null,
    pub trustee_callsign: &'a str,
    pub trustee_indicator: &'a str,
    pub physician_certification: &'a str,
    pub ve_signature: &'a str,
    pub systematic_callsign_change: &'a str,
    pub vanity_callsign_change: &'a str,
    pub vanity_relationship: &'a str,
    pub previous_callsign: &'a str,
    pub previous_operator_class: &'a str,
    pub trustee_name: &'a str,
    pub entity_type: &'a str,
    pub licensee_id: &'a str,
    pub entity_name: &'a str,
    pub first_name: &'a str,
    pub mi: &'a str,
    pub last_name: &'a str,
    pub suffix: &'a str,
    pub phone: &'a str,
    pub fax: &'a str,
    pub email: &'a str,
    pub street_address: &'a str,
    pub city: &'a str,
    pub state: &'a str,
    pub zip_code: &'a str,
    pub po_box: &'a str,
    pub attention_line: &'a str,
    pub sgin: &'a str,
    pub frn: &'a str,
    pub applicant_type_code: &'a str,
    pub status_code: &'a str,
    pub status_date: &'a str,
    pub lic_category_code: &'a str,
    pub linked_license_id: &'a str,
    pub linked_callsign: &'a str,
    pub license_status: &'a crate::data::LicenseStatus,
    pub radio_service_code: &'a str,
    pub grant_date: &'a str,
    pub expired_date: &'a str,
    pub cancellation_date: &'a str,
    pub eligibility_rule_num: &'a str,
    pub applicant_type_code_reserved: &'a str,
    pub alien: &'a str,
    pub alien_government: &'a str,
    pub alien_corporation: &'a str,
    pub alien_officer: &'a str,
    pub alien_control: &'a str,
    pub revoked: &'a str,
    pub convicted: &'a str,
    pub adjudged: &'a str,
    pub involved_reserved: &'a str,
    pub common_carrier: &'a str,
    pub non_common_carrier: &'a str,
    pub private_comm: &'a str,
    pub fixed: &'a str,
    pub mobile: &'a str,
    pub radiolocation: &'a str,
    pub satellite: &'a str,
    pub developmental_or_sta: &'a crate::data::DevelopmentalStaDemonstration,
    pub interconnected_service: &'a str,
    pub certifier_first_name: &'a str,
    pub certifier_mi: &'a str,
    pub certifier_last_name: &'a str,
    pub certifier_suffix: &'a str,
    pub certifier_title: &'a str,
    pub gender: &'a str,
    pub african_american: &'a str,
    pub native_american: &'a str,
    pub hawaiian: &'a str,
    pub asian: &'a str,
    pub white: &'a str,
    pub ethnicity: &'a str,
    pub effective_date: &'a str,
    pub last_action_date: &'a str,
    pub auction_id: &'a crate::data::U64Null,
    pub reg_stat_broad_serv: &'a str,
    pub band_manager: &'a str,
    pub type_serv_broad_serv: &'a str,
    pub alien_ruling: &'a str,
    pub licensee_name_change: &'a str,
    pub whitespace_ind: &'a str,
    pub additional_cert_choice: &'a str,
    pub additional_cert_answer: &'a str,
    pub discontinuation_ind: &'a str,
    pub regulatory_compliance_ind: &'a str,
    pub eligibility_cert_900: &'a str,
    pub transition_plan_cert_900: &'a str,
    pub return_spectrum_cert_900: &'a str,
    pub payment_cert_900: &'a str,
}

fn parse_am_file(filename: &str) -> Vec<data::Amateur> {
    let mut am_records: Vec<data::Amateur> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
        for line in lines {
            if let Ok(line) = line {
                let am_record = parse::parse_am_line(line);
                am_records.push(am_record);
            }
        }
    }

    am_records
}

fn am_hashmap(data: &Vec<data::Amateur>) -> HashMap<u32, &data::Amateur> {
    let mut am_records: HashMap<u32, &data::Amateur> = HashMap::new();
    for record in data {
        am_records.insert(record.unique_system_identifier, record);
    }
    am_records
}

fn parse_en_file(filename: &str) -> Vec<data::Entity> {
    let mut en_records: Vec<data::Entity> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
        for line in lines {
            if let Ok(line) = line {
                let en_record = parse::parse_en_line(line);
                en_records.push(en_record);
            }
        }
    }

    en_records
}

fn en_hashmap(data: &Vec<data::Entity>) -> HashMap<u32, &data::Entity> {
    let mut en_records: HashMap<u32, &data::Entity> = HashMap::new();
    for record in data {
        en_records.insert(record.unique_system_identifier, record);
    }
    en_records
}

fn parse_hd_file(filename: &str) -> Vec<data::ApplicationLicenseHeader> {
    let mut hd_records: Vec<data::ApplicationLicenseHeader> = Vec::new();
    if let Ok(lines) = read_lines(filename) {
        for line in lines {
            if let Ok(line) = line {
                let hd_record = parse::parse_hd_line(line);
                hd_records.push(hd_record);
            }
        }
    }
    hd_records
}

fn hd_hashmap(
    data: &Vec<data::ApplicationLicenseHeader>,
) -> HashMap<u32, &data::ApplicationLicenseHeader> {
    let mut hd_records: HashMap<u32, &data::ApplicationLicenseHeader> = HashMap::new();
    for record in data {
        hd_records.insert(record.unique_system_identifier, record);
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

fn build_fcc_db2(orig_db: &FccDB) -> Vec<FccDB2<'_>> {
    let amateur_map = am_hashmap(&orig_db.amateur);
    let entity_map = en_hashmap(&orig_db.entity);
    let hd_map = hd_hashmap(&orig_db.application_license_header);

    let mut retval: Vec<FccDB2> = Vec::new();

    for item in &orig_db.entity {
        let this_am = match amateur_map.get(&item.unique_system_identifier) {
            Some(x) => x,
            None => {
                continue;
            }
        };
        let this_en = match entity_map.get(&item.unique_system_identifier) {
            Some(x) => x,
            None => {
                continue;
            }
        };
        let this_hd = match hd_map.get(&item.unique_system_identifier) {
            Some(x) => x,
            None => {
                continue;
            }
        };
        let this_item = FccDB2 {
            unique_system_identifier: this_am.unique_system_identifier,
            uls_file_num: &this_am.uls_file_num,
            ebf_number: &this_am.ebf_number,
            callsign: &this_am.callsign,
            operator_class: &this_am.operator_class,
            group_code: &this_am.group_code,
            region_code: &this_am.region_code,
            trustee_callsign: &this_am.trustee_callsign,
            trustee_indicator: &this_am.trustee_indicator,
            physician_certification: &this_am.physician_certification,
            ve_signature: &this_am.ve_signature,
            systematic_callsign_change: &this_am.systematic_callsign_change,
            vanity_callsign_change: &this_am.vanity_callsign_change,
            vanity_relationship: &this_am.vanity_relationship,
            previous_callsign: &this_am.previous_callsign,
            previous_operator_class: &this_am.previous_operator_class,
            trustee_name: &this_am.trustee_name,
            entity_type: &this_en.entity_type,
            licensee_id: &this_en.licensee_id,
            entity_name: &this_en.entity_name,
            first_name: &this_en.first_name,
            mi: &this_en.mi,
            last_name: &this_en.last_name,
            suffix: &this_en.suffix,
            phone: &this_en.phone,
            fax: &this_en.fax,
            email: &this_en.email,
            street_address: &this_en.street_address,
            city: &this_en.city,
            state: &this_en.state,
            zip_code: &this_en.zip_code,
            po_box: &this_en.po_box,
            attention_line: &this_en.attention_line,
            sgin: &this_en.sgin,
            frn: &this_en.frn,
            applicant_type_code: &this_en.applicant_type_code,
            status_code: &this_en.status_code,
            status_date: &this_en.status_date,
            lic_category_code: &this_en.lic_category_code,
            linked_license_id: &this_en.linked_license_id,
            linked_callsign: &this_en.linked_callsign,
            license_status: &this_hd.license_status,
            radio_service_code: &this_hd.radio_service_code,
            grant_date: &this_hd.grant_date,
            expired_date: &this_hd.expired_date,
            cancellation_date: &this_hd.cancellation_date,
            eligibility_rule_num: &this_hd.eligibility_rule_num,
            applicant_type_code_reserved: &this_hd.applicant_type_code_reserved,
            alien: &this_hd.alien,
            alien_government: &this_hd.alien_government,
            alien_corporation: &this_hd.alien_corporation,
            alien_officer: &this_hd.alien_officer,
            alien_control: &this_hd.alien_control,
            revoked: &this_hd.revoked,
            convicted: &this_hd.convicted,
            adjudged: &this_hd.adjudged,
            involved_reserved: &this_hd.involved_reserved,
            common_carrier: &this_hd.common_carrier,
            non_common_carrier: &this_hd.non_common_carrier,
            private_comm: &this_hd.private_comm,
            fixed: &this_hd.fixed,
            mobile: &this_hd.mobile,
            radiolocation: &this_hd.radiolocation,
            satellite: &this_hd.satellite,
            developmental_or_sta: &this_hd.developmental_or_sta,
            interconnected_service: &this_hd.interconnected_service,
            certifier_first_name: &this_hd.certifier_first_name,
            certifier_mi: &this_hd.certifier_mi,
            certifier_last_name: &this_hd.certifier_last_name,
            certifier_suffix: &this_hd.certifier_suffix,
            certifier_title: &this_hd.certifier_title,
            gender: &this_hd.gender,
            african_american: &this_hd.african_american,
            native_american: &this_hd.native_american,
            hawaiian: &this_hd.hawaiian,
            asian: &this_hd.asian,
            white: &this_hd.white,
            ethnicity: &this_hd.ethnicity,
            effective_date: &this_hd.effective_date,
            last_action_date: &this_hd.last_action_date,
            auction_id: &this_hd.auction_id,
            reg_stat_broad_serv: &this_hd.reg_stat_broad_serv,
            band_manager: &this_hd.band_manager,
            type_serv_broad_serv: &this_hd.type_serv_broad_serv,
            alien_ruling: &this_hd.alien_ruling,
            licensee_name_change: &this_hd.licensee_name_change,
            whitespace_ind: &this_hd.whitespace_ind,
            additional_cert_choice: &this_hd.additional_cert_choice,
            additional_cert_answer: &this_hd.additional_cert_answer,
            discontinuation_ind: &this_hd.discontinuation_ind,
            regulatory_compliance_ind: &this_hd.regulatory_compliance_ind,
            eligibility_cert_900: &this_hd.eligibility_cert_900,
            transition_plan_cert_900: &this_hd.transition_plan_cert_900,
            return_spectrum_cert_900: &this_hd.return_spectrum_cert_900,
            payment_cert_900: &this_hd.payment_cert_900,
        };
        retval.push(this_item);
    }
    retval
}

#[allow(unused_must_use)]
#[tokio::main]
async fn main() {
    let config_file = std::fs::File::open("config.yaml").expect("create failed");
    let main_config: config::Configuration =
        serde_yaml::from_reader(config_file).expect("Could not read values.");
    if main_config.download_db {
        match get_uls::download_ham_db().await {
            Err(why) => panic!("download_ham_db failed: {}", why),
            Ok(res) => res,
        };
    }
    unzip_uls::unzip_uls();
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
            let amateur: Vec<data::Amateur> = parse_am_file("data/AM.dat");
            sql::insert_am_rows(&main_config.mysql_config.sql_url, amateur);
        }
        println!("Done writing ham_AM MySQL");

        // ham_EN
        println!("Beginning writing ham_EN MySQL");
        {
            let entity: Vec<data::Entity> = parse_en_file("data/EN.dat");
            sql::insert_en_rows(&main_config.mysql_config.sql_url, entity);
        }
        println!("Done writing ham_EN MySQL");

        // ham_HD
        println!("Beginning writing ham_HD MySQL");
        {
            let application_license_header: Vec<data::ApplicationLicenseHeader> =
                parse_hd_file("data/HD.dat");
            sql::insert_hd_rows(
                &main_config.mysql_config.sql_url,
                application_license_header,
            );
        }
        println!("Done writing ham_HD MySQL");
    }

    if main_config.write_json | main_config.write_dat {
        let fcc_db = build_fcc_db();
        if main_config.write_json {
            let fcc_db2 = build_fcc_db2(&fcc_db);
            let serialized = serde_json::to_string_pretty(&fcc_db2).unwrap();
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
