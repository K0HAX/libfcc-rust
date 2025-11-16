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

#[derive(Serialize, Deserialize, Clone)]
pub struct FccDB2 {
    pub unique_system_identifier: u32,
    pub uls_file_num: String,
    pub ebf_number: String,
    pub callsign: String,
    pub operator_class: crate::data::OperatorClass,
    pub group_code: String,
    pub region_code: crate::data::U64Null,
    pub trustee_callsign: String,
    pub trustee_indicator: String,
    pub physician_certification: String,
    pub ve_signature: String,
    pub systematic_callsign_change: String,
    pub vanity_callsign_change: String,
    pub vanity_relationship: String,
    pub previous_callsign: String,
    pub previous_operator_class: String,
    pub trustee_name: String,
    pub entity_type: String,
    pub licensee_id: String,
    pub entity_name: String,
    pub first_name: String,
    pub mi: String,
    pub last_name: String,
    pub suffix: String,
    pub phone: String,
    pub fax: String,
    pub email: String,
    pub street_address: String,
    pub city: String,
    pub state: String,
    pub zip_code: String,
    pub po_box: String,
    pub attention_line: String,
    pub sgin: String,
    pub frn: String,
    pub applicant_type_code: String,
    pub status_code: String,
    pub status_date: String,
    pub lic_category_code: String,
    pub linked_license_id: String,
    pub linked_callsign: String,
    pub license_status: crate::data::LicenseStatus,
    pub radio_service_code: String,
    pub grant_date: String,
    pub expired_date: String,
    pub cancellation_date: String,
    pub eligibility_rule_num: String,
    pub applicant_type_code_reserved: String,
    pub alien: String,
    pub alien_government: String,
    pub alien_corporation: String,
    pub alien_officer: String,
    pub alien_control: String,
    pub revoked: String,
    pub convicted: String,
    pub adjudged: String,
    pub involved_reserved: String,
    pub common_carrier: String,
    pub non_common_carrier: String,
    pub private_comm: String,
    pub fixed: String,
    pub mobile: String,
    pub radiolocation: String,
    pub satellite: String,
    pub developmental_or_sta: crate::data::DevelopmentalStaDemonstration,
    pub interconnected_service: String,
    pub certifier_first_name: String,
    pub certifier_mi: String,
    pub certifier_last_name: String,
    pub certifier_suffix: String,
    pub certifier_title: String,
    pub gender: String,
    pub african_american: String,
    pub native_american: String,
    pub hawaiian: String,
    pub asian: String,
    pub white: String,
    pub ethnicity: String,
    pub effective_date: String,
    pub last_action_date: String,
    pub auction_id: crate::data::U64Null,
    pub reg_stat_broad_serv: String,
    pub band_manager: String,
    pub type_serv_broad_serv: String,
    pub alien_ruling: String,
    pub licensee_name_change: String,
    pub whitespace_ind: String,
    pub additional_cert_choice: String,
    pub additional_cert_answer: String,
    pub discontinuation_ind: String,
    pub regulatory_compliance_ind: String,
    pub eligibility_cert_900: String,
    pub transition_plan_cert_900: String,
    pub return_spectrum_cert_900: String,
    pub payment_cert_900: String,
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

fn am_hashmap(data: Vec<data::Amateur>) -> HashMap<u32, data::Amateur> {
    let mut am_records: HashMap<u32, data::Amateur> = HashMap::new();
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

fn en_hashmap(data: Vec<data::Entity>) -> HashMap<u32, data::Entity> {
    let mut en_records: HashMap<u32, data::Entity> = HashMap::new();
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
    data: Vec<data::ApplicationLicenseHeader>,
) -> HashMap<u32, data::ApplicationLicenseHeader> {
    let mut hd_records: HashMap<u32, data::ApplicationLicenseHeader> = HashMap::new();
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

fn build_fcc_db2() -> Vec<FccDB2> {
    let amateur = parse_am_file("data/AM.dat");
    let entity = parse_en_file("data/EN.dat");
    let application_license_header = parse_hd_file("data/HD.dat");

    let amateur_map = am_hashmap(amateur);
    let entity_map = en_hashmap(entity.clone());
    let hd_map = hd_hashmap(application_license_header);

    let mut retval: Vec<FccDB2> = Vec::new();

    for item in entity {
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
            uls_file_num: this_am.uls_file_num.clone(),
            ebf_number: this_am.ebf_number.clone(),
            callsign: this_am.callsign.clone(),
            operator_class: this_am.operator_class.clone(),
            group_code: this_am.group_code.clone(),
            region_code: this_am.region_code.clone(),
            trustee_callsign: this_am.trustee_callsign.clone(),
            trustee_indicator: this_am.trustee_indicator.clone(),
            physician_certification: this_am.physician_certification.clone(),
            ve_signature: this_am.ve_signature.clone(),
            systematic_callsign_change: this_am.systematic_callsign_change.clone(),
            vanity_callsign_change: this_am.vanity_callsign_change.clone(),
            vanity_relationship: this_am.vanity_relationship.clone(),
            previous_callsign: this_am.previous_callsign.clone(),
            previous_operator_class: this_am.previous_operator_class.clone(),
            trustee_name: this_am.trustee_name.clone(),
            entity_type: this_en.entity_type.clone(),
            licensee_id: this_en.licensee_id.clone(),
            entity_name: this_en.entity_name.clone(),
            first_name: this_en.first_name.clone(),
            mi: this_en.mi.clone(),
            last_name: this_en.last_name.clone(),
            suffix: this_en.suffix.clone(),
            phone: this_en.phone.clone(),
            fax: this_en.fax.clone(),
            email: this_en.email.clone(),
            street_address: this_en.street_address.clone(),
            city: this_en.city.clone(),
            state: this_en.state.clone(),
            zip_code: this_en.zip_code.clone(),
            po_box: this_en.po_box.clone(),
            attention_line: this_en.attention_line.clone(),
            sgin: this_en.sgin.clone(),
            frn: this_en.frn.clone(),
            applicant_type_code: this_en.applicant_type_code.clone(),
            status_code: this_en.status_code.clone(),
            status_date: this_en.status_date.clone(),
            lic_category_code: this_en.lic_category_code.clone(),
            linked_license_id: this_en.linked_license_id.clone(),
            linked_callsign: this_en.linked_callsign.clone(),
            license_status: this_hd.license_status.clone(),
            radio_service_code: this_hd.radio_service_code.clone(),
            grant_date: this_hd.grant_date.clone(),
            expired_date: this_hd.expired_date.clone(),
            cancellation_date: this_hd.cancellation_date.clone(),
            eligibility_rule_num: this_hd.eligibility_rule_num.clone(),
            applicant_type_code_reserved: this_hd.applicant_type_code_reserved.clone(),
            alien: this_hd.alien.clone(),
            alien_government: this_hd.alien_government.clone(),
            alien_corporation: this_hd.alien_corporation.clone(),
            alien_officer: this_hd.alien_officer.clone(),
            alien_control: this_hd.alien_control.clone(),
            revoked: this_hd.revoked.clone(),
            convicted: this_hd.convicted.clone(),
            adjudged: this_hd.adjudged.clone(),
            involved_reserved: this_hd.involved_reserved.clone(),
            common_carrier: this_hd.common_carrier.clone(),
            non_common_carrier: this_hd.non_common_carrier.clone(),
            private_comm: this_hd.private_comm.clone(),
            fixed: this_hd.fixed.clone(),
            mobile: this_hd.mobile.clone(),
            radiolocation: this_hd.radiolocation.clone(),
            satellite: this_hd.satellite.clone(),
            developmental_or_sta: this_hd.developmental_or_sta.clone(),
            interconnected_service: this_hd.interconnected_service.clone(),
            certifier_first_name: this_hd.certifier_first_name.clone(),
            certifier_mi: this_hd.certifier_mi.clone(),
            certifier_last_name: this_hd.certifier_last_name.clone(),
            certifier_suffix: this_hd.certifier_suffix.clone(),
            certifier_title: this_hd.certifier_title.clone(),
            gender: this_hd.gender.clone(),
            african_american: this_hd.african_american.clone(),
            native_american: this_hd.native_american.clone(),
            hawaiian: this_hd.hawaiian.clone(),
            asian: this_hd.asian.clone(),
            white: this_hd.white.clone(),
            ethnicity: this_hd.ethnicity.clone(),
            effective_date: this_hd.effective_date.clone(),
            last_action_date: this_hd.last_action_date.clone(),
            auction_id: this_hd.auction_id.clone(),
            reg_stat_broad_serv: this_hd.reg_stat_broad_serv.clone(),
            band_manager: this_hd.band_manager.clone(),
            type_serv_broad_serv: this_hd.type_serv_broad_serv.clone(),
            alien_ruling: this_hd.alien_ruling.clone(),
            licensee_name_change: this_hd.licensee_name_change.clone(),
            whitespace_ind: this_hd.whitespace_ind.clone(),
            additional_cert_choice: this_hd.additional_cert_choice.clone(),
            additional_cert_answer: this_hd.additional_cert_answer.clone(),
            discontinuation_ind: this_hd.discontinuation_ind.clone(),
            regulatory_compliance_ind: this_hd.regulatory_compliance_ind.clone(),
            eligibility_cert_900: this_hd.eligibility_cert_900.clone(),
            transition_plan_cert_900: this_hd.transition_plan_cert_900.clone(),
            return_spectrum_cert_900: this_hd.return_spectrum_cert_900.clone(),
            payment_cert_900: this_hd.payment_cert_900.clone(),
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

    // fccdb.json
    let fcc_db: Option<FccDB>;
    if main_config.write_json | main_config.write_dat {
        fcc_db = Some(build_fcc_db());
    } else {
        fcc_db = None;
    }
    if main_config.write_json {
        let fcc_db2 = build_fcc_db2();
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

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
