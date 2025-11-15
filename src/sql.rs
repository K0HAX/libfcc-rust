use indicatif::{MultiProgress, ProgressBar};
use mysql::prelude::*;
use mysql::*;
use rayon::ThreadPoolBuilder;
use std::convert::From;
use std::sync::Arc;

use crate::data;

// Begin Enums
impl From<data::OperatorClass> for mysql::Value {
    fn from(operator_class: data::OperatorClass) -> Self {
        match operator_class {
            data::OperatorClass::Advanced => mysql::Value::Bytes("A".as_bytes().to_vec()),
            data::OperatorClass::AmateurExtra => {
                mysql::Value::Bytes("E".as_bytes().to_vec())
            }
            data::OperatorClass::General => mysql::Value::Bytes("G".as_bytes().to_vec()),
            data::OperatorClass::Novice => mysql::Value::Bytes("N".as_bytes().to_vec()),
            data::OperatorClass::TechnicianPlus => {
                mysql::Value::Bytes("P".as_bytes().to_vec())
            }
            data::OperatorClass::Technician => mysql::Value::Bytes("T".as_bytes().to_vec()),
            data::OperatorClass::Unknown => mysql::Value::Bytes("".as_bytes().to_vec()),
        }
    }
}

impl From<data::U64Null> for mysql::Value {
    fn from(val: data::U64Null) -> Self {
        match val {
            data::U64Null::Value(value) => mysql::Value::UInt(value),
            data::U64Null::NULL => mysql::Value::NULL,
        }
    }
}

impl From<data::EntityType> for mysql::Value {
    fn from(val: data::EntityType) -> Self {
        match val {
            data::EntityType::TransfereeContact => {
                mysql::Value::Bytes("CE".as_bytes().to_vec())
            }
            data::EntityType::LicenseeContact => {
                mysql::Value::Bytes("CL".as_bytes().to_vec())
            }
            data::EntityType::AssignorOrTransferorContact => {
                mysql::Value::Bytes("CR".as_bytes().to_vec())
            }
            data::EntityType::LesseeContact => mysql::Value::Bytes("CS".as_bytes().to_vec()),
            data::EntityType::Transferee => mysql::Value::Bytes("E".as_bytes().to_vec()),
            data::EntityType::LicenseeOrAssignee => {
                mysql::Value::Bytes("L".as_bytes().to_vec())
            }
            data::EntityType::Owner => mysql::Value::Bytes("O".as_bytes().to_vec()),
            data::EntityType::AssignorOrTransferor => {
                mysql::Value::Bytes("R".as_bytes().to_vec())
            }
            data::EntityType::Lessee => mysql::Value::Bytes("S".as_bytes().to_vec()),
            data::EntityType::Unknown => mysql::Value::Bytes("".as_bytes().to_vec()),
        }
    }
}

impl From<data::ApplicantTypeCode> for mysql::Value {
    fn from(val: data::ApplicantTypeCode) -> Self {
        match val {
            data::ApplicantTypeCode::AmateurClub => {
                mysql::Value::Bytes("B".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::Corporation => {
                mysql::Value::Bytes("C".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::GeneralPartnership => {
                mysql::Value::Bytes("D".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::LimitedPartnership => {
                mysql::Value::Bytes("E".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::LimitedLiabilityPartnership => {
                mysql::Value::Bytes("F".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::GovernmentalEntity => {
                mysql::Value::Bytes("G".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::Other => mysql::Value::Bytes("H".as_bytes().to_vec()),
            data::ApplicantTypeCode::Individual => {
                mysql::Value::Bytes("I".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::JointVenture => {
                mysql::Value::Bytes("J".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::LimitedLiabilityCompany => {
                mysql::Value::Bytes("L".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::MilitaryRecreation => {
                mysql::Value::Bytes("M".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::Consortium => {
                mysql::Value::Bytes("O".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::Partnership => {
                mysql::Value::Bytes("P".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::RACES => mysql::Value::Bytes("R".as_bytes().to_vec()),
            data::ApplicantTypeCode::Trust => mysql::Value::Bytes("T".as_bytes().to_vec()),
            data::ApplicantTypeCode::UnincorporatedAssociation => {
                mysql::Value::Bytes("U".as_bytes().to_vec())
            }
            data::ApplicantTypeCode::Unknown => mysql::Value::Bytes("".as_bytes().to_vec()),
        }
    }
}

impl From<data::EnStatusCode> for mysql::Value {
    fn from(val: data::EnStatusCode) -> Self {
        match val {
            data::EnStatusCode::TerminationPending => {
                mysql::Value::Bytes("X".as_bytes().to_vec())
            }
            data::EnStatusCode::Terminated => mysql::Value::Bytes("T".as_bytes().to_vec()),
            data::EnStatusCode::Active => mysql::Value::Bytes("".as_bytes().to_vec()),
        }
    }
}

impl From<data::LicenseStatus> for mysql::Value {
    fn from(val: data::LicenseStatus) -> Self {
        match val {
            data::LicenseStatus::Active => mysql::Value::Bytes("A".as_bytes().to_vec()),
            data::LicenseStatus::Cancelled => mysql::Value::Bytes("C".as_bytes().to_vec()),
            data::LicenseStatus::Expired => mysql::Value::Bytes("E".as_bytes().to_vec()),
            data::LicenseStatus::PendingLegalStatus => {
                mysql::Value::Bytes("L".as_bytes().to_vec())
            }
            data::LicenseStatus::ParentStationCanceled => {
                mysql::Value::Bytes("P".as_bytes().to_vec())
            }
            data::LicenseStatus::Terminated => mysql::Value::Bytes("T".as_bytes().to_vec()),
            data::LicenseStatus::TermPending => mysql::Value::Bytes("X".as_bytes().to_vec()),
            data::LicenseStatus::Unknown => mysql::Value::Bytes("".as_bytes().to_vec()),
        }
    }
}

impl From<data::DevelopmentalStaDemonstration> for mysql::Value {
    fn from(val: data::DevelopmentalStaDemonstration) -> Self {
        match val {
            data::DevelopmentalStaDemonstration::Developmental => {
                mysql::Value::Bytes("D".as_bytes().to_vec())
            }
            data::DevelopmentalStaDemonstration::Demonstration => {
                mysql::Value::Bytes("M".as_bytes().to_vec())
            }
            data::DevelopmentalStaDemonstration::Regular => {
                mysql::Value::Bytes("N".as_bytes().to_vec())
            }
            data::DevelopmentalStaDemonstration::SpecialTemporaryAuthority => {
                mysql::Value::Bytes("S".as_bytes().to_vec())
            }
            data::DevelopmentalStaDemonstration::Unknown => {
                mysql::Value::Bytes("".as_bytes().to_vec())
            }
        }
    }
}
// End Enums

// BEGIN ham_AM //
fn split_am_rows(input_records: Vec<data::Amateur>) -> Vec<Vec<data::Amateur>> {
    let mut retval: Vec<Vec<data::Amateur>> = Vec::new();
    let mut i = 0;
    let mut n = 0;
    let split_modulus = input_records.len() / 10;
    for am_row in input_records {
        if (n % split_modulus) == 0 {
            let mut this_vec: Vec<data::Amateur> = Vec::new();
            this_vec.push(am_row);
            retval.push(this_vec);
            i = retval.len() - 1;
        } else {
            retval[i].push(am_row);
        }
        n = n + 1;
    }
    return retval;
}

fn do_am_drop(mut conn: mysql::PooledConn) -> mysql::Result<()> {
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = tx.exec_drop("DELETE FROM ham_AM", ());
    tx.commit().unwrap();
    return result;
}

fn insert_am_rows_batch(
    mut conn: mysql::PooledConn,
    am_records: Vec<data::Amateur>,
    chunk_id: u32,
    tot_chunks: usize,
    pb: &ProgressBar,
) {
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = tx.exec_batch(
        "INSERT INTO ham_AM (\
				`Record Type`, \
				`Unique System Identifier`, \
				`ULS File Number`, \
				`EBF Number`, \
				`Call Sign`, \
				`Operator Class`, \
				`Group Code`, \
				`Region Code`, \
				`Trustee Call Sign`, \
				`Trustee Indicator`, \
				`Physician Certification`, \
				`VE Signature`, \
				`Systematic Call Sign Change`, \
				`Vanity Call Sign Change`, \
				`Vanity Relationship`, \
				`Previous Call Sign`, \
				`Previous Operator Class`, \
				`Trustee Name`) \
				VALUES (\
				:record_type, \
				:unique_system_identifier, \
				:uls_file_num, \
				:ebf_number, \
				:callsign, \
				:operator_class, \
				:group_code, \
				:region_code, \
				:trustee_callsign, \
				:trustee_indicator, \
				:physician_certification, \
				:ve_signature, \
				:systematic_callsign_change, \
				:vanity_callsign_change, \
				:vanity_relationship, \
				:previous_callsign, \
				:previous_operator_class, \
				:trustee_name \
				)",
        am_records.iter().map(|p| {
            pb.inc(1);
            params! {
            "record_type" => p.record_type.clone(),
            "unique_system_identifier" => p.unique_system_identifier,
            "uls_file_num" => p.uls_file_num.clone(),
            "ebf_number" => p.ebf_number.clone(),
            "callsign" => p.callsign.clone(),
            "operator_class" => p.operator_class.clone(),
            "group_code" => p.group_code.clone(),
            "region_code" => p.region_code.clone(),
            "trustee_callsign" => p.trustee_callsign.clone(),
            "trustee_indicator" => p.trustee_indicator.clone(),
            "physician_certification" => p.physician_certification.clone(),
            "ve_signature" => p.ve_signature.clone(),
            "systematic_callsign_change" => p.systematic_callsign_change.clone(),
            "vanity_callsign_change" => p.vanity_callsign_change.clone(),
            "vanity_relationship" => p.vanity_relationship.clone(),
            "previous_callsign" => p.previous_callsign.clone(),
            "previous_operator_class" => p.previous_operator_class.clone(),
            "trustee_name" => p.trustee_name.clone(),
            }
        }),
    );
    match result {
        Ok(_result_value) => {
            tx.commit().unwrap();
        }
        Err(result_value) => {
            println!("Error: {:#?}", result_value);
            tx.rollback().unwrap();
        }
    };
    pb.finish();
    println!("Chunk {}/{} complete", chunk_id, tot_chunks);
}

pub fn insert_am_rows(sql_url: &str, am_records: Vec<data::Amateur>) {
    let pool = Pool::new(sql_url).unwrap();

    println!("Splitting rows");
    let am_records_split = split_am_rows(am_records);
    println!("Rows split into {} chunks", am_records_split.len());

    println!("Dropping ham_AM");
    do_am_drop(pool.get_conn().unwrap()).unwrap();

    println!("Inserting rows");
    let multiprogress_bar = Arc::new(MultiProgress::new());
    let tpool = ThreadPoolBuilder::new().num_threads(10).build().unwrap();
    let mut this_chunk = 0;
    let tot_chunks = am_records_split.len();
    let mp_clone = multiprogress_bar.clone();
    println!("Entering In Place Scope");
    tpool.in_place_scope(move |s| {
        for am_chunk in am_records_split {
            let conn = pool.get_conn().unwrap();
            this_chunk = this_chunk + 1;
            let m_clone = multiprogress_bar.clone();
            s.spawn(move |_| {
                let pb2 = m_clone.add(ProgressBar::new(am_chunk.len().try_into().unwrap()));
                insert_am_rows_batch(conn, am_chunk, this_chunk, tot_chunks, &pb2);
            });
        }
    });
    let _ = mp_clone.clear();
    println!("In Place Scope exited");
    //let _ = tx.commit();
    //let commit_result = tx.commit();
    //println!("commit_result: {:#?}", commit_result);
    return ();
}
// END ham_AM //

// BEGIN ham_EN //
fn insert_en_rows_batch(
    mut conn: PooledConn,
    en_records: Vec<data::Entity>,
    _chunk_id: u32,
    _tot_chunks: usize,
    pb: &ProgressBar,
) {
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = tx.exec_batch(
        "INSERT INTO ham_EN (\
				`Record Type`, \
				`Unique System Identifier`, \
				`ULS File Number`, \
				`EBF Number`, \
				`Call Sign`, \
				`Entity Type`, \
				`Licensee ID`, \
				`Entity Name`, \
				`First Name`, \
				`MI`, \
				`Last Name`, \
				`Suffix`, \
				`Phone`, \
				`Fax`, \
				`Email`, \
				`Street Address`, \
				`City`, \
				`State`, \
				`Zip Code`, \
				`PO Box`, \
				`Attention Line`, \
				`SGIN`, \
				`FRN`, \
				`Applicant Type Code`, \
				`Status Code`, \
				`Status Date` \
				) \
				VALUES (\
				:record_type, \
				:unique_system_identifier, \
				:uls_file_num, \
				:ebf_number, \
				:call_sign, \
				:entity_type, \
				:licensee_id, \
				:entity_name, \
				:first_name, \
				:mi, \
				:last_name, \
				:suffix, \
				:phone, \
				:fax, \
				:email, \
				:street_address, \
				:city, \
				:state, \
				:zip_code, \
				:po_box, \
				:attention_line, \
				:sgin, \
				:frn, \
				:applicant_type_code, \
				:status_code, \
				:status_date \
				)",
        en_records.iter().map(|p| {
            pb.inc(1);
            params! {
            "record_type" => p.record_type.clone(),
            "unique_system_identifier" => p.unique_system_identifier,
            "uls_file_num" => p.uls_file_num.clone(),
            "ebf_number" => p.ebf_number.clone(),
            "call_sign" => p.call_sign.clone(),
            "entity_type" => p.entity_type.clone(),
            "licensee_id" => p.licensee_id.clone(),
            "entity_name" => p.entity_name.clone(),
            "first_name" => p.first_name.clone(),
            "mi" => p.mi.clone(),
            "last_name" => p.last_name.clone(),
            "suffix" => p.suffix.clone(),
            "phone" => p.phone.clone(),
            "fax" => p.fax.clone(),
            "email" => p.email.clone(),
            "street_address" => p.street_address.clone(),
            "city" => p.city.clone(),
            "state" => p.state.clone(),
            "zip_code" => p.zip_code.clone(),
            "po_box" => p.po_box.clone(),
            "attention_line" => p.attention_line.clone(),
            "sgin" => p.sgin.clone(),
            "frn" => p.frn.clone(),
            "applicant_type_code" => p.applicant_type_code.clone(),
            "status_code" => p.status_code.clone(),
            "status_date" => p.status_date.clone(),
            }
        }),
    );
    let result_value = match result {
        Ok(_result_value) => {
            tx.commit().unwrap();
        }
        Err(result_value) => {
            println!("Error: {:#?}", result_value);
            tx.rollback().unwrap();
        }
    };
    pb.finish();
    //println!("Chunk {}/{} complete", chunk_id, tot_chunks);
    return result_value;
}

fn split_en_rows(input_records: Vec<data::Entity>) -> Vec<Vec<data::Entity>> {
    let mut retval: Vec<Vec<data::Entity>> = Vec::new();
    let mut i = 0;
    let mut n = 0;
    let split_modulus = input_records.len() / 10;
    for en_row in input_records {
        if (n % split_modulus) == 0 {
            let mut this_vec: Vec<data::Entity> = Vec::new();
            this_vec.push(en_row);
            retval.push(this_vec);
            i = retval.len() - 1;
        } else {
            retval[i].push(en_row);
        }
        n = n + 1;
    }
    return retval;
}

fn do_en_drop(mut conn: mysql::PooledConn) -> mysql::Result<()> {
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = tx.exec_drop("DELETE FROM ham_EN", ());
    tx.commit().unwrap();
    return result;
}

pub fn insert_en_rows(sql_url: &str, en_records: Vec<data::Entity>) {
    let pool = Pool::new(sql_url).unwrap();

    println!("Splitting rows!");
    let en_records_split = split_en_rows(en_records);
    println!("Rows split into {} chunks!", en_records_split.len());

    println!("Dropping ham_EN!");
    do_en_drop(pool.get_conn().unwrap()).unwrap();

    println!("Inserting rows!");
    let multiprogress_bar = Arc::new(MultiProgress::new());
    let tpool = ThreadPoolBuilder::new().num_threads(10).build().unwrap();
    let mut this_chunk = 0;
    let tot_chunks = en_records_split.len();
    let mp_clone = multiprogress_bar.clone();
    println!("Entering In Place Scope!");
    tpool.in_place_scope(move |s| {
        for en_chunk in en_records_split {
            let multiprogress_clone = multiprogress_bar.clone();
            let conn = pool.get_conn().unwrap();
            this_chunk = this_chunk + 1;
            //println!("Starting chunk {}/{}", this_chunk, tot_chunks);
            s.spawn(move |_| {
                let pb =
                    multiprogress_clone.add(ProgressBar::new(en_chunk.len().try_into().unwrap()));
                insert_en_rows_batch(conn, en_chunk, this_chunk, tot_chunks, &pb);
            });
        }
    });
    let _ = mp_clone.clear();
    println!("In Place Scope exited");

    //let _ = tx.commit();
    //let commit_result = tx.commit();
    //println!("commit_result: {:#?}", commit_result);
    return ();
}
// END ham_EN //

// BEGIN ham_HD //
fn insert_hd_rows_batch(
    mut conn: PooledConn,
    hd_records: Vec<data::ApplicationLicenseHeader>,
    _chunk_id: u32,
    _tot_chunks: usize,
    pb: &ProgressBar,
) {
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = tx.exec_batch(
        "INSERT INTO ham_HD (\
				`Record Type`, \
				`Unique System Identifier`, \
				`ULS File Number`, \
				`EBF Number`, \
				`Call Sign`, \
				`License Status`, \
				`Radio Service Code`, \
				`Grant Date`, \
				`Expired Date`, \
				`Cancellation Date`, \
				`Eligibility Rule Num`, \
				`Reserved`, \
				`Alien`, \
				`Alien Government`, \
				`Alien Corporation`, \
				`Alien Officer`, \
				`Alien Control`, \
				`Revoked`, \
				`Convicted`, \
				`Adjudged`, \
				`Involved Reserved`, \
				`Common Carrier`, \
				`Non Common Carrier`, \
				`Private Comm`, \
				`Fixed`, \
				`Mobile`, \
				`Radiolocation`, \
				`Satellite`, \
				`Developmental or STA or Demonstration`, \
				`Interconnected Service`, \
				`Certifier First Name`, \
				`Certifier MI`, \
				`Certifier Last Name`, \
				`Certifier Suffix`, \
				`Certifier Title`, \
				`Gender`, \
				`African American`, \
				`Native American`, \
				`Hawaiian`, \
				`Asian`, \
				`White`, \
				`Ethnicity`, \
				`Effective Date`, \
				`Last Action Date`, \
				`Auction ID`, \
				`Broadcast Services - Regulatory Status`, \
				`Band Manager`, \
				`Broadcast Services - Type of Radio Service`, \
				`Alien Ruling`, \
				`Licensee Name Change`, \
				`Whitespace Ind`, \
				`Additional Cert Choice`, \
				`Additional Cert Answer`, \
				`Discontinuation Ind`, \
				`Regulatory Compliance Ind`, \
				`Eligibility Cert 900`, \
				`Transition Plan Cert 900`, \
				`Return Spectrum Cert 900`, \
				`Payment Cert 900` \
				) \
				VALUES (\
				:record_type, \
				:unique_system_identifier, \
				:uls_file_num, \
				:ebf_number, \
				:call_sign, \
				:license_status, \
				:radio_service_code, \
				:grant_date, \
				:expired_date, \
				:cancellation_date, \
				:eligibility_rule_num, \
				:applicant_type_code_reserved, \
				:alien, \
				:alien_government, \
				:alien_corporation, \
				:alien_officer, \
				:alien_control, \
				:revoked, \
				:convicted, \
				:adjudged, \
				:involved_reserved, \
				:common_carrier, \
				:non_common_carrier, \
				:private_comm, \
				:fixed, \
				:mobile, \
				:radiolocation, \
				:satellite, \
				:developmental_or_sta, \
				:interconnected_service, \
				:certifier_first_name, \
				:certifier_mi, \
				:certifier_last_name, \
				:certifier_suffix, \
				:certifier_title, \
				:gender, \
				:african_american, \
				:native_american, \
				:hawaiian, \
				:asian, \
				:white, \
				:ethnicity, \
				:effective_date, \
				:last_action_date, \
				:auction_id, \
				:reg_stat_broad_serv, \
				:band_manager, \
				:type_serv_broad_serv, \
				:alien_ruling, \
				:licensee_name_change, \
				:whitespace_ind, \
				:additional_cert_choice, \
				:additional_cert_answer, \
				:discontinuation_ind, \
				:regulatory_compliance_ind, \
				:eligibility_cert_900, \
				:transition_plan_cert_900, \
				:return_spectrum_cert_900, \
				:payment_cert_900 \
				)",
        hd_records.iter().map(|p| {
            pb.inc(1);
            params! {
            "record_type" => p.record_type.clone(),
            "unique_system_identifier" => p.unique_system_identifier,
            "uls_file_num" => p.uls_file_num.clone(),
            "ebf_number" => p.ebf_number.clone(),
            "call_sign" => p.call_sign.clone(),
            "license_status" => p.license_status.clone(),
            "radio_service_code" => p.radio_service_code.clone(),
            "grant_date" => p.grant_date.clone(),
            "expired_date" => p.expired_date.clone(),
            "cancellation_date" => p.cancellation_date.clone(),
            "eligibility_rule_num" => p.eligibility_rule_num.clone(),
            "applicant_type_code_reserved" => p.applicant_type_code_reserved.clone(),
            "alien" => p.alien.clone(),
            "alien_government" => p.alien_government.clone(),
            "alien_corporation" => p.alien_corporation.clone(),
            "alien_officer" => p.alien_officer.clone(),
            "alien_control" => p.alien_control.clone(),
            "revoked" => p.revoked.clone(),
            "convicted" => p.convicted.clone(),
            "adjudged" => p.adjudged.clone(),
            "involved_reserved" => p.involved_reserved.clone(),
            "common_carrier" => p.common_carrier.clone(),
            "non_common_carrier" => p.non_common_carrier.clone(),
            "private_comm" => p.private_comm.clone(),
            "fixed" => p.fixed.clone(),
            "mobile" => p.mobile.clone(),
            "radiolocation" => p.radiolocation.clone(),
            "satellite" => p.satellite.clone(),
            "developmental_or_sta" => p.developmental_or_sta.clone(),
            "interconnected_service" => p.interconnected_service.clone(),
            "certifier_first_name" => p.certifier_first_name.clone(),
            "certifier_mi" => p.certifier_mi.clone(),
            "certifier_last_name" => p.certifier_last_name.clone(),
            "certifier_suffix" => p.certifier_suffix.clone(),
            "certifier_title" => p.certifier_title.clone(),
            "gender" => p.gender.clone(),
            "african_american" => p.african_american.clone(),
            "native_american" => p.native_american.clone(),
            "hawaiian" => p.hawaiian.clone(),
            "asian" => p.asian.clone(),
            "white" => p.white.clone(),
            "ethnicity" => p.ethnicity.clone(),
            "effective_date" => p.effective_date.clone(),
            "last_action_date" => p.last_action_date.clone(),
            "auction_id" => p.auction_id.clone(),
            "reg_stat_broad_serv" => p.reg_stat_broad_serv.clone(),
            "band_manager" => p.band_manager.clone(),
            "type_serv_broad_serv" => p.type_serv_broad_serv.clone(),
            "alien_ruling" => p.alien_ruling.clone(),
            "licensee_name_change" => p.licensee_name_change.clone(),
            "whitespace_ind" => p.whitespace_ind.clone(),
            "additional_cert_choice" => p.additional_cert_choice.clone(),
            "additional_cert_answer" => p.additional_cert_answer.clone(),
            "discontinuation_ind" => p.discontinuation_ind.clone(),
            "regulatory_compliance_ind" => p.regulatory_compliance_ind.clone(),
            "eligibility_cert_900" => p.eligibility_cert_900.clone(),
            "transition_plan_cert_900" => p.transition_plan_cert_900.clone(),
            "return_spectrum_cert_900" => p.return_spectrum_cert_900.clone(),
            "payment_cert_900" => p.payment_cert_900.clone(),
            }
        }),
    );
    let result_value = match result {
        Ok(_result_value) => {
            //println!("{:#?}", result_value);
            tx.commit().unwrap();
        }
        Err(result_value) => {
            println!("Error: {:#?}", result_value);
            tx.rollback().unwrap();
        }
    };
    pb.finish();
    //println!("Chunk {}/{} complete", chunk_id, tot_chunks);
    return result_value;
}

fn split_hd_rows(
    input_records: Vec<data::ApplicationLicenseHeader>,
) -> Vec<Vec<data::ApplicationLicenseHeader>> {
    let mut retval: Vec<Vec<data::ApplicationLicenseHeader>> = Vec::new();
    let mut i = 0;
    let mut n = 0;
    let split_modulus = input_records.len() / 10;
    for hd_row in input_records {
        if (n % split_modulus) == 0 {
            let mut this_vec: Vec<data::ApplicationLicenseHeader> = Vec::new();
            this_vec.push(hd_row);
            retval.push(this_vec);
            i = retval.len() - 1;
        } else {
            retval[i].push(hd_row);
        }
        n = n + 1;
    }
    return retval;
}

fn do_hd_drop(mut conn: mysql::PooledConn) -> mysql::Result<()> {
    let mut tx = conn.start_transaction(TxOpts::default()).unwrap();
    let result = tx.exec_drop("DELETE FROM ham_HD", ());
    tx.commit().unwrap();
    return result;
}

pub fn insert_hd_rows(sql_url: &str, hd_records: Vec<data::ApplicationLicenseHeader>) {
    let pool = Pool::new(sql_url).unwrap();

    println!("Splitting rows!");
    let hd_records_split = split_hd_rows(hd_records);
    println!("Rows split into {} chunks!", hd_records_split.len());

    println!("Dropping ham_HD!");
    do_hd_drop(pool.get_conn().unwrap()).unwrap();

    println!("Inserting rows!");
    let multiprogress_bar = Arc::new(MultiProgress::new());
    let tpool = ThreadPoolBuilder::new().num_threads(10).build().unwrap();
    let mut this_chunk = 0;
    let tot_chunks = hd_records_split.len();
    println!("Entering In Place Scope!");
    let mp_clone = multiprogress_bar.clone();
    tpool.in_place_scope(move |s| {
        for hd_chunk in hd_records_split {
            let multiprogress_clone = multiprogress_bar.clone();
            let conn = pool.get_conn().unwrap();
            this_chunk = this_chunk + 1;
            s.spawn(move |_| {
                let pb =
                    multiprogress_clone.add(ProgressBar::new(hd_chunk.len().try_into().unwrap()));
                insert_hd_rows_batch(conn, hd_chunk, this_chunk, tot_chunks, &pb);
            });
        }
    });
    let _ = mp_clone.clear();
    println!("In Place Scope exited");

    //let _ = tx.commit();
    //let commit_result = tx.commit();
    //println!("commit_result: {:#?}", commit_result);
    return ();
}
// END ham_HD //
