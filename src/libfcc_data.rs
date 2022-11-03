use std::convert::{From};
use serde::{Serialize, Deserialize};

// Begin Enums
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum U64Null {
    Value(u64),
    NULL
}

// BEGIN AM
#[derive(Serialize, Deserialize, Clone)]
pub enum OperatorClass {
    Advanced,
    AmateurExtra,
    General,
    Novice,
    TechnicianPlus,
    Technician,
    Unknown,
}

impl From<&str> for OperatorClass {
    fn from(operator_class: &str) -> Self {
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
}
// END AM

// BEGIN EN
#[derive(Serialize, Deserialize, Clone)]
pub enum EntityType {
    TransfereeContact,
    LicenseeContact,
    AssignorOrTransferorContact,
    LesseeContact,
    Transferee,
    LicenseeOrAssignee,
    Owner,
    AssignorOrTransferor,
    Lessee,
    Unknown,
}

impl From<&str> for EntityType {
    fn from(entity_type: &str) -> Self {
	match entity_type {
	    "CE" => EntityType::TransfereeContact,
	    "CL" => EntityType::LicenseeContact,
	    "CR" => EntityType::AssignorOrTransferorContact,
	    "CS" => EntityType::LesseeContact,
	    "E" => EntityType::Transferee,
	    "L" => EntityType::LicenseeOrAssignee,
	    "O" => EntityType::Owner,
	    "R" =>  EntityType::AssignorOrTransferor,
	    "S" => EntityType::Lessee,
	    _ => EntityType::Unknown,
	}
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum ApplicantTypeCode {
    AmateurClub,
    Corporation,
    GeneralPartnership,
    LimitedPartnership,
    LimitedLiabilityPartnership,
    GovernmentalEntity,
    Other,
    Individual,
    JointVenture,
    LimitedLiabilityCompany,
    MilitaryRecreation,
    Consortium,
    Partnership,
    RACES,
    Trust,
    UnincorporatedAssociation,
    Unknown,
}

impl From<&str> for ApplicantTypeCode {
    fn from(applicant_type_code: &str) -> Self {
	match applicant_type_code {
	    "B" => ApplicantTypeCode::AmateurClub,
	    "C" => ApplicantTypeCode::Corporation,
	    "D" => ApplicantTypeCode::GeneralPartnership,
	    "E" => ApplicantTypeCode::LimitedPartnership,
	    "F" => ApplicantTypeCode::LimitedLiabilityPartnership,
	    "G" => ApplicantTypeCode::GovernmentalEntity,
	    "H" => ApplicantTypeCode::Other,
	    "I" => ApplicantTypeCode::Individual,
	    "J" => ApplicantTypeCode::JointVenture,
	    "L" => ApplicantTypeCode::LimitedLiabilityCompany,
	    "M" => ApplicantTypeCode::MilitaryRecreation,
	    "O" => ApplicantTypeCode::Consortium,
	    "P" => ApplicantTypeCode::Partnership,
	    "R" => ApplicantTypeCode::RACES,
	    "T" => ApplicantTypeCode::Trust,
	    "U" => ApplicantTypeCode::UnincorporatedAssociation,
	    _ => ApplicantTypeCode::Unknown,
	}
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum EnStatusCode {
    Active,
    TerminationPending,
    Terminated,
}

impl From<&str> for EnStatusCode {
    fn from(status_code: &str) -> Self {
	match status_code {
	    "X" => EnStatusCode::TerminationPending,
	    "T" => EnStatusCode::Terminated,
	    _ => EnStatusCode::Active,
	}
    }
}

// END EN

// BEGIN HD
#[derive(Serialize, Deserialize, Clone)]
pub enum LicenseStatus {
    Active,
    Cancelled,
    Expired,
    PendingLegalStatus,
    ParentStationCanceled,
    Terminated,
    TermPending,
    Unknown,
}

impl From<&str> for LicenseStatus {
    fn from(license_status: &str) -> Self {
	match license_status {
	    "A" => LicenseStatus::Active,
	    "C" => LicenseStatus::Cancelled,
	    "E" => LicenseStatus::Expired,
	    "L" => LicenseStatus::PendingLegalStatus,
	    "P" => LicenseStatus::ParentStationCanceled,
	    "T" => LicenseStatus::Terminated,
	    "X" => LicenseStatus::TermPending,
	    _ => LicenseStatus::Unknown,
	}
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum DevelopmentalStaDemonstration {
    Developmental,
    Demonstration,
    Regular,
    SpecialTemporaryAuthority,
    Unknown,
}

impl From<&str> for DevelopmentalStaDemonstration {
    fn from (developmental_or_sta: &str) -> Self {
	match developmental_or_sta {
	    "D" => DevelopmentalStaDemonstration::Developmental,
	    "M" => DevelopmentalStaDemonstration::Demonstration,
	    "N" => DevelopmentalStaDemonstration::Regular,
	    "S" => DevelopmentalStaDemonstration::SpecialTemporaryAuthority,
	    _ => DevelopmentalStaDemonstration::Unknown,
	}
    }
}
// END HD

// AM.dat
#[derive(Serialize, Deserialize, Clone)]
pub struct Amateur {
    pub record_type: String,
    pub unique_system_identifier: u32,
    pub uls_file_num: String,
    pub ebf_number: String,
    pub callsign: String,
    pub operator_class: OperatorClass,
    pub group_code: String,
    pub region_code: U64Null,
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
}

// EN.dat
#[derive(Serialize, Deserialize, Clone)]
pub struct Entity {
    pub record_type: String,
    pub unique_system_identifier: u32,
    pub uls_file_num: String,
    pub ebf_number: String,
    pub call_sign: String,
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
}

// HD.dat
// Application License/Header
#[derive(Serialize, Deserialize, Clone)]
pub struct ApplicationLicenseHeader {
    pub record_type: String,
    pub unique_system_identifier: u32,
    pub uls_file_num: String,
    pub ebf_number: String,
    pub call_sign: String,
    pub license_status: LicenseStatus,
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
    pub developmental_or_sta: DevelopmentalStaDemonstration,
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
    pub auction_id: U64Null,
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
