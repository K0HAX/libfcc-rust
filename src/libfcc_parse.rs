use crate::libfcc_data;

pub fn parse_am_line(line: String) -> libfcc_data::Amateur {
    let split: Vec<&str> = line.split("|").collect();
    let unique_system_identifier: u32 = split[1]
        .trim()
        .parse()
        .expect("Unique System Identifier is not a number!");
    let region_code_result = split[7].trim().parse::<u64>();
    let region_code = match region_code_result {
        Ok(value) => libfcc_data::U64Null::Value(value),
        Err(_error) => libfcc_data::U64Null::NULL,
    };
    let operator_class = libfcc_data::OperatorClass::from(split[5]);
    libfcc_data::Amateur {
        record_type: String::from(split[0]),
        unique_system_identifier: unique_system_identifier,
        uls_file_num: String::from(split[2]),
        ebf_number: String::from(split[3]),
        callsign: String::from(split[4]),
        operator_class: operator_class,
        group_code: String::from(split[6]),
        region_code: region_code,
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

pub fn parse_en_line(line: String) -> libfcc_data::Entity {
    let split: Vec<&str> = line.split("|").collect();
    let unique_system_identifier: u32 = split[1]
        .trim()
        .parse()
        .expect("Unique System Identifier is not a number!");
    libfcc_data::Entity {
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

pub fn parse_hd_line(line: String) -> libfcc_data::ApplicationLicenseHeader {
    let split: Vec<&str> = line.split("|").collect();
    let unique_system_identifier: u32 = split[1]
        .trim()
        .parse()
        .expect("Unique System Identifier is not a number!");
    let license_status: libfcc_data::LicenseStatus = libfcc_data::LicenseStatus::from(split[5]);
    let developmental_or_sta: libfcc_data::DevelopmentalStaDemonstration =
        libfcc_data::DevelopmentalStaDemonstration::from(split[28]);
    let auction_id_result = split[44].trim().parse::<u64>();
    let auction_id = match auction_id_result {
        Ok(value) => libfcc_data::U64Null::Value(value),
        Err(_error) => libfcc_data::U64Null::NULL,
    };
    libfcc_data::ApplicationLicenseHeader {
        record_type: String::from(split[0]),
        unique_system_identifier: unique_system_identifier,
        uls_file_num: String::from(split[2]),
        ebf_number: String::from(split[3]),
        call_sign: String::from(split[4]),
        license_status: license_status,
        radio_service_code: String::from(split[6]),
        grant_date: String::from(split[7]),
        expired_date: String::from(split[8]),
        cancellation_date: String::from(split[9]),
        eligibility_rule_num: String::from(split[10]),
        applicant_type_code_reserved: String::from(split[11]),
        alien: String::from(split[12]),
        alien_government: String::from(split[13]),
        alien_corporation: String::from(split[14]),
        alien_officer: String::from(split[15]),
        alien_control: String::from(split[16]),
        revoked: String::from(split[17]),
        convicted: String::from(split[18]),
        adjudged: String::from(split[19]),
        involved_reserved: String::from(split[20]),
        common_carrier: String::from(split[21]),
        non_common_carrier: String::from(split[22]),
        private_comm: String::from(split[23]),
        fixed: String::from(split[24]),
        mobile: String::from(split[25]),
        radiolocation: String::from(split[26]),
        satellite: String::from(split[27]),
        developmental_or_sta: developmental_or_sta,
        interconnected_service: String::from(split[29]),
        certifier_first_name: String::from(split[30]),
        certifier_mi: String::from(split[31]),
        certifier_last_name: String::from(split[32]),
        certifier_suffix: String::from(split[33]),
        certifier_title: String::from(split[34]),
        gender: String::from(split[35]),
        african_american: String::from(split[36]),
        native_american: String::from(split[37]),
        hawaiian: String::from(split[38]),
        asian: String::from(split[39]),
        white: String::from(split[40]),
        ethnicity: String::from(split[41]),
        effective_date: String::from(split[42]),
        last_action_date: String::from(split[43]),
        auction_id: auction_id,
        reg_stat_broad_serv: String::from(split[45]),
        band_manager: String::from(split[46]),
        type_serv_broad_serv: String::from(split[47]),
        alien_ruling: String::from(split[48]),
        licensee_name_change: String::from(split[49]),
        whitespace_ind: String::from(split[50]),
        additional_cert_choice: String::from(split[51]),
        additional_cert_answer: String::from(split[52]),
        discontinuation_ind: String::from(split[53]),
        regulatory_compliance_ind: String::from(split[54]),
        eligibility_cert_900: String::from(split[55]),
        transition_plan_cert_900: String::from(split[56]),
        return_spectrum_cert_900: String::from(split[57]),
        payment_cert_900: String::from(split[58]),
    }
}
