package npi

import (
	"fmt"
	"github.com/dimfeld/bloomdb"
	"github.com/dimfeld/bloomnpi/csvHeaderReader"
	"io"
	"strconv"
	"sync"
)

type tableDesc struct {
	name     string
	channel  chan []string
	idColumn string
	columns  []string
}

func Upsert(file io.ReadCloser, file_id string) {
	var wg sync.WaitGroup

	npis := make(chan []string, 100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := csvHeaderReader.NewReader(file)

		for {
			row, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("Error: ", err)
				return
			}

			list_size := 331
			npi_values := make([]string, list_size)
			npi_values[0] = row.Value("NPI")
			npi_values[1] = row.Value("Entity Type Code")
			npi_values[2] = row.Value("Replacement NPI")
			npi_values[3] = row.Value("Employer Identification Number (EIN)")
			npi_values[4] = row.Value("Provider Organization Name (Legal Business Name)")
			npi_values[5] = row.Value("Provider Last Name (Legal Name)")
			npi_values[6] = row.Value("Provider First Name")
			npi_values[7] = row.Value("Provider Middle Name")
			npi_values[8] = row.Value("Provider Name Prefix Text")
			npi_values[9] = row.Value("Provider Name Suffix Text")
			npi_values[10] = row.Value("Provider Credential Text")
			npi_values[11] = row.Value("Provider Other Organization Name")
			npi_values[12] = row.Value("Provider Other Organization Name Type Code")
			npi_values[13] = row.Value("Provider Other Last Name")
			npi_values[14] = row.Value("Provider Other First Name")
			npi_values[15] = row.Value("Provider Other Middle Name")
			npi_values[16] = row.Value("Provider Other Name Prefix Text")
			npi_values[17] = row.Value("Provider Other Name Suffix Text")
			npi_values[18] = row.Value("Provider Other Credential Text")
			npi_values[19] = row.Value("Provider Other Last Name Type Code")

			business_zip := row.Value("Provider Business Mailing Address Postal Code")
			if business_zip != "" {
				business_address := row.Value("Provider First Line Business Mailing Address")
				business_details := row.Value("Provider Second Line Business Mailing Address")
				business_city := row.Value("Provider Business Mailing Address City Name")
				business_state := row.Value("Provider Business Mailing Address State Name")
				business_country := row.Value("Provider Business Mailing Address Country Code (If outside U.S.)")
				business_phone := row.Value("Provider Business Mailing Address Telephone Number")
				business_fax := row.Value("Provider Business Mailing Address Fax Number")

				npi_values[20] = business_address
				npi_values[21] = business_details
				npi_values[22] = business_city
				npi_values[23] = business_state
				npi_values[24] = business_zip
				npi_values[25] = business_country
				npi_values[26] = business_phone
				npi_values[27] = business_fax
			}

			practice_zip := row.Value("Provider Business Practice Location Address Postal Code")
			if practice_zip != "" {
				practice_address := row.Value("Provider First Line Business Practice Location Address")
				practice_details := row.Value("Provider Second Line Business Practice Location Address")
				practice_city := row.Value("Provider Business Practice Location Address City Name")
				practice_state := row.Value("Provider Business Practice Location Address State Name")
				practice_country := row.Value("Provider Business Practice Location Address Country Code (If outside U.S.)")
				practice_phone := row.Value("Provider Business Practice Location Address Telephone Number")
				practice_fax := row.Value("Provider Business Practice Location Address Fax Number")

				npi_values[28] = practice_address
				npi_values[29] = practice_details
				npi_values[30] = practice_city
				npi_values[31] = practice_state
				npi_values[31] = practice_zip
				npi_values[32] = practice_country
				npi_values[34] = practice_phone
				npi_values[35] = practice_fax
			}

			npi_values[36] = row.Value("Provider Enumeration Date")
			npi_values[37] = row.Value("Last Update Date")
			npi_values[38] = row.Value("NPI Deactivation Reason Code")
			npi_values[39] = row.Value("NPI Deactivation Date")
			npi_values[40] = row.Value("NPI Reactivation Date")
			npi_values[41] = row.Value("Provider Gender Code")

			official_last_name := row.Value("Authorized Official Last Name")
			if official_last_name != "" {
				first_name := row.Value("Authorized Official First Name")
				middle_name := row.Value("Authorized Official Middle Name")
				title := row.Value("Authorized Official Title or Position")
				telephone_number := row.Value("Authorized Official Telephone Number")
				name_prefix := row.Value("Authorized Official Name Prefix Text")
				name_suffix := row.Value("Authorized Official Name Suffix Text")
				credential := row.Value("Authorized Official Credential Text")

				npi_values[42] = official_last_name
				npi_values[43] = first_name
				npi_values[44] = middle_name
				npi_values[45] = title
				npi_values[46] = telephone_number
				npi_values[47] = name_prefix
				npi_values[48] = name_suffix
				npi_values[49] = credential
			}

			npi_values[50] = row.Value("Is Sole Proprietor")
			npi_values[51] = row.Value("Is Organization Subpart")
			parent_business_name := row.Value("Parent Organization LBN")
			if parent_business_name != "" {
				tax_identification_number := row.Value("Parent Organization TIN")
				npi_values[52] = parent_business_name
				npi_values[53] = tax_identification_number
			}

			taxonomyIndex := 54
			for i := 1; i <= 15; i++ {
				taxonomyCode := row.Value("Healthcare Provider Taxonomy Code_" + strconv.Itoa(i))
				taxonomySwitch := row.Value("Healthcare Provider Primary Taxonomy Switch_" + strconv.Itoa(i))

				if taxonomyCode != "" {
					npi_values[taxonomyIndex] = taxonomyCode
					npi_values[taxonomyIndex+1] = taxonomySwitch
				}
				taxonomyIndex += 2
			}

			for i := 1; i <= 15; i++ {
				taxonomy := row.Value("Healthcare Provider Taxonomy Group_" + strconv.Itoa(i))

				if taxonomy != "" {
					npi_values[taxonomyIndex] = taxonomy
				}
				taxonomyIndex += 1
			}

			for i := 1; i <= 15; i++ {
				licenseNumber := row.Value("Provider License Number_" + strconv.Itoa(i))
				if licenseNumber != "" {
					licenseState := row.Value("Provider License Number State Code_" + strconv.Itoa(i))
					npi_values[taxonomyIndex] = licenseNumber
					npi_values[taxonomyIndex+1] = licenseState
				}
				taxonomyIndex += 2
			}

			for i := 1; i <= 50; i++ {
				identifier := row.Value("Other Provider Identifier_" + strconv.Itoa(i))
				if identifier != "" {
					idType := row.Value("Other Provider Identifier Type Code_" + strconv.Itoa(i))
					state := row.Value("Other Provider Identifier State_" + strconv.Itoa(i))
					issuer := row.Value("Other Provider Identifier Issuer_" + strconv.Itoa(i))

					npi_values[taxonomyIndex] = identifier
					npi_values[taxonomyIndex+1] = idType
					npi_values[taxonomyIndex+2] = state
					npi_values[taxonomyIndex+3] = issuer
				}
				taxonomyIndex += 4
			}

			if taxonomyIndex != list_size {
				// Make sure we're not getting out of sync.
				panic(fmt.Sprintf("Taxonomy size was %d, expected %d", taxonomyIndex, list_size))
			}

			npis <- npi_values
		}

		close(npis)
	}()

	dests := []tableDesc{
		tableDesc{
			name:     "npis",
			channel:  npis,
			idColumn: "npi",
			columns: []string{
				"npi",
				"entity_type_code",
				"replacement_npi",
				"employer_identification_number_ein",
				"provider_organization_name_legal_business_name",
				"provider_last_name_legal_name",
				"provider_first_name",
				"provider_middle_name",
				"provider_name_prefix_text",
				"provider_name_suffix_text",
				"provider_credential_text",
				"provider_other_organization_name",
				"provider_other_organization_name_type_code",
				"provider_other_last_name",
				"provider_other_first_name",
				"provider_other_middle_name",
				"provider_other_name_prefix_text",
				"provider_other_name_suffix_text",
				"provider_other_credential_text",
				"provider_other_last_name_type_code",
				"provider_first_line_business_mailing_address",
				"provider_second_line_business_mailing_address",
				"provider_business_mailing_address_city_name",
				"provider_business_mailing_address_state_name",
				"provider_business_mailing_address_postal_code",
				"provider_business_mailing_address_country_code_if_outside_u_s",
				"provider_business_mailing_address_telephone_number",
				"provider_business_mailing_address_fax_number",
				"provider_first_line_business_practice_location_address",
				"provider_second_line_business_practice_location_address",
				"provider_business_practice_location_address_city_name",
				"provider_business_practice_location_address_state_name",
				"provider_business_practice_location_address_postal_code",
				"provider_business_practice_location_address_country_code_if_out",
				"provider_business_practice_location_address_telephone_number",
				"provider_business_practice_location_address_fax_number",
				"provider_enumeration_date",
				"last_update_date",
				"npi_deactivation_reason_code",
				"npi_deactivation_date",
				"npi_reactivation_date",
				"provider_gender_code",
				"authorized_official_last_name",
				"authorized_official_first_name",
				"authorized_official_middle_name",
				"authorized_official_title_or_position",
				"authorized_official_telephone_number",
				"authorized_official_name_prefix_text",
				"authorized_official_name_suffix_text",
				"authorized_official_credential_text",
				"is_sole_proprietor",
				"is_organization_subpart",
				"parent_organization_lbn",
				"parent_organization_tin",
				"healthcare_provider_taxonomy_code_1",
				"healthcare_provider_primary_taxonomy_switch_1",
				"healthcare_provider_taxonomy_code_2",
				"healthcare_provider_primary_taxonomy_switch_2",
				"healthcare_provider_taxonomy_code_3",
				"healthcare_provider_primary_taxonomy_switch_3",
				"healthcare_provider_taxonomy_code_4",
				"healthcare_provider_primary_taxonomy_switch_4",
				"healthcare_provider_taxonomy_code_5",
				"healthcare_provider_primary_taxonomy_switch_5",
				"healthcare_provider_taxonomy_code_6",
				"healthcare_provider_primary_taxonomy_switch_6",
				"healthcare_provider_taxonomy_code_7",
				"healthcare_provider_primary_taxonomy_switch_7",
				"healthcare_provider_taxonomy_code_8",
				"healthcare_provider_primary_taxonomy_switch_8",
				"healthcare_provider_taxonomy_code_9",
				"healthcare_provider_primary_taxonomy_switch_9",
				"healthcare_provider_taxonomy_code_10",
				"healthcare_provider_primary_taxonomy_switch_10",
				"healthcare_provider_taxonomy_code_11",
				"healthcare_provider_primary_taxonomy_switch_11",
				"healthcare_provider_taxonomy_code_12",
				"healthcare_provider_primary_taxonomy_switch_12",
				"healthcare_provider_taxonomy_code_13",
				"healthcare_provider_primary_taxonomy_switch_13",
				"healthcare_provider_taxonomy_code_14",
				"healthcare_provider_primary_taxonomy_switch_14",
				"healthcare_provider_taxonomy_code_15",
				"healthcare_provider_primary_taxonomy_switch_15",
				"healthcare_provider_taxonomy_group_1",
				"healthcare_provider_taxonomy_group_2",
				"healthcare_provider_taxonomy_group_3",
				"healthcare_provider_taxonomy_group_4",
				"healthcare_provider_taxonomy_group_5",
				"healthcare_provider_taxonomy_group_6",
				"healthcare_provider_taxonomy_group_7",
				"healthcare_provider_taxonomy_group_8",
				"healthcare_provider_taxonomy_group_9",
				"healthcare_provider_taxonomy_group_10",
				"healthcare_provider_taxonomy_group_11",
				"healthcare_provider_taxonomy_group_12",
				"healthcare_provider_taxonomy_group_13",
				"healthcare_provider_taxonomy_group_14",
				"healthcare_provider_taxonomy_group_15",
				"provider_license_number_1",
				"provider_license_number_state_code_1",
				"provider_license_number_2",
				"provider_license_number_state_code_2",
				"provider_license_number_3",
				"provider_license_number_state_code_3",
				"provider_license_number_4",
				"provider_license_number_state_code_4",
				"provider_license_number_5",
				"provider_license_number_state_code_5",
				"provider_license_number_6",
				"provider_license_number_state_code_6",
				"provider_license_number_7",
				"provider_license_number_state_code_7",
				"provider_license_number_8",
				"provider_license_number_state_code_8",
				"provider_license_number_9",
				"provider_license_number_state_code_9",
				"provider_license_number_10",
				"provider_license_number_state_code_10",
				"provider_license_number_11",
				"provider_license_number_state_code_11",
				"provider_license_number_12",
				"provider_license_number_state_code_12",
				"provider_license_number_13",
				"provider_license_number_state_code_13",
				"provider_license_number_14",
				"provider_license_number_state_code_14",
				"provider_license_number_15",
				"provider_license_number_state_code_15",
				"other_provider_identifier_1",
				"other_provider_identifier_type_code_1",
				"other_provider_identifier_state_1",
				"other_provider_identifier_issuer_1",
				"other_provider_identifier_2",
				"other_provider_identifier_type_code_2",
				"other_provider_identifier_state_2",
				"other_provider_identifier_issuer_2",
				"other_provider_identifier_3",
				"other_provider_identifier_type_code_3",
				"other_provider_identifier_state_3",
				"other_provider_identifier_issuer_3",
				"other_provider_identifier_4",
				"other_provider_identifier_type_code_4",
				"other_provider_identifier_state_4",
				"other_provider_identifier_issuer_4",
				"other_provider_identifier_5",
				"other_provider_identifier_type_code_5",
				"other_provider_identifier_state_5",
				"other_provider_identifier_issuer_5",
				"other_provider_identifier_6",
				"other_provider_identifier_type_code_6",
				"other_provider_identifier_state_6",
				"other_provider_identifier_issuer_6",
				"other_provider_identifier_7",
				"other_provider_identifier_type_code_7",
				"other_provider_identifier_state_7",
				"other_provider_identifier_issuer_7",
				"other_provider_identifier_8",
				"other_provider_identifier_type_code_8",
				"other_provider_identifier_state_8",
				"other_provider_identifier_issuer_8",
				"other_provider_identifier_9",
				"other_provider_identifier_type_code_9",
				"other_provider_identifier_state_9",
				"other_provider_identifier_issuer_9",
				"other_provider_identifier_10",
				"other_provider_identifier_type_code_10",
				"other_provider_identifier_state_10",
				"other_provider_identifier_issuer_10",
				"other_provider_identifier_11",
				"other_provider_identifier_type_code_11",
				"other_provider_identifier_state_11",
				"other_provider_identifier_issuer_11",
				"other_provider_identifier_12",
				"other_provider_identifier_type_code_12",
				"other_provider_identifier_state_12",
				"other_provider_identifier_issuer_12",
				"other_provider_identifier_13",
				"other_provider_identifier_type_code_13",
				"other_provider_identifier_state_13",
				"other_provider_identifier_issuer_13",
				"other_provider_identifier_14",
				"other_provider_identifier_type_code_14",
				"other_provider_identifier_state_14",
				"other_provider_identifier_issuer_14",
				"other_provider_identifier_15",
				"other_provider_identifier_type_code_15",
				"other_provider_identifier_state_15",
				"other_provider_identifier_issuer_15",
				"other_provider_identifier_16",
				"other_provider_identifier_type_code_16",
				"other_provider_identifier_state_16",
				"other_provider_identifier_issuer_16",
				"other_provider_identifier_17",
				"other_provider_identifier_type_code_17",
				"other_provider_identifier_state_17",
				"other_provider_identifier_issuer_17",
				"other_provider_identifier_18",
				"other_provider_identifier_type_code_18",
				"other_provider_identifier_state_18",
				"other_provider_identifier_issuer_18",
				"other_provider_identifier_19",
				"other_provider_identifier_type_code_19",
				"other_provider_identifier_state_19",
				"other_provider_identifier_issuer_19",
				"other_provider_identifier_20",
				"other_provider_identifier_type_code_20",
				"other_provider_identifier_state_20",
				"other_provider_identifier_issuer_20",
				"other_provider_identifier_21",
				"other_provider_identifier_type_code_21",
				"other_provider_identifier_state_21",
				"other_provider_identifier_issuer_21",
				"other_provider_identifier_22",
				"other_provider_identifier_type_code_22",
				"other_provider_identifier_state_22",
				"other_provider_identifier_issuer_22",
				"other_provider_identifier_23",
				"other_provider_identifier_type_code_23",
				"other_provider_identifier_state_23",
				"other_provider_identifier_issuer_23",
				"other_provider_identifier_24",
				"other_provider_identifier_type_code_24",
				"other_provider_identifier_state_24",
				"other_provider_identifier_issuer_24",
				"other_provider_identifier_25",
				"other_provider_identifier_type_code_25",
				"other_provider_identifier_state_25",
				"other_provider_identifier_issuer_25",
				"other_provider_identifier_26",
				"other_provider_identifier_type_code_26",
				"other_provider_identifier_state_26",
				"other_provider_identifier_issuer_26",
				"other_provider_identifier_27",
				"other_provider_identifier_type_code_27",
				"other_provider_identifier_state_27",
				"other_provider_identifier_issuer_27",
				"other_provider_identifier_28",
				"other_provider_identifier_type_code_28",
				"other_provider_identifier_state_28",
				"other_provider_identifier_issuer_28",
				"other_provider_identifier_29",
				"other_provider_identifier_type_code_29",
				"other_provider_identifier_state_29",
				"other_provider_identifier_issuer_29",
				"other_provider_identifier_30",
				"other_provider_identifier_type_code_30",
				"other_provider_identifier_state_30",
				"other_provider_identifier_issuer_30",
				"other_provider_identifier_31",
				"other_provider_identifier_type_code_31",
				"other_provider_identifier_state_31",
				"other_provider_identifier_issuer_31",
				"other_provider_identifier_32",
				"other_provider_identifier_type_code_32",
				"other_provider_identifier_state_32",
				"other_provider_identifier_issuer_32",
				"other_provider_identifier_33",
				"other_provider_identifier_type_code_33",
				"other_provider_identifier_state_33",
				"other_provider_identifier_issuer_33",
				"other_provider_identifier_34",
				"other_provider_identifier_type_code_34",
				"other_provider_identifier_state_34",
				"other_provider_identifier_issuer_34",
				"other_provider_identifier_35",
				"other_provider_identifier_type_code_35",
				"other_provider_identifier_state_35",
				"other_provider_identifier_issuer_35",
				"other_provider_identifier_36",
				"other_provider_identifier_type_code_36",
				"other_provider_identifier_state_36",
				"other_provider_identifier_issuer_36",
				"other_provider_identifier_37",
				"other_provider_identifier_type_code_37",
				"other_provider_identifier_state_37",
				"other_provider_identifier_issuer_37",
				"other_provider_identifier_38",
				"other_provider_identifier_type_code_38",
				"other_provider_identifier_state_38",
				"other_provider_identifier_issuer_38",
				"other_provider_identifier_39",
				"other_provider_identifier_type_code_39",
				"other_provider_identifier_state_39",
				"other_provider_identifier_issuer_39",
				"other_provider_identifier_40",
				"other_provider_identifier_type_code_40",
				"other_provider_identifier_state_40",
				"other_provider_identifier_issuer_40",
				"other_provider_identifier_41",
				"other_provider_identifier_type_code_41",
				"other_provider_identifier_state_41",
				"other_provider_identifier_issuer_41",
				"other_provider_identifier_42",
				"other_provider_identifier_type_code_42",
				"other_provider_identifier_state_42",
				"other_provider_identifier_issuer_42",
				"other_provider_identifier_43",
				"other_provider_identifier_type_code_43",
				"other_provider_identifier_state_43",
				"other_provider_identifier_issuer_43",
				"other_provider_identifier_44",
				"other_provider_identifier_type_code_44",
				"other_provider_identifier_state_44",
				"other_provider_identifier_issuer_44",
				"other_provider_identifier_45",
				"other_provider_identifier_type_code_45",
				"other_provider_identifier_state_45",
				"other_provider_identifier_issuer_45",
				"other_provider_identifier_46",
				"other_provider_identifier_type_code_46",
				"other_provider_identifier_state_46",
				"other_provider_identifier_issuer_46",
				"other_provider_identifier_47",
				"other_provider_identifier_type_code_47",
				"other_provider_identifier_state_47",
				"other_provider_identifier_issuer_47",
				"other_provider_identifier_48",
				"other_provider_identifier_type_code_48",
				"other_provider_identifier_state_48",
				"other_provider_identifier_issuer_48",
				"other_provider_identifier_49",
				"other_provider_identifier_type_code_49",
				"other_provider_identifier_state_49",
				"other_provider_identifier_issuer_49",
				"other_provider_identifier_50",
				"other_provider_identifier_type_code_50",
				"other_provider_identifier_state_50",
				"other_provider_identifier_issuer_50",
			},
		},
	}

	bdb := bloomdb.CreateDB()
	for _, dest := range dests {
		wg.Add(1)
		go func(dest tableDesc) {
			defer wg.Done()

			db, err := bdb.SqlConnection()
			if err != nil {
				fmt.Println("Error:", err)
				return
			}

			err = bloomdb.Upsert(db, "bloom."+dest.name, dest.idColumn, dest.columns,
				dest.channel)
			if err != nil {
				fmt.Println("Error:", err)
				return
			}
		}(dest)
	}

	wg.Wait()
}
