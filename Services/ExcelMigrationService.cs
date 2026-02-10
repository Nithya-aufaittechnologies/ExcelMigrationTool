using ExcelMigrationTool.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.Text;

namespace ExcelMigrationTool.Services;

public class ExcelMigrationService : IExcelMigrationService
{
    // SQL Command timeout in seconds (10 minutes for large datasets)
    private const int SqlCommandTimeout = 600;

    // Unit ID columns that should be resolved via UnitMaster by unit name
    private static readonly HashSet<string> UnitIdColumns = new(StringComparer.OrdinalIgnoreCase)
    {
        "AmbientTemperatureUnitID",
        "TemperatureRiseDeltaTUnitID",
        "ElectricalDesignUnitID",
        "InstrumentAirPressureUnitID",
        "CWSupplyTemperatureUnitID",
        "CWSupplyPressureUnitID",
        "DesignPressureUnitID",
        "PressureDropUnitID",
        "ExhaustPressureUnitID",
        "PressureUnitID"
    };


    public static readonly Dictionary<string, string> ProjectMapping = new(StringComparer.OrdinalIgnoreCase)
        {

            { "k__uuu_shell_creator", "ProjectID" },
            { "record_no", "RecordNo" },

            { "uuu_record_last_update_date", "UpdatedAt" },
            { "uuu_record_last_update_user", "UpdatedName" },
            { "k__uuu_record_last_update_user", "UpdatedBy" },

            { "process_status", "ProcessStatus" },
            { "status", "Status" },

            { "creator_id", "CreatedName" },
            { "k__creator_id", "CreatedBy" },

            { "uot_c_number_sdt120", "CNumber" },

            { "ucp_pm_smn", "ManagerName" },
            { "k__ci_project_manager_upk", "ManagerID" },

            { "description", "Description" },

            { "uuu_shell_template_picker", "ProjectTemplateID" },
            { "uuu_shell_location", "ProjectTypeMasterID" },

            { "ugenprojectname", "ProjectName" },

            { "uuu_creation_date", "CreatedAt" }
        };

    // Hardcoded column mapping for CommunicationProtocol table
    private static readonly Dictionary<string, string> CommunicationProtocolColumnMapping = new(StringComparer.OrdinalIgnoreCase)
    {

{ "id", "CommunicationProtocolID" },
{ "record_no", "RecordNo" },

{ "uuu_record_last_update_date", "UpdatedAt" },
{ "uuu_record_last_update_user", "UpdatedName" },
{ "k__uuu_record_last_update_user", "UpdatedBy" },

{ "process_status", "ProcessStatus" },
{ "status", "Status" },
{ "creator_id", "CreatedName" },
{ "k__creator_id", "CreatedBy" },

{ "project_id", "ProjectID" },

{ "uot_copies_to_be_sent_to_dp", "ICHeadName" },
{ "k__uot_copies_to_be_sent_to_dp", "ICHeadID" },

{ "phone_number11", "ICHeadPhoneNumber" },

{ "email_id11", "ICHeadEmail" },
{ "uot_project_lead_dp", "ProjectLeaderName" },
{ "k__uot_project_lead_dp", "ProjectLeaderID" },
{ "phone_number51", "ProjectLeaderPhoneNumber" },
{ "email_id51", "ProjectLeaderEmail" },
{ "mobile_number_headofdept", "HODMobileNumber" },
{ "mobile_number_headspares", "HOSSMobileNumber" },
{ "extention31", "ProjectManagerExtension" },
{ "uot_pur_countrypd", "Country" },
{ "uuu_user_fax", "Address_Fax" },
{ "postal_address", "PostalAddress" },
{ "uot_project_co_ordinator_dp", "ProjectManagerName" },
{ "k__uot_project_co_ordinator_dp", "ProjectManagerID" },
{ "uuu_creation_date", "CreatedAt" },
{ "phone_number21", "HOSSPhoneNumber" },
{ "uot_phone1_sdt50", "Phone" },
{ "phone_number61", "ZonalHeadPhoneNumber" },
{ "uot_purchase_add3_sdt120", "Address3" },
{ "phone_number0", "HODPhoneNumber" },
{ "uot_email1tb120", "Email" },
{ "ucpt_kindly_attn_sdt120", "KindlyAttn" },
{ "uot_mails_to_be_sent_to_dp", "ZonalHeadName" },
{ "k__uot_mails_to_be_sent_to_dp", "ZonalHeadID" },
{ "email_id61", "ZonalHeadEmail" },
{ "mobile_number0", "InchargeMobileNumber" },
{ "cp_email_id_dc", "DocumentControllerEmail" },
{ "mobile_number21", "ProjectLeaderMobileNumber" },
{ "uot_pur_citytxt50", "City" },
{ "extention41", "InchargeExtension" },
{ "phone_number31", "ProjectManagerPhoneNumber" },
{ "ucp_pm_smn", "ProjectManagerName.1" },
{ "email_id31", "ProjectManagerEmail" },
{ "uot_state", "StateProvinceOtherThanIndia" },
{ "uot_india_states_pd", "StateProvince" },
{ "cp_extension_dc", "DocumentControllerExtension" },
{ "mobile_number11", "ICHeadMobileNumber" },
{ "uot_head_spares_and_service", "HOSSName" },
{ "k__uot_head_spares_and_service", "HOSSID" },
{ "extention11", "ICHeadExtension" },
{ "extention51", "ProjectLeaderExtension" },
{ "uot_purchase_add_sdt120", "Address1" },
{ "uot_purchase_add2_sdt120", "Address2" },
{ "cp_phone_number_dc", "DocumentControllerPhoneNumber" },
{ "phone_number41", "InchargePhoneNumber" },
{ "email_id0", "HODEmail" },
{ "phone_number002", "WorkPhone" },
{ "email_id41", "InchargeEmail" },
{ "uot_incharge_exports_dp", "InchargeName" },
{ "k__uot_incharge_exports_dp", "InchargeID" },
{ "mobile_number_mails", "ZonalHeadMobileNumber" },
{ "extention0", "HODExtension" },
{ "fax1", "Fax" },
{ "uot_head_of_department", "HODName" },
{ "k__uot_head_of_department", "HODId" },
{ "extention21", "HOSSExtension" },
{ "cp_document_manager_dc", "DocumentControllerName" },
{ "k__cp_document_manager_dc", "DocumentControllerID" },
{ "ucp_cp_250", "CommunicationProtocolFormat" },
{ "uot_comp_name3_sdt250", "SoldToParty" },
{ "cp_mobile_number_dc", "DocumentControllerMobileNumber" },
{ "extention61", "ZonalHeadExtension" },
{ "ucp_boarline_sdt120", "BoardLine" },
{ "ucp_format_sdt120", "CPFormat" }
    };

    private static readonly Dictionary<string, string> BankGuaranteeMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
            { "id", "BankGuaranteeID" },
            { "record_no", "RecordNo" },
            { "uuu_record_last_update_date", "UpdatedDate" },
            { "process_status", "ProcessStatus" },
            { "status", "Status" },
            { "project_id", "ProjectID" },

            { "ubg_type_of_bg_pd", "TypeOfGuarantee" },
            { "ubg_others01_sdt250", "TypeOfGuaranteeOthers" },

            { "uuu_creation_date", "CreatedAt" },
            { "k__creator_id", "CreatedBy" },
            { "creator_id", "CreatedName" },

            { "ubg_claim_period_01dop", "ClaimPeriodDate" },
            { "ubg_date_dop", "ContractDate" },
            { "ubg_validity_dop", "ValidityDate" },

            { "ubg_contractual_order_ca", "TotalOrderValue" },
            { "ubg_equivalent_value_ca", "TotalOrderValueINR" },
            { "ubg_currency_amount_ca", "BankGuaranteeAmount" },

            { "ubg_percent_bg_da", "PercentageOfGuarantee" },
            { "ubg_exchange_rate_da", "ExchangeRate" },

            { "ubg_currency_pd01", "Currency" },

            { "ubg_contract_01pd", "IsContractCopyAttached" },
            { "ubg_draft_bank_ynpd", "IsDraftFormatAttached" },

            { "ubg_draft_gurante_format_pd", "DraftFormat" },
            { "ubg_others_sdt2501", "DraftFormatDetails" },

            { "ubg_guarantee_num_sdt250", "BankGuaranteeNo" },
            { "ubg_issuing_bank_sdt250", "IssuingBank" },

            { "ubg_gurantee_against_pd", "GuaranteeAgainst" },
            { "ubg_others_sdt2503", "GuaranteeAgainstOthers" },

            { "ubg_warranty_clause_pd", "WarrantyClause" },
            { "ubg_others2601_sdt250", "WarrantyClauseOthers" },

            { "ubg_agre_sdt_250", "ContractReferenceNo" },

            { "ubg_amrrndment_pd", "BankGuaranteeType" },

            { "ubg_remarks_ldt", "InitiatorReviewRemarks" },

            { "uot_sd", "CProjectNumber" }
    };

    // Hardcoded column mapping for OrderTransmittal tables (applies to tables with "OrderTransmittal" prefix)
    private static readonly Dictionary<string, string> OrderTransmittalMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        { "id", "OrderTransmittalID" },
            { "record_no", "RecordNo" },
            { "process_status", "ProcessStatus" },
            { "status", "Status" },
            { "project_id", "ProjectID" },

            { "k__customer_contacts_dp", "CustomerContactID" },
            { "k__customer_contacts_dp1", "CustomerContactID2" },
            { "k__customer_contacts_dp3", "EndUserContactID" },
            { "k__customer_contacts_dp4", "EndUserContactID2" },

            { "uot_bus_sec_pd", "BusinessSector" },
            { "uot_if_others11_sdt250", "OthersBusinessSector" },

            { "uot_order_type_pd", "OrderType" },
            { "ot_order_class_pd", "EPCorDirect" },

            { "ci_order_date_dop", "OrderDate" },
            { "uot_po_num_sdt120", "PurchaseOrderNumber" },
            { "uot_po_date_dop", "PODate" },
            { "uot_contract_sdt120", "ContractNumber" },
            { "uot_contract_date_dop", "ContractDate" },
            { "uot_agreement_sdt120", "AgreementNumber" },
            { "uot_agreement_date_dop", "AgreementDate" },

            { "uot_first_adv_pd", "FirstAdvanceReceived" },
            { "uot_miles_dop", "ReceiptOfFirstAdvancePaymentDate" },
            { "uot_frquency_pd", "Frequency" },

            { "uot_con_deliv_dop", "ContractualDeliveryDate" },
            { "contractual_commissioning", "ContractualCommissioningDate" },

            { "ot_currency_smn", "Currency" },
            { "ot_exchange_rate_da", "ExchangeRate" },

            { "uot_cs_pd", "ServiceType" },
            { "ib_supply2_da", "SupplyValue" },
            { "turnkey_value_ca", "Turnkey" },
            { "supervision_ca", "Supervision" },
            { "free_manday_supervision_da", "FreeMandaySupervision" },
            { "charges_free_manday_da", "ChargesAfterFreeMandays" },

            { "scope_of_spares", "ScopeOfSpares" },
            { "ot_total_order_value2_da", "OrderValue" },
            { "spares_value_ca", "SpareValue" },
            { "ci_order_valueinr_da", "OrderValueINR" },
            { "ot_order_supp_da", "OrderValueSupply" },
            { "ot_order_ec_da", "OrderValueEandC" },
            { "ot_order_supp_inr_da", "OrderValueSupplyINR" },
            { "ot_order_ec_inr_da", "OrderValueEandCINR" },

            { "uot_costsheet_ynpd", "CostSheetAttached" },
            { "agent_commission_pd", "AgentCommission" },
            { "uot_incoterms_pd", "INCOTerms" },
            { "uot_gst_pd", "GST" },

            { "uot_sea_worthy_packing_pd", "ScopeOfSeaworthyPacking" },
            { "uot_marine_ins_pd", "MarineInsurance" },
            { "uot_taxes_pd", "TaxesDutiesSpecify" },
            { "ci_tax_duties_da", "TaxesAndDutiesPercent" },
            //old
            //{ "k__uot_sold_party_dp", "CustomerMasterID" },
            //{ "k__uot_ship_to_partydp", "EndUserID" },
            //new 
             { "k__uot_sold_party_dp", "CustomerMasterID" },
            { "k__uot_ship_to_partydp", "EndUserID" },

            { "uot_sales_order_sdt120", "SupplySaleOrderno" },
            { "uot_sales_order_ec_sdt120", "ECSaleOrderNo" },
            { "uot_c_number_sdt120", "CProjectNumber" },
            { "k__uot_bpcreator_bc", "OldOTId" },

            { "uot_spl_notes_sdt2000", "SpecialNotes" },
            { "uot_spl_notes1_sdt2000", "SpecialNotesCustomerInformation" },

            { "ot_type_order_pd", "TypeOfOrder" },
            { "uot_site_insurance_pd", "SiteInsurance" },
            { "transit_insurance", "TransitInsurance" },
            { "uot_compre_insurance_pd", "ComprehensiveInsurance" },
            { "uot_fright_pd", "ScopeOfFrieght" },

            { "uot_spcy_sdt_250", "LimitIfAgreed" },
            { "uot_statu_app_pd", "StatutoryApproval" },

            { "otr_cost_rating_pd", "CostOverrunRiskRating" },
            { "otr_cost_impact_pd", "CostOverrunImpact" },
            { "otr_con_del_rating_pd", "ContractualDeliveryRiskRating" },
            { "otr_con_del_impact_pd", "ContractualDeliveryImpact" },
            { "otr_payment_rating_pd", "CommercialTermsRiskRating" },
            { "otr_payment_impact_pd", "CommercialTermsImpact" },

            { "otr_crs_rating_pd", "CustomerRelationshipRiskRating" },
            { "otr_crs_impact_pd", "CustomerRelationshipImpact" },

            { "otr_financial_rating_pd", "FinancialHealthRiskRating" },
            { "otr_financial_impact_pd", "FinancialHealthImpact" },

            { "otr_tg_rating_pd", "AgreedPerformanceRiskRating" },
            { "otr_tg_impact_pd", "AgreedPerformanceImpact" },

            { "otr_comm_terms_rating_pd", "WarrantyTermsRiskRating" },
            { "otr_comm_terms_impact_pd", "WarrantyTermsImpact" },

            { "uot_transmittaltype_pd", "TransmittalTypeID" },
        { "uot_loi_sdt120", "LetterOfIntentNumber" },
{ "uot_loi_date_dop", "LOIDate" },
{ "uot_comp_name2_sdt250", "CompanyNameConsultant" },
{ "uot_contact_name1_sdt250", "ContactPersonNameConsultant" },
{ "uot_designation3_sdt50", "DesignationConsultant" },
{ "uot_email3_tb120", "EmailConsultant" },
{ "uot_phone3_sdt50", "PhoneConsultant" },
{ "ugenfaxtxt16", "FaxConsultant" },
{ "uot_cons_citytxt50", "CityConsultant" },
{ "uot_india_consu_states_pd", "StateProvinceConsultant" },
{ "uot_consu_countrypd", "CountryConsultant" },
{ "uot_state2", "OtherStateProvinceConsultant" },
{ "uot_types_serv_pd", "TypesOfServicesEandC" },
{ "ot_mob_pd", "MobileCraneFacilityEandC" },
{ "uot_erection_crane_pd", "EotCraneFacilityEandC" },
{ "uot_erection_pd", "ErectionCraneEandC" },
{ "uot_conev_pd", "ConveyanceForEngineerEandC" },
{ "uot_unloading_pd", "UnloadingAtSiteEandC" },
{ "uot_grouting_pd", "GroutingEandC" },
{ "uot_grout_pd", "GroutingMaterialSupplyEandC" },
{ "uot_storage_pd", "StorageAtSiteEandC" },
{ "uot_const_pd", "ConstructionPowerWaterEandC" },
{ "uot_erection_cable_pd", "ErectionCableAndBaseEandC" },
{ "comissioning_spares_pd", "TypeOfSparesEandC" },
{ "uot_spares_desc_sdt500", "DescriptionEandC" },
{ "uot_additiona_sdt250", "AdditionalScopeConditions" },

{ "type_of_warranty_pd", "TypeOfWarranty" },
{ "others_please_specify1", "OtherTypeOfWarranty" },
{ "replaced_parts_warranty_pd", "ReplacedPartsWarranty" },
{ "uot_amb_temp_da", "AmbientTemperature" },
{ "uot_temp9_unit_pd", "AmbientTemperatureUnitID" },
{ "uot_temp_da", "TemperatureRiseDeltaT" },
{ "uot_temp12_unit_pd", "TemperatureRiseDeltaTUnitID" },
{ "ot_so_min_int", "TemperatureMin" },
{ "ot_so_max_int", "TemperatureMax" },

{ "uot_humidity_da", "RelativeHumidityPercent" },
{ "uot_altitude_da", "AltitudeAboveMSLMetres" },

{ "uot_earth_zone_pd", "EarthquakeZone" },
{ "uot_if_others8_sdt250", "EarthquakeZoneOther" },

{ "uot_ed_da", "ElectricalDesign" },
{ "uot_temp10_unit_pd", "ElectricalDesignUnitID" },

{ "uot_iap_dp", "InstrumentAirPressure" },
{ "uot_unit25_pd", "InstrumentAirPressureUnitID" },

{ "uot_cooling_water_pd", "CoolingWater" },
{ "uot_supply_da", "CWSupplyTemperature" },
{ "uot_temp11_unit_pd", "CWSupplyTemperatureUnitID" },
{ "uot_supply_pressure_da", "CWSupplyPressure" },
{ "uot_unit26_pd", "CWSupplyPressureUnitID" },

{ "uot_design_pres_sdt120", "DesignPressure" },
{ "uot_unit28_pd", "DesignPressureUnitID" },

{ "uot_presuure_drop_sdt120", "PressureDrop" },
{ "uot_unit27_pd", "PressureDropUnitID" },

{ "uot_motor_eff_pd", "MotorEfficiency" },
{ "uot_if_others6_sdt250", "MotorEfficiencyOther" },

{ "uot_main_pd", "GeneratedVoltageRating" },
{ "uot_if_others7_sdt250", "GeneratedVoltageOther" },

{ "uot_variation_da", "VariationCWPercent" },
{ "uot_variation2_da", "VariationFreqPercent" },
{ "uot_com_da", "CombinedVariationPercent" },

{ "uot_aux_power_pd", "AuxiliaryVoltageRating" },
{ "ele_others_23", "AuxiliaryVoltageOther" },

{ "uot_environment_pd", "Environment" },
{ "uot_if_others5_sdt250", "EnvironmentOther" },

{ "uot_scopepd", "ScopeForCivil" },
{ "uuu_creation_date", "CreatedAt" },
    };

    //Hardcoded column mapping for CustomerMaster
    private static readonly Dictionary<string, string> CustomerMasterMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "id", "CustomerID" },
            { "record_no", "RecordNo" },

            { "uuu_record_last_update_date", "UpdatedAt" },
            { "k__uuu_record_last_update_user", "UpdatedBy" },
            {"uuu_record_last_update_user","UpdatedName" },

            { "status", "Status" },

            { "k__creator_id", "CreatedBy" },
             { "creator_id", "CreatedName" },
            { "uuu_creation_date", "CreatedAt" },

            { "ucm_comp_name_sdt120", "CompanyName" },
            { "uot_sold_party_code_sdt120", "CompanyCode" },

            { "uuu_proj_phone", "Phone" },
            { "phone_number004", "WorkPhone" },

            { "uveemailtb120", "Email" },
            { "uuu_user_fax", "FaxNumber" },

            { "uvetaxidtb16", "GST" },
            { "uvelicensenotb16", "LicenseNo" },

            { "uot_shipping_pd", "StateIndia" },
            { "uot_state", "StateOther" },

            { "ugencitytxt50", "City" },
            { "ugencountrypd", "Country" },

            { "ucm_url_hp", "CompanyURL" },

            // Composite address – concatenate in code
            {
                "ugenaddress1txt120+ugenaddress2txt120+ugenaddress3txt120",
                "Address"
            }
        };

    //Hardcoded column mapping for CustomerContacts
    private static readonly Dictionary<string, string> CustomerContactMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
           { "id", "CustomerContactID" },
            { "record_no", "RecordNo" },
            { "title", "Title" },

            { "uuu_record_last_update_date", "UpdatedAt" },
            { "k__creator_id", "CreatedBy" },
            { "uuu_creation_date", "CreatedAt" },

            { "uircntctfstnmtb", "ContactName" },
            { "k__uot_sold_party_dp", "CustomerID" },

            { "uuu_proj_city", "City" },
            { "uuu_user_state", "State" },
            { "ugenzipcodetxt16", "ZipPostalCode" },
            { "ugencountrypd", "Country" },

            { "uot_designation3_sdt50", "Designation" },

            { "uue_user_contactphone", "ContactPhone" },
            { "uuu_user_workphone", "WorkPhone" },

            { "uveemailtb120", "Email" },

            // Composite address – must be concatenated in code
            {
                "uaddress1txt120+uaddress2txt120+ugenaddress3txt120",
                "Address"
            }
            // You can expand this dictionary with actual Excel column names
        };

    //Hardcoded column mapping for VendorMaster
    private static readonly Dictionary<string, string> VendorMasterMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
             { "id", "VendorID" },
            { "record_no", "RecordNo" },

            { "uuu_record_last_update_date", "UpdatedAt" },
            { "uuu_record_last_update_user", "UpdatedByName" },
            { "k__uuu_record_last_update_user", "UpdatedBy" },

            { "process_status", "ProcessStatus" },
            { "status", "StatusID" },

            { "creator_id", "CreatedByName" },
            { "k__creator_id", "CreatedBy" },

            { "vendor_master_vendor", "VendorName" },
            { "vendor_master_con_person", "ContactPerson" },
            { "vendor_master_manu_add", "ManufacturingAddress" },
            { "vendor_master_con_number", "ContactNumber" },

            { "uuu_creation_date", "CreatedAt" },
            { "vendor_master_code", "VendorCode" }
        };

    private static readonly Dictionary<string, string> MechanicalDBOMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            { "id", "MechanicalDBOId" },
            { "record_no", "RecordNo" },
            { "process_status", "ProcessStatus" },
            { "status", "Status" },
            { "project_id", "ProjectId" },
        {"k__ot_sel_ot_rec_bpp","OrderTransmittalID" },
            { "uot_others_411", "TubeSheetsAdditionalDetails" },
            { "uot_others_410", "ShellAdditionalDetails" },
            { "uot_start_ejector_pd", "StartupEjector" },
            { "uot_others_412", "TubesAdditionalDetails" },
            { "uot_water_pd", "WaterBoxes" },
            { "uot_others_4181", "QuantityAdditionalDetails" },
            { "uot_ttlscope7_pd", "AdditionalBOPScope" },
            { "uot_shell_interpd", "ShellOfInterAfterCondenser" },
            { "uot_unit29_pd", "ExhaustPressureUnitID" },
            { "uot_tubes_pd", "Tubes" },
            { "uot_spl_notes3_sdt2000", "SpecialNotes" },
            { "uot_plugging_pd", "PluggingMargin" },
            { "uot_others_408", "WaterBoxesAdditionalDetails" },
            { "uot_others_407", "StartupEjectorAdditionalDetails" },
            { "uot_ejector_pd", "EjectorNozzle" },
            { "uot_mech_outlet_temp_pd", "CWOutletTemperature" },
            { "uot_others_409", "BafflesAdditionalDetails" },
            { "uot_design_pressure_pd", "CWDesignPressure" },
            { "uot_conden_scope_pd", "CondenserScope" },
            { "uot_condensing_sdt120", "CondensingCapacity" },
            { "gland_scope", "GlandVentCondenserScope" },

            { "uuu_creation_date", "CreatedAt" },

            { "ele_others_11", "TubesOfInterAfterCondenserAdditionalDetails" },
            { "uot_add_bop_pd", "AdditionalBOP" },
            { "ele_others_08", "MainEjectorAdditionalDetails" },
            { "uot_fouling_pd", "FoulingFactor" },
            { "uot_during_start_pd", "EjectionSystemDuringStartup" },
            { "uot_vel_pd", "CWVelocity" },
            { "condensate_shell", "GlandVentShell" },
            { "glans_tubes", "GlandVentTubes" },
            { "uot_flow_rating_pd", "FlowRating" },
            { "remarks_tb1", "CondensorRemarks" },
            { "pressure10_pd", "PressureUnitID" },

            { "uot_others_429", "AdditionalBOPAdditionalDetails" },
            { "uot_others_426", "CleanlinessFactorAdditionalDetails" },
            { "uot_auxiliary_steam_da", "AuxiliarySteamTemperature" },
            { "uot_others_425", "FoulingFactorAdditionalDetails" },
            { "uot_tube_sheets_pd", "TubeSheets" },
            { "uot_others_422", "CWOutletTemperatureAdditionalDetails" },
            { "uot_others_421", "CWSupplyPressureAdditionalDetails" },
            { "uot_auto_gland_pd", "AutoGlandSealingSystem" },
            { "uot_others_424", "PluggingMarginAdditionalDetails" },
            { "exhaust_pressure_condensyst", "ExhaustPressure" },
            { "uot_others_423", "CWInletTemperatureAdditionalDetails" },
            { "uot_baffles_pd", "Baffles" },
            { "uot_others_420", "CWDesignPressureAdditionalDetails" },
            { "uot_condensate_pd", "CondensateExtractionPumpScope" },
            { "uot_auxilary_steam_da", "AuxiliarySteamPressure" },
            { "uot_rated_diff_head_pd", "RatedDifferentialHead" },
            { "uot_others_419", "CWVelocityAdditionalDetails" },
            { "uot_others_418", "HotelWellRetentionTimeAdditionalDetails" },
            { "lp_gland_sealing", "LPGlandSealingAndDesuperheater" },
            { "uot_others_415", "FlowRatingAdditionalDetails" },
            { "uot_others_417", "RatedDifferentialHeadAdditionalDetails" },
            { "uot_others_416", "MaterialOfCasingAdditionalDetails" },

            { "uot_ttlscope3_pd", "EjectionSystemScope" },
            { "uot_tubesheet_pd", "TubesSheetOfInterAfterCondenser" },
            { "ot_select_project_sp", "CloneProjectId" },

            { "remarks_tb13", "AuxiliaryRemarks" },
            { "remarks_tb15", "EjectionRemarks" },
            { "remarks_tb11", "CondensateRemarks" },
            { "remarks_tb12", "GlantVentRemarks" },

            { "ambient_temperature1", "AmbientTemperature" },
            { "uot_safety_condensor_pd", "SafetyDeviceForCondenser" },
            { "condensate_sheet", "GlandVentTubesSheet" },
            { "uot_mech_cleanli_pd", "CleanlinessFactor" },
            { "ele_others_50", "GlandVentShellAdditionalDetails" },
            { "uot_materail_pd", "MaterialOfCasing" },
            { "ele_others_51", "GlandVentTubesAdditionalDetails" },
            { "ele_others_52", "GlandVentTubesSheetAdditionalDetails" },
            { "uot_roto_meter_pd", "Rotometer" },
            { "msparameter_scope", "MSParameterGlandSealingEjectionSystemScope" },
            { "uot_inetr_pd", "InterAfterCondenser" },
            { "uot_relief_valve_pd", "ReliefValve" },
            { "uot_qty_pd", "Quantity" },
            { "uot_gland_sealing_ms", "GlandSealing" },
            { "temperature10_pd", "TemperatureUnitID" },
            { "uot_tubesof_inter_pd", "TubesOfInterAfterCondenser" },
            { "uot_temp8_unit_pd", "AmbientTemperatureUnitID" },
            { "ele_others_20", "EjectionSystemDuringStartupAdditionalDetails" },
            { "ele_others_21", "EjectionSystemForContinuousAdditionalDetails" },
            { "gland_blower", "Blower" },
            { "uot_cw_inlet_pd", "CWInletTemperature" },
            { "uot_main_ejector_pd", "MainEjector" },
            { "ot_vac_bre_pd", "VacuumBreakerValve" },
            { "uot_shell_pd", "Shell" },
            { "uot_condesning_type_pd", "Type" },
            { "uot_hotel_pd", "HotelWellRetentionTime" },
            { "uot_mec_dump_pd", "DumpProvision" },
            { "uot_supply_pressure_pd", "CWSupplyPressure" },
            { "uot_cross_overduct_pd", "CrossOverduct" },
            { "uot_for_continuous_pd", "EjectionSystemForContinuous" }
        };

    // Hardcoded column mapping for BPAttachments table
    private static readonly Dictionary<string, string> BPAttachmentMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        //{ "id", "BPAttachmentID" },
        { "parent_type", "RecordNo" },
        { "project_id", "ProjectID" },
        { "file_name", "FileName" },   // Note: file_name maps to both FileName and FilePath (handled in MatchColumnsForBPAttachments)
       // { "parent_id", "OrderTransmittalRecordID" },  // Conditionally mapped based on parent_type (only when parent_type = 'uxot2')
        { "upload_date", "CreatedAt" },
        { "upload_by", "CreatedBy" },
        {"parent_id","UnifierAttchmentID" }
    };
    // Hardcoded column mapping for BPAttachments when AttachmentRecordType = "Comment"
    private static readonly Dictionary<string, string> BPAttachmentCommentMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        //{ "id", "BPAttachmentID" },
        { "parent_type", "RecordNo" },
        { "project_id", "ProjectID" },
        { "file_name", "FileName" },   // Note: file_name maps to both FileName and FilePath (handled in MatchColumnsForBPAttachments)
        //{ "parent_id", "BPCommentRecordID" },  // For Comment type, parent_id maps to BPCommentRecordID
        { "upload_date", "CreatedAt" },
        { "upload_by", "CreatedBy" },
         {"parent_id","UnifierAttchmentID" }

    };

    // Hardcoded column mapping for BPAttachments when AttachmentRecordType = "OrderTransmittal"
    private static readonly Dictionary<string, string> BPAttachmentOTMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        //{ "id", "BPAttachmentID" },
        { "parent_type", "RecordNo" },
        { "project_id", "ProjectID" },
        { "file_name", "FileName" },   // Note: file_name maps to both FileName and FilePath (handled in MatchColumnsForBPAttachments)
        { "parent_id", "OrderTransmittalRecordID" },  // For OrderTransmittal type, parent_id maps to OrderTransmittalRecordID
        { "upload_date", "CreatedAt" },
        { "upload_by", "CreatedBy" }
    };

    // Hardcoded column mapping for BPComments table
    private static readonly Dictionary<string, string> BPCommentsMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        { "id", "BPCommentsID" },
        { "project_id", "ProjectID" },
        { "file_name", "CompanyID" },
        { "content", "Comments" },
        { "creatorid", "CreatedBy" },
        { "upload_by", "UserName" },
        //{ "parent_object_id", "" },
        { "lastmodified", "UpdatedAt" },
        {"parent_object_id","UnifierCommentsID" },
        {"parent_object_type","Attachments" }
    };
    private static readonly Dictionary<string, string> BPCommentsOrderTransmittalRecordMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        { "id", "BPCommentsID" },
        { "project_id", "ProjectID" },
        { "file_name", "CompanyID" },
        { "content", "Comments" },
        { "creatorid", "CreatedBy" },
        { "upload_by", "UserName" },
        { "parent_object_id", "OrderTransmittalRecordID" },
        { "lastmodified", "UpdatedAt" },
            {"header_id","UnifierCommentsID" }
    };

    // Hardcoded column mapping for Turbine table
    private static readonly Dictionary<string, string> TurbineMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
            { "id", "TurbineID" },
            { "record_no", "RecordNo" },
            { "title", "Title" },
            { "due_date", "DueDate" },
            { "end_date", "EndDate" },

            { "uuu_record_last_update_date", "UpdatedAt" },
            { "k__uuu_record_last_update_user", "UpdatedBy" },

            { "process_status", "ProcessStatus" },
            { "status", "StatusId" },

            { "k__creator_id", "CreatedBy" },

            { "project_id", "ProjectId" },

            { "uot_gearbox_pd", "GearBoxTypeID" },
            { "uot_eff_cons_pd", "EfficiencyId" },

            { "ec2_remarks2_mdt4000", "RemarksCoupling" },

            { "uot_control_oil_filter_pd", "ControlOilFilterId" },
            { "uot_hmbd_pd", "HMBDSubmittedId" },
            { "uot_drive_pd", "DrivenEquipmentId" },
            { "uot_dirct_rot_da", "RotationDirectionID" },
            { "uot_tube_pd", "TubeMOCId" },
            { "uot_mec_over_pd", "OverHeadTankId" },

            { "uuu_dm_publish_path", "uuu_dm_publish_path" },
            { "uuu_creation_date", "CreatedAt" },

            { "uot_min_power_da1", "MinLoadExtraction" },


            { "ec2_remarks9_mdt4000", "Remarks_Gearbox" },
            { "uot_noisepd", "GearBox_NoiseLevelID" },

            { "uot_docu_ms", "DocumentationID" },

            { "ele_others_06", "AnyOtherPointsAdditionalDetails" },
            { "ele_others_07", "TubeSheetsAdditionalDetails" },

            { "ot_others_specify_sdt250", "ManufacturingStandardOthersSpecify" },

            { "uot_mech_sb_sdt250", "StatorBlades" },
            { "gearbox_scope", "GearBoxScope" },

            { "uot_secondary_gear_pd", "SecondaryGearBoxID" },
            { "uot_driven_eqptpd", "SecondaryGBDrivenEqId" },

            { "uot_required_mos_pd", "IfRequiredMOCId" },

            { "uot_others_433", "FoulingFactorAdditionalDetails" },
            { "uot_mech_oil_heaters_pd", "OilHeatersId" },
            { "uot_others_432", "SSTypeAdditionalDetails" },

            { "uot_turbine_pd", "TypeOfTurbineId" },
            { "uot_others_435", "OtherSpecify_Type1" },
            { "uot_others_434", "TubeMOCAdditionalDetails" },

            { "uot_type_inlet_pd", "InletOrientationId" },
            { "uot_others_431", "OilCentrifugeAdditionalDetails" },
            { "uot_others_430", "IfRequiredCapacityAdditionalDetails" },

            { "uot_mech_bs_sdt250", "uot_mech_bs_sdt250" },

            { "ele_others_64", "PluggingMarginAdditonalDetails" },

            { "ot_ss_type_pd", "SSTypeId" },
            { "uot_manu_sta_pd", "ManufacturingStandardID" },

            { "uot_if_others10_sdt250", "NoiseLeveOthersSpecify" },

            { "uot_mech_reduction_pd", "ReductionID" },
            { "ec1_remrks6_mdt4000", "GovernorRemarks" },

            { "uot_if_others9_sdt250", "DrivenEquipmentOthersSpecify" },

            { "k__ot_sel_ot_rec_bpp", "OrderTransmittalID" },

            { "uot_material_pd", "MaterialOfConstruction" },
            { "uot_mech_ffpd", "FoulingFactorId" },

            { "uot_ttlscope6_pd", "GovernorScope" },
            { "uot_mech_casings_sdt250", "Casings" },

            { "uot_nonstandard_pd", "FrameId" },
            { "ot_others_specify6_sdt250", "SSTypeAdditionalDetails" },

            { "uot_turbine_details_scope_p", "ScopeId" },
            { "uot_vendor_pd", "VendorMasterID" },

            { "uot_type_drive_pd", "MOPDriveId" },
            { "uot_mech_accoustic_pd", "AcousticHoodId" },
            { "uot_bearing_pd", "BarringGearID" },

            { "prc1_remarks2_mtb400", "LubeOilRemarks" },

            { "uot_couplg_type_pd", "Type1Id" },
            { "uot_oil_cooler_pd", "OilCoolerId" },

            { "uot_foot_print_replcement_p", "FootPrintReplacementId" },

            { "ec1_remrks5_mdt4000", "Remarks" },

            { "uot_min_load_da", "MinLoadBleed" },

            { "uot_noise_tg_pd", "NoiseLevelID" },



            { "uot_ttlscope8_pd", "HighSpeedScopeId" },

            { "uot_mech_oil_filter_pd", "OilFilterId" },

            { "uot_ratiing_ia", "RatingKW" },

            { "uot_245others_sdt120", "OthersText" },

            { "uot_mech_dirty_pd", "DirtyOilTankId" },

            { "ot_others_specify7_sdt250", "OtherSpecify_Type2" },

            { "uot_mec_vapour_extrr_pd", "VapourExtractorId" },

            { "uot_mech_amot_pd", "AMOTTCVId" },

            { "uot_mech_tubesheet_pd", "TubeSheetsId" },

            { "uot_others_437", "ShortCircuitFactorAdditionalDetails" },
            { "uot_others_436", "GearBox_NoiseLevel_AdditionalDetails" },

            { "low_speed_coupling_type", "Type2Id" },

            { "uot_type_exhst_pd", "ExhaustOrientationId" },

            { "uot_service_pd", "ServiceFactorAdditionalDetails" },
            { "uot_others_438", "ServiceFactorAdditionalDetails" },

            { "uot_governorpd", "Governor" },

            { "uot_mangficant_pd", "ShortCircuitFactorID" },

            { "uot_margin_pd", "PluggingMarginId" },

            { "uot_ttlscope5_pd", "LubeOilScopeId" },

            { "uot_drns_ms", "DrawingsID" },

            { "specify_if_non_standard_tb", "NonStandardFrame" },

            { "uot_lube_oil_piping_pd", "LubeOilPipingId" },

            { "uot_mec_points_ms", "AnyOtherPointsId" },

            { "ec2_remarks5_mdt4000", "MaterialRemarks" },

            { "uot_ttl_scope_pd", "PrimarySecondaryGBId" },

            { "uot_spl_notes2_sdt2000", "SpecialNotes" },

            { "uot_capacity_pd", "IfRequiredCapacityId" },

            { "uot_oil_centre_pd", "OilCentrifugeId" },

            { "uot_qap_pd", "QAPID" },

            { "uot_mech_rotor_sdt250", "Rotor" },

            { "low_speed_coupling_scope_pd", "LowSpeedScopeId_pd" },

            { "lubetype", "LubeTypeId" },

            { "uot_mech_rb_sdt250", "RotorBlades" }
    };

    public static readonly Dictionary<string, string> ElectricalInstrumentationDBOMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            // ---- Core / Audit ----
            { "id", "ElectricalInstrumentationDBOID" },
            { "record_no", "RecordNo" },
            { "project_id", "ProjectId" },
            { "process_status", "ProcessStatus" },
            { "status", "Status" },
            { "creator_id", "CreatedBy" },
            { "uuu_creation_date", "CreatedAt" },
            { "uuu_record_last_update_date", "UpdatedAt" },
            { "uuu_record_last_update_user", "UpdatedBy" },

            // ---- TVM / Metering ----
            { "uot_tvm_mounting_pd", "TVMMountingID" },
            { "uot_tvm_accuracy_pd", "TVMAccuracyID" },
            { "uot_tvm_type_pd", "TVMTypeID" },
            { "ele_others_404", "TVMMountingOthersSpecify" },

            // ---- Panels / Scope ----
            { "uot_avr_panel_scopepd", "TurbineGaugePanelScopeID" },
            { "uot_avr_panel_scopepd2", "AVRPanelScopeID" },
            { "uot_avr_panel_scopepd2alt", "AlternatorScopeID" },
            { "uot_avr_panel_scopepd2meter", "SynchronizingPanelScopeID" },
            { "uot_avr_panel_scopepd2plc", "TurbineControlPanelScopeID" },
            { "uot_avr_panel_scopepd2ng", "TransformerPanelScopeID" },

            // ---- Ratings / IP ----
            { "ip_rating_turbinecontrol", "TCP_IPRatingID" },
            { "ip_rating_dcmotor", "DC_Motor_IPRatingID" },
            { "ip_rating_avr", "Avr_IPRatingID" },
            { "ip_rating_meter", "IPRatingID" },
            { "ip_rating_generator", "Relay_IPRatingID" },
            { "ip_rating_acb", "Acb_IPRatingID" },
            { "ip_rating_switch", "SwitchGear_IPRatingID" },
            { "ip_rating_motor", "MotorControl_IPRatingID" },

            // ---- Cables / Busduct ----
            { "lt_power", "LTPowerCablingID" },
            { "ht_power", "HTPowerCablingID" },
            { "lt_powercable_moc", "LTPowerCableMOCID" },
            { "ht_cable_moc", "HTPowerCableMOCID" },
            { "ot_length_sdt50", "HTPowerCableLength" },
            { "ot_length2_sdt50", "BusDuctLength" },
            { "ot_bdt_pd", "BusDuctTypeID" },
            { "uot_bbt_pd", "BusDuctID" },

            // ---- Battery / UPS ----
            { "uot_type_pd", "Battery_TypeID" },
            { "uot_float_cum_boost_charger", "Battery_TypeOfChargerID" },
            { "uot_bcc_volt_pd", "Battery_VoltageRatingID" },
            { "uot_bcc_capc_pd", "Battery_CapacityID" },
            { "ip_rating_battery", "Battery_IPRatingID" },

            // ---- Control / PLC / TCP ----
            { "uot_control_mode_pd", "ControlModeID" },
            { "uot_control_cabling_pd", "ControlCablingID" },
            { "uot_plc_based_instruments", "PLCBasedInstrumentsID" },
            { "uot_commu_type_pd", "TCP_CommunicationTypeID" },
            { "uot_sil_rating_pd", "TCP_SILRatingID" },
            { "uot_elect_scope_pd", "TCP_SpecificationID" },

            // ---- Remarks / Notes ----
            { "uot_spl_notes4_sdt2000", "SpecialNotes" },
            { "elec_ot_rmk_mtb4000", "Remarks" },
            { "elec_ot_rmk2_mtb4000", "AVRRemarks" },
            { "elec_ot_rmk3_mtb4000", "ACBRemarks" },
            { "elec_ot_rmk4_mtb4000", "Metering_Remarks" },
            { "elec_ot_rmk5_mtb4000", "TransformerRemarks" },
            { "elec_ot_rmk6_mtb4000", "LASCPTRemarks" },
            { "elec_ot_rmk7_mtb4000", "SwitchGearRemarks" },
            { "elec_ot_rmk8_mtb4000", "MotorControlRemarks" },
            { "elec_ot_rmk9_mtb4000", "BatteryRemarks" },

            // ---- OT / Reference ----
            { "ot_sel_ot_rec_bpp", "OrderTransmittalID" },
            { "ot_select_project_sp", "CloneProjectId" }
        };
	private static readonly Dictionary<string, string> ContractClearanceMapping = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
{
    // Identifiers
    { "record_no", "RecordNo" },
	{ "id", "ContractClearanceId" },
	{ "project_id", "ProjectId" },
	{ "k__uot_ship_to_partydp", "EndUserID" },
	{ "k__uot_sold_party_dp", "CustomerMasterID" },
	{ "uot_bp_picker_bp", "OrderTransmittalId" },
	{ "CCRecordSelectionId", "CCRecordSelectionId" },

    // Status / Workflow
    { "status", "Status" },
	{ "creator_id", "CreatedName" },
	{ "k__creator_id", "CreatedBy" },
	{ "uuu_creation_date", "CreatedAt" },
	{ "uuu_record_last_update_date", "UpdatedAt" },

    // Turbine / Equipment Details
    { "uot_turbine_pd", "TypeofTurbine" },
	{ "uot_tur_framepd", "TypeofTurbine" },
	{ "uot_ratiing_ia", "TurbineRatingKW" },
	{ "turbine_material_code", "TurbineMaterialCode" },
	{ "uot_nonstandard_pd", "FrameStandard" },
	{ "specify_if_non_standard_tb", "FrameNonStandard" },
	{ "comissioning_spares_pd", "TypeofSpares" },

    // Contract / Warranty / Service
    { "uot_contract_clnce_sdt120", "ContractClearanceFormat" },
	{ "type_of_warranty_pd", "TypeOfWarranty" },
	{ "uot_cs_pd", "ServiceType" },
	{ "uot_qap_pd", "QAP" },
	{ "order_acceptance", "OrderAcceptance" },

    // Dates / Scheduling
    { "ucc_schedul_dop", "ScheduledDispatchDate" },
	{ "ucc_kom1", "ProposedKickOffDate" },
	{ "ot_date", "OTDate" },

    // Billing / Finance
    { "cc_bm_turbine_dop1", "TurbineBillingMonth" },
	{ "cc_bv_turbine_da", "TurbineBillingValue" },
	{ "cc_bm_dbo_dop", "DBOBillingMonth" },
	{ "cc_bv_dbo_da", "DBOBillingValue" },

    // Special Instructions
    { "ugeninstructionmtl4000", "SpecialInstructions" }
};


	public async Task<UploadResponse> MigrateExcelToSqlServerAsync(
        string connectionString,
        string schemaName,
        string tableName,
        DataTable excelData,
        string? attachmentRecordType = null,
        CancellationToken cancellationToken = default)
    {
        var response = new UploadResponse();

        if (excelData == null || excelData.Rows.Count == 0)
        {
            response.ErrorMessages.Add("Excel file contains no data.");
            return response;
        }

        // Check if table name starts with "OrderTransmittal" - migrate to all matching tables
        if (tableName.StartsWith("OrderTransmittal", StringComparison.OrdinalIgnoreCase))
        {
            return await MigrateToOrderTransmittalTablesAsync(connectionString, schemaName, tableName, excelData, cancellationToken);
        }

        // Check if table name starts with "MechanicalDBO" - migrate to all matching tables
        if (tableName.StartsWith("MechanicalDBO", StringComparison.OrdinalIgnoreCase))
        {
            return await MigrateToMechanicalDBOTablesAsync(connectionString, schemaName, tableName, excelData, cancellationToken);
        }

        // Check if this is BPComments table - use single table migration
        if (string.Equals(tableName, "BPComments", StringComparison.OrdinalIgnoreCase))
        {
            await using var bpCommentsConnection = new SqlConnection(connectionString);
            await bpCommentsConnection.OpenAsync(cancellationToken);
            return await MigrateToSingleTableAsync(bpCommentsConnection, schemaName, tableName, excelData, attachmentRecordType, cancellationToken);
        }

        // Check if this is BPAttachments table - use single table migration with attachmentRecordType
        if (string.Equals(tableName, "BPAttachments", StringComparison.OrdinalIgnoreCase))
        {
            await using var bpAttachmentsConnection = new SqlConnection(connectionString);
            await bpAttachmentsConnection.OpenAsync(cancellationToken);
            return await MigrateToSingleTableAsync(bpAttachmentsConnection, schemaName, tableName, excelData, attachmentRecordType, cancellationToken);
        }

        // Check if table name starts with "ElectricalInstrumentationDBO" - migrate to all matching tables
        if (tableName.StartsWith("ElectricalInstrumentationDBO", StringComparison.OrdinalIgnoreCase))
        {
            return await MigrateToElectricalInstrumentationDBOTablesAsync(connectionString, schemaName, tableName, excelData, cancellationToken);
        }

        // Check if table name starts with "Turbine" - migrate to all matching tables
        if (tableName.StartsWith("Turbine", StringComparison.OrdinalIgnoreCase))
        {
            return await MigrateToTurbineTablesAsync(connectionString, schemaName, tableName, excelData, cancellationToken);
        }

        // For single table migration (existing logic)
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        var transaction = connection.BeginTransaction();
        var tempTableName = $"#TMP_{Guid.NewGuid():N}";

        try
        {
            // Step 1: Get target table metadata
            var tableMetadata = await GetTableMetadataAsync(connection, transaction, schemaName, tableName, cancellationToken);

            if (tableMetadata.Count == 0)
            {
                response.ErrorMessages.Add($"Table '{schemaName}.{tableName}' not found or has no columns.");
                transaction.Rollback();
                return response;
            }

            // Step 2: Match Excel columns to SQL columns
            var columnMappings = MatchColumns(excelData, tableMetadata, tableName, attachmentRecordType);

            if (columnMappings.Count == 0)
            {
                response.ErrorMessages.Add("No matching columns found between Excel and SQL table.");
                transaction.Rollback();
                return response;
            }

            // Step 3: Check for identity column
            var identityColumn = tableMetadata.FirstOrDefault(m => m.IsIdentity);
            var hasIdentityInExcel = identityColumn != null &&
                                    columnMappings.Any(m => m.SqlColumnName.Equals(identityColumn.ColumnName, StringComparison.OrdinalIgnoreCase));

            // Step 4: Create temp table
            await CreateTempTableAsync(connection, transaction, tempTableName, tableMetadata, cancellationToken);

            // Step 5: Prepare DataTable with only matched columns
            var (mappedDataTable, rowErrors) = await PrepareMappedDataTableAsync(connection, transaction, excelData, columnMappings, tableName, schemaName, tableMetadata, cancellationToken);

            // Add row errors to response
            response.RowErrors.AddRange(rowErrors);

            // Step 6: Bulk copy to temp table
            var rowsCopiedToTemp = await BulkCopyToTempTableAsync(
                connection,
                transaction,
                tempTableName,
                mappedDataTable,
                columnMappings,
                hasIdentityInExcel,
                cancellationToken);

            // Step 7: Get primary key columns
            var primaryKeyColumns = tableMetadata.Where(m => m.IsPrimaryKey).Select(m => m.ColumnName).ToList();

            // Step 8: Upsert from temp table to target table using MERGE
            var (rowsInserted, rowsUpdated) = await MergeFromTempToTargetAsync(
                connection,
                transaction,
                schemaName,
                tableName,
                tempTableName,
                columnMappings,
                primaryKeyColumns,
                identityColumn,
                hasIdentityInExcel,
                cancellationToken);

            transaction.Commit();

            response.Success = rowErrors.Count == 0;
            response.RowsInserted = rowsInserted;
            response.RowsUpdated = rowsUpdated;
            response.RowsFailed = rowErrors.Count;

            var totalProcessed = rowsInserted + rowsUpdated;
            if (totalProcessed > 0)
            {
                response.Message = $"Successfully processed {totalProcessed} row(s): {rowsInserted} inserted, {rowsUpdated} updated.";
            }

            if (rowErrors.Count > 0)
            {
                response.ErrorMessages.Add($"{rowErrors.Count} row(s) failed during data preparation. See RowErrors for details.");
            }

        }
        catch (Exception ex)
        {
            transaction.Rollback();
            response.ErrorMessages.Add($"Error during migration: {ex.Message}");
            if (ex.InnerException != null)
            {
                response.ErrorMessages.Add($"Inner exception: {ex.InnerException.Message}");
            }
        }
        finally
        {
            // Clean up temp table
            try
            {
                await DropTempTableAsync(connection, transaction, tempTableName, cancellationToken);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        return response;
    }

    private async Task<UploadResponse> MigrateToOrderTransmittalTablesAsync(
        string connectionString,
        string schemaName,
        string tableNamePrefix,
        DataTable excelData,
        CancellationToken cancellationToken = default)
    {
        var response = new UploadResponse();
        var allTableResults = new List<(string tableName, int inserted, int updated, int failed, List<string> errors)>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        try
        {
            // Step 1: Find all tables starting with "OrderTransmittal" in the schema
            var matchingTables = await GetTablesWithPrefixAsync(connection, schemaName, "OrderTransmittal", cancellationToken);

            if (matchingTables.Count == 0)
            {
                response.ErrorMessages.Add($"No tables found with prefix 'OrderTransmittal' in schema '{schemaName}'.");
                return response;
            }

            // Step 2: Sort tables to ensure parent table is migrated first
            // Parent table is "OrderTransmittal" (exact match), child tables have underscores
            var parentTable = matchingTables.FirstOrDefault(t =>
                string.Equals(t, "OrderTransmittal", StringComparison.OrdinalIgnoreCase));
            var childTables = matchingTables.Where(t =>
                !string.Equals(t, "OrderTransmittal", StringComparison.OrdinalIgnoreCase))
                .OrderBy(t => t).ToList();

            // Build ordered list: parent first, then children
            var orderedTables = new List<string>();
            if (parentTable != null)
            {
                orderedTables.Add(parentTable);
            }
            orderedTables.AddRange(childTables);

            // Step 3: Migrate Excel data to each matching table in order
            foreach (var targetTable in orderedTables)
            {
                var tableResponse = await MigrateToSingleTableAsync(
                    connection,
                    schemaName,
                    targetTable,
                    excelData,
                    null,
                    cancellationToken);

                allTableResults.Add((
                    targetTable,
                    tableResponse.RowsInserted,
                    tableResponse.RowsUpdated,
                    tableResponse.RowsFailed,
                    tableResponse.ErrorMessages.ToList()
                ));

                // Aggregate row errors
                response.RowErrors.AddRange(tableResponse.RowErrors);
            }

            // Step 3: Aggregate results
            response.Success = allTableResults.All(r => r.errors.Count == 0) && response.RowErrors.Count == 0;
            response.RowsInserted = allTableResults.Sum(r => r.inserted);
            response.RowsUpdated = allTableResults.Sum(r => r.updated);
            response.RowsFailed = allTableResults.Sum(r => r.failed) + response.RowErrors.Count;

            // Build summary message
            var successCount = allTableResults.Count(r => r.errors.Count == 0);
            var totalTables = allTableResults.Count;
            var totalProcessed = response.RowsInserted + response.RowsUpdated;

            if (totalProcessed > 0)
            {
                response.Message = $"Migrated to {totalTables} table(s): {successCount} succeeded. " +
                                 $"Total: {totalProcessed} row(s) processed ({response.RowsInserted} inserted, {response.RowsUpdated} updated).";
            }

            // Add per-table error messages
            foreach (var result in allTableResults.Where(r => r.errors.Count > 0))
            {
                response.ErrorMessages.Add($"Table '{result.tableName}': {string.Join("; ", result.errors)}");
            }

            if (response.RowErrors.Count > 0)
            {
                response.ErrorMessages.Add($"{response.RowErrors.Count} row(s) failed during data preparation. See RowErrors for details.");
            }
        }
        catch (Exception ex)
        {
            response.ErrorMessages.Add($"Error during OrderTransmittal migration: {ex.Message}");
            if (ex.InnerException != null)
            {
                response.ErrorMessages.Add($"Inner exception: {ex.InnerException.Message}");
            }
        }

        return response;
    }

    private async Task<UploadResponse> MigrateToTurbineTablesAsync(
        string connectionString,
        string schemaName,
        string tableNamePrefix,
        DataTable excelData,
        CancellationToken cancellationToken = default)
    {
        var response = new UploadResponse();
        var allTableResults = new List<(string tableName, int inserted, int updated, int failed, List<string> errors)>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        try
        {
            // Step 1: Find all tables starting with "Turbine" in the schema
            var matchingTables = await GetTablesWithPrefixAsync(connection, schemaName, "Turbine", cancellationToken);

            if (matchingTables.Count == 0)
            {
                response.ErrorMessages.Add($"No tables found with prefix 'Turbine' in schema '{schemaName}'.");
                return response;
            }

            // Step 2: Sort tables to ensure parent table is migrated first
            // Parent table is "Turbine" (exact match), child tables have underscores
            var parentTable = matchingTables.FirstOrDefault(t =>
                string.Equals(t, "Turbine", StringComparison.OrdinalIgnoreCase));
            var childTables = matchingTables.Where(t =>
                !string.Equals(t, "Turbine", StringComparison.OrdinalIgnoreCase))
                .OrderBy(t => t).ToList();

            // Build ordered list: parent first, then children
            var orderedTables = new List<string>();
            if (parentTable != null)
            {
                orderedTables.Add(parentTable);
            }
            orderedTables.AddRange(childTables);

            // Step 3: Migrate Excel data to each matching table in order
            foreach (var targetTable in orderedTables)
            {
                var tableResponse = await MigrateToSingleTableAsync(
                    connection,
                    schemaName,
                    targetTable,
                    excelData,
                    null,
                    cancellationToken);

                allTableResults.Add((
                    targetTable,
                    tableResponse.RowsInserted,
                    tableResponse.RowsUpdated,
                    tableResponse.RowsFailed,
                    tableResponse.ErrorMessages.ToList()
                ));

                // Aggregate row errors
                response.RowErrors.AddRange(tableResponse.RowErrors);
            }

            // Step 4: Aggregate results
            response.Success = allTableResults.All(r => r.errors.Count == 0) && response.RowErrors.Count == 0;
            response.RowsInserted = allTableResults.Sum(r => r.inserted);
            response.RowsUpdated = allTableResults.Sum(r => r.updated);
            response.RowsFailed = allTableResults.Sum(r => r.failed) + response.RowErrors.Count;

            // Build summary message
            var successCount = allTableResults.Count(r => r.errors.Count == 0);
            var totalTables = allTableResults.Count;
            var totalProcessed = response.RowsInserted + response.RowsUpdated;

            if (totalProcessed > 0)
            {
                response.Message = $"Migrated to {totalTables} table(s): {successCount} succeeded. " +
                                 $"Total: {totalProcessed} row(s) processed ({response.RowsInserted} inserted, {response.RowsUpdated} updated).";
            }

            // Add per-table error messages
            foreach (var result in allTableResults.Where(r => r.errors.Count > 0))
            {
                response.ErrorMessages.Add($"Table '{result.tableName}': {string.Join("; ", result.errors)}");
            }

            if (response.RowErrors.Count > 0)
            {
                response.ErrorMessages.Add($"{response.RowErrors.Count} row(s) failed during data preparation. See RowErrors for details.");
            }
        }
        catch (Exception ex)
        {
            response.ErrorMessages.Add($"Error during Turbine migration: {ex.Message}");
            if (ex.InnerException != null)
            {
                response.ErrorMessages.Add($"Inner exception: {ex.InnerException.Message}");
            }
        }

        return response;
    }

    private async Task<UploadResponse> MigrateToMechanicalDBOTablesAsync(
        string connectionString,
        string schemaName,
        string tableNamePrefix,
        DataTable excelData,
        CancellationToken cancellationToken = default)
    {
        var response = new UploadResponse();
        var allTableResults = new List<(string tableName, int inserted, int updated, int failed, List<string> errors)>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        try
        {
            // Step 1: Find all tables starting with "MechanicalDBO" in the schema
            var matchingTables = await GetTablesWithPrefixAsync(connection, schemaName, "MechanicalDBO", cancellationToken);

            if (matchingTables.Count == 0)
            {
                response.ErrorMessages.Add($"No tables found with prefix 'MechanicalDBO' in schema '{schemaName}'.");
                return response;
            }

            // Step 2: Sort tables to ensure parent table is migrated first
            // Parent table is "MechanicalDBO" (exact match), child tables have underscores
            var parentTable = matchingTables.FirstOrDefault(t =>
                string.Equals(t, "MechanicalDBO", StringComparison.OrdinalIgnoreCase));
            var childTables = matchingTables.Where(t =>
                !string.Equals(t, "MechanicalDBO", StringComparison.OrdinalIgnoreCase))
                .OrderBy(t => t).ToList();

            // Build ordered list: parent first, then children
            var orderedTables = new List<string>();
            if (parentTable != null)
            {
                orderedTables.Add(parentTable);
            }
            orderedTables.AddRange(childTables);

            // Step 3: Migrate Excel data to each matching table in order
            foreach (var targetTable in orderedTables)
            {
                var tableResponse = await MigrateToSingleTableAsync(
                    connection,
                    schemaName,
                    targetTable,
                    excelData,
                    null,
                    cancellationToken);

                allTableResults.Add((
                    targetTable,
                    tableResponse.RowsInserted,
                    tableResponse.RowsUpdated,
                    tableResponse.RowsFailed,
                    tableResponse.ErrorMessages.ToList()
                ));

                // Aggregate row errors
                response.RowErrors.AddRange(tableResponse.RowErrors);
            }

            // Step 4: Aggregate results
            response.Success = allTableResults.All(r => r.errors.Count == 0) && response.RowErrors.Count == 0;
            response.RowsInserted = allTableResults.Sum(r => r.inserted);
            response.RowsUpdated = allTableResults.Sum(r => r.updated);
            response.RowsFailed = allTableResults.Sum(r => r.failed) + response.RowErrors.Count;

            // Build summary message
            var successCount = allTableResults.Count(r => r.errors.Count == 0);
            var totalTables = allTableResults.Count;
            var totalProcessed = response.RowsInserted + response.RowsUpdated;

            if (totalProcessed > 0)
            {
                response.Message = $"Migrated to {totalTables} table(s): {successCount} succeeded. " +
                                 $"Total: {totalProcessed} row(s) processed ({response.RowsInserted} inserted, {response.RowsUpdated} updated).";
            }

            // Add per-table error messages
            foreach (var result in allTableResults.Where(r => r.errors.Count > 0))
            {
                response.ErrorMessages.Add($"Table '{result.tableName}': {string.Join("; ", result.errors)}");
            }

            if (response.RowErrors.Count > 0)
            {
                response.ErrorMessages.Add($"{response.RowErrors.Count} row(s) failed during data preparation. See RowErrors for details.");
            }
        }
        catch (Exception ex)
        {
            response.ErrorMessages.Add($"Error during MechanicalDBO migration: {ex.Message}");
            if (ex.InnerException != null)
            {
                response.ErrorMessages.Add($"Inner exception: {ex.InnerException.Message}");
            }
        }

        return response;
    }

    private async Task<UploadResponse> MigrateToElectricalInstrumentationDBOTablesAsync(
        string connectionString,
        string schemaName,
        string tableNamePrefix,
        DataTable excelData,
        CancellationToken cancellationToken = default)
    {
        var response = new UploadResponse();
        var allTableResults = new List<(string tableName, int inserted, int updated, int failed, List<string> errors)>();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        try
        {
            // Step 1: Find all tables starting with "ElectricalInstrumentationDBO" in the schema
            var matchingTables = await GetTablesWithPrefixAsync(connection, schemaName, "ElectricalInstrumentationDBO", cancellationToken);

            if (matchingTables.Count == 0)
            {
                response.ErrorMessages.Add($"No tables found with prefix 'ElectricalInstrumentationDBO' in schema '{schemaName}'.");
                return response;
            }

            // Step 2: Sort tables to ensure parent table is migrated first
            // Parent table is "ElectricalInstrumentationDBO" (exact match), child tables have underscores
            var parentTable = matchingTables.FirstOrDefault(t =>
                string.Equals(t, "ElectricalInstrumentationDBO", StringComparison.OrdinalIgnoreCase));
            var childTables = matchingTables.Where(t =>
                !string.Equals(t, "ElectricalInstrumentationDBO", StringComparison.OrdinalIgnoreCase))
                .OrderBy(t => t).ToList();

            // Build ordered list: parent first, then children
            var orderedTables = new List<string>();
            if (parentTable != null)
            {
                orderedTables.Add(parentTable);
            }
            orderedTables.AddRange(childTables);

            // Step 3: Migrate Excel data to each matching table in order
            foreach (var targetTable in orderedTables)
            {
                var tableResponse = await MigrateToSingleTableAsync(
                    connection,
                    schemaName,
                    targetTable,
                    excelData,
                    null,
                    cancellationToken);

                allTableResults.Add((
                    targetTable,
                    tableResponse.RowsInserted,
                    tableResponse.RowsUpdated,
                    tableResponse.RowsFailed,
                    tableResponse.ErrorMessages.ToList()
                ));

                // Aggregate row errors
                response.RowErrors.AddRange(tableResponse.RowErrors);
            }

            // Step 4: Aggregate results
            response.Success = allTableResults.All(r => r.errors.Count == 0) && response.RowErrors.Count == 0;
            response.RowsInserted = allTableResults.Sum(r => r.inserted);
            response.RowsUpdated = allTableResults.Sum(r => r.updated);
            response.RowsFailed = allTableResults.Sum(r => r.failed) + response.RowErrors.Count;

            // Build summary message
            var successCount = allTableResults.Count(r => r.errors.Count == 0);
            var totalTables = allTableResults.Count;
            var totalProcessed = response.RowsInserted + response.RowsUpdated;

            if (totalProcessed > 0)
            {
                response.Message = $"Migrated to {totalTables} table(s): {successCount} succeeded. " +
                                 $"Total: {totalProcessed} row(s) processed ({response.RowsInserted} inserted, {response.RowsUpdated} updated).";
            }

            // Add per-table error messages
            foreach (var result in allTableResults.Where(r => r.errors.Count > 0))
            {
                response.ErrorMessages.Add($"Table '{result.tableName}': {string.Join("; ", result.errors)}");
            }

            if (response.RowErrors.Count > 0)
            {
                response.ErrorMessages.Add($"{response.RowErrors.Count} row(s) failed during data preparation. See RowErrors for details.");
            }
        }
        catch (Exception ex)
        {
            response.ErrorMessages.Add($"Error during ElectricalInstrumentationDBO migration: {ex.Message}");
            if (ex.InnerException != null)
            {
                response.ErrorMessages.Add($"Inner exception: {ex.InnerException.Message}");
            }
        }

        return response;
    }

    private async Task<List<string>> GetTablesWithPrefixAsync(
        SqlConnection connection,
        string schemaName,
        string tablePrefix,
        CancellationToken cancellationToken)
    {
        var tables = new List<string>();

        var query = @"
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = @SchemaName
                AND TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME LIKE @TablePrefix + '%'
            ORDER BY TABLE_NAME";

        await using var command = new SqlCommand(query, connection);
        command.CommandTimeout = SqlCommandTimeout;
        command.Parameters.AddWithValue("@SchemaName", schemaName);
        command.Parameters.AddWithValue("@TablePrefix", tablePrefix);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            tables.Add(reader.GetString(0));
        }

        return tables;
    }

    private async Task<UploadResponse> MigrateToSingleTableAsync(
        SqlConnection connection,
        string schemaName,
        string tableName,
        DataTable excelData,
        string? attachmentRecordType = null,
        CancellationToken cancellationToken = default)
    {
        var response = new UploadResponse();

        // Use a separate transaction for each table to ensure isolation
        var transaction = connection.BeginTransaction();
        var tempTableName = $"#TMP_{Guid.NewGuid():N}";

        try
        {
            // Step 1: Get target table metadata
            var tableMetadata = await GetTableMetadataAsync(connection, transaction, schemaName, tableName, cancellationToken);

            if (tableMetadata.Count == 0)
            {
                response.ErrorMessages.Add($"Table '{schemaName}.{tableName}' not found or has no columns.");
                transaction.Rollback();
                return response;
            }

            // Step 2: Match Excel columns to SQL columns
            var columnMappings = MatchColumns(excelData, tableMetadata, tableName, attachmentRecordType);

            // SPECIAL RULE: Ensure the first Excel column is mapped to the Primary Key
            // This supports the requirement: "use the value from the first column of the Excel sheet as the primary key"
            // EXCEPTION: Skip this rule for BPComments and BPAttachments where IDs should typically be auto-generated to avoid conflicts (e.g. -1 values)
            bool shouldAutoMapPk = !string.Equals(tableName, "BPComments", StringComparison.OrdinalIgnoreCase) && 
                                   !string.Equals(tableName, "BPAttachments", StringComparison.OrdinalIgnoreCase);

            if (shouldAutoMapPk && excelData.Columns.Count > 0)
            {
                var primaryKeyColumn = tableMetadata.FirstOrDefault(m => m.IsPrimaryKey);
                if (primaryKeyColumn != null)
                {
                    // Check if PK is already mapped
                    var isPkMapped = columnMappings.Any(m => m.SqlColumnName.Equals(primaryKeyColumn.ColumnName, StringComparison.OrdinalIgnoreCase));
                    
                    if (!isPkMapped)
                    {
                        // Map first Excel column to Primary Key
                        columnMappings.Insert(0, new ColumnMapping
                        {
                            ExcelColumnName = excelData.Columns[0].ColumnName,
                            SqlColumnName = primaryKeyColumn.ColumnName,
                            SqlDataType = primaryKeyColumn.DataType,
                            IsIdentity = primaryKeyColumn.IsIdentity,
                            IsNullable = primaryKeyColumn.IsNullable
                        });
                    }
                }
            }

            if (columnMappings.Count == 0)
            {
                response.ErrorMessages.Add($"No matching columns found between Excel and SQL table '{tableName}'.");
                transaction.Rollback();
                return response;
            }

            // Step 3: Check for identity column
            var identityColumn = tableMetadata.FirstOrDefault(m => m.IsIdentity);
            var hasIdentityInExcel = identityColumn != null &&
                                    columnMappings.Any(m => m.SqlColumnName.Equals(identityColumn.ColumnName, StringComparison.OrdinalIgnoreCase));

            // Step 4: Create temp table
            await CreateTempTableAsync(connection, transaction, tempTableName, tableMetadata, cancellationToken);

            // Step 5: Prepare DataTable with only matched columns
            var (mappedDataTable, rowErrors) = await PrepareMappedDataTableAsync(connection, transaction, excelData, columnMappings, tableName, schemaName, tableMetadata, cancellationToken);

            // Add row errors to response
            response.RowErrors.AddRange(rowErrors);

            // Step 6: Bulk copy to temp table
            var rowsCopiedToTemp = await BulkCopyToTempTableAsync(
                connection,
                transaction,
                tempTableName,
                mappedDataTable,
                columnMappings,
                hasIdentityInExcel,
                cancellationToken);

            // Step 7: Get primary key columns
            var primaryKeyColumns = tableMetadata.Where(m => m.IsPrimaryKey).Select(m => m.ColumnName).ToList();

            // Step 8: Upsert from temp table to target table using MERGE
            var (rowsInserted, rowsUpdated) = await MergeFromTempToTargetAsync(
                connection,
                transaction,
                schemaName,
                tableName,
                tempTableName,
                columnMappings,
                primaryKeyColumns,
                identityColumn,
                hasIdentityInExcel,
                cancellationToken);

            transaction.Commit();

            response.Success = rowErrors.Count == 0;
            response.RowsInserted = rowsInserted;
            response.RowsUpdated = rowsUpdated;
            response.RowsFailed = rowErrors.Count;

        }
        catch (Exception ex)
        {
            transaction.Rollback();
            response.ErrorMessages.Add($"Error migrating to table '{tableName}': {ex.Message}");
            if (ex.InnerException != null)
            {
                response.ErrorMessages.Add($"Inner exception: {ex.InnerException.Message}");
            }
        }
        finally
        {
            // Clean up temp table
            try
            {
                await DropTempTableAsync(connection, transaction, tempTableName, cancellationToken);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        return response;
    }

    private async Task<List<ColumnMetadata>> GetTableMetadataAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        var metadata = new List<ColumnMetadata>();

        var query = @"
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                CASE WHEN ic.OBJECT_ID IS NOT NULL THEN 1 ELSE 0 END AS IS_IDENTITY,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN sys.identity_columns ic ON ic.object_id = OBJECT_ID(@SchemaTable) 
                AND ic.name = c.COLUMN_NAME
            LEFT JOIN (
                SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                    AND tc.TABLE_NAME = ku.TABLE_NAME
            ) pk ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA 
                AND pk.TABLE_NAME = c.TABLE_NAME 
                AND pk.COLUMN_NAME = c.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = @SchemaName 
                AND c.TABLE_NAME = @TableName
            ORDER BY c.ORDINAL_POSITION";

        await using var command = new SqlCommand(query, connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        command.Parameters.AddWithValue("@SchemaName", schemaName);
        command.Parameters.AddWithValue("@TableName", tableName);
        command.Parameters.AddWithValue("@SchemaTable", $"{schemaName}.{tableName}");

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            metadata.Add(new ColumnMetadata
            {
                ColumnName = reader.GetString(0),
                DataType = reader.GetString(1),
                IsNullable = reader.GetString(2) == "YES",
                MaxLength = reader.IsDBNull(3) ? (int?)null : Convert.ToInt32(reader.GetValue(3)),
                NumericPrecision = reader.IsDBNull(4) ? (int?)null : Convert.ToInt32(reader.GetValue(4)),
                NumericScale = reader.IsDBNull(5) ? (int?)null : Convert.ToInt32(reader.GetValue(5)),
                IsIdentity = Convert.ToInt32(reader.GetValue(6)) == 1,
                IsPrimaryKey = Convert.ToInt32(reader.GetValue(7)) == 1
            });
        }

        return metadata;
    }

    private async Task<string?> FindLookupColumnAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string? parentTableSchema,
        string? parentTableName,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(parentTableSchema) || string.IsNullOrWhiteSpace(parentTableName))
            return null;

        // Common column names to search for (in order of preference)
        var lookupColumnNames = new[] { "Name", "ContactName", "Description", "Title", "DisplayName", "FullName" };

        var query = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @SchemaName
                AND TABLE_NAME = @TableName
                AND COLUMN_NAME IN ('Name', 'ContactName', 'Description', 'Title', 'DisplayName', 'FullName')
            ORDER BY CASE COLUMN_NAME
                WHEN 'Name' THEN 1
                WHEN 'ContactName' THEN 2
                WHEN 'Description' THEN 3
                WHEN 'Title' THEN 4
                WHEN 'DisplayName' THEN 5
                WHEN 'FullName' THEN 6
                ELSE 99
            END";

        try
        {
            await using var command = new SqlCommand(query, connection, transaction);
            command.CommandTimeout = SqlCommandTimeout;
            command.Parameters.AddWithValue("@SchemaName", parentTableSchema);
            command.Parameters.AddWithValue("@TableName", parentTableName);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                return reader.GetString(0);
            }
        }
        catch
        {
            // If lookup fails, return null - we'll try direct ID conversion
        }

        return null;
    }

    private bool IsZeroValue(object value)
    {
        if (value == null || value == DBNull.Value)
            return false;

        return value switch
        {
            int intVal => intVal == 0,
            long longVal => longVal == 0,
            short shortVal => shortVal == 0,
            byte byteVal => byteVal == 0,
            decimal decimalVal => decimalVal == 0,
            double doubleVal => doubleVal == 0,
            float floatVal => floatVal == 0,
            string strVal => strVal == "0" || string.IsNullOrWhiteSpace(strVal),
            _ => false
        };
    }

    private async Task<bool> ValidateForeignKeyValueAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string fkTableSchema,
        string fkTableName,
        string fkColumnName,
        object fkValue,
        CancellationToken cancellationToken)
    {
        try
        {
            var query = $@"
                SELECT COUNT(1)
                FROM [{fkTableSchema}].[{fkTableName}]
                WHERE [{fkColumnName}] = @FkValue";

            await using var command = new SqlCommand(query, connection, transaction);
            command.CommandTimeout = SqlCommandTimeout;
            command.Parameters.AddWithValue("@FkValue", fkValue);

            var count = await command.ExecuteScalarAsync(cancellationToken);
            return count != null && Convert.ToInt32(count) > 0;
        }
        catch
        {
            // If validation fails (e.g., table doesn't exist), return false to skip the row
            return false;
        }
    }

    private async Task<object?> LookupForeignKeyValueAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string parentTableSchema,
        string parentTableName,
        string parentKeyColumn,
        string? lookupColumnName,
        object excelValue,
        CancellationToken cancellationToken)
    {
        if (excelValue == null || excelValue == DBNull.Value)
            return DBNull.Value;

        var lookupValue = excelValue.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(lookupValue))
            return DBNull.Value;

        try
        {
            // First, try to see if the Excel value is already a numeric ID
            // If it's numeric, try direct lookup by ID first
            object? numericIdValue = null;
            if (long.TryParse(lookupValue, out var numericId))
            {
                numericIdValue = numericId;
            }
            else if (int.TryParse(lookupValue, out var intId))
            {
                numericIdValue = intId;
            }

            if (numericIdValue != null)
            {
                var directQuery = $@"
                    SELECT TOP 1 [{parentKeyColumn}]
                    FROM [{parentTableSchema}].[{parentTableName}]
                    WHERE [{parentKeyColumn}] = @DirectId";

                await using var directCommand = new SqlCommand(directQuery, connection, transaction);
                directCommand.CommandTimeout = SqlCommandTimeout;
                directCommand.Parameters.AddWithValue("@DirectId", numericIdValue);

                var directResult = await directCommand.ExecuteScalarAsync(cancellationToken);
                if (directResult != null && directResult != DBNull.Value)
                {
                    return directResult;
                }
            }

            // If direct ID lookup failed or value is not numeric, try lookup by name/description
            if (!string.IsNullOrWhiteSpace(lookupColumnName))
            {
                var nameQuery = $@"
                    SELECT TOP 1 [{parentKeyColumn}]
                    FROM [{parentTableSchema}].[{parentTableName}]
                    WHERE [{lookupColumnName}] = @LookupValue
                    ORDER BY [{parentKeyColumn}]";

                await using var nameCommand = new SqlCommand(nameQuery, connection, transaction);
                nameCommand.CommandTimeout = SqlCommandTimeout;
                nameCommand.Parameters.AddWithValue("@LookupValue", lookupValue);

                var nameResult = await nameCommand.ExecuteScalarAsync(cancellationToken);
                if (nameResult != null && nameResult != DBNull.Value)
                {
                    return nameResult;
                }
            }

            // If both lookups failed, return DBNull (will be handled as conversion error)
            return DBNull.Value;
        }
        catch
        {
            // If lookup fails, return DBNull - let the conversion handle it
            // (might be a direct ID that needs type conversion)
            return DBNull.Value;
        }
    }

    private List<ColumnMapping> MatchColumns(DataTable excelData, List<ColumnMetadata> tableMetadata, string tableName, string? attachmentRecordType = null)
    {
        var mappings = new List<ColumnMapping>();

        // Check if this is CommunicationProtocol table - use hardcoded mapping
        if (string.Equals(tableName, "CommunicationProtocol", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForCommunicationProtocol(excelData, tableMetadata);
        }
		if (string.Equals(tableName, "ContractClearance", StringComparison.OrdinalIgnoreCase))
		{
			return MatchColumnsForContractClearance(excelData, tableMetadata);
		}

		// Check if this is BankGuarantee table - use hardcoded mapping
		if (string.Equals(tableName, "BankGuarantee", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForBankGuarantee(excelData, tableMetadata);
        }

        // Check if this is CustomerMaster table - use hardcoded mapping
        if (string.Equals(tableName, "CustomerMaster", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForCustomerMaster(excelData, tableMetadata);
        }

        // Check if this is CustomerContacts table - use hardcoded mapping
        if (string.Equals(tableName, "CustomerContacts", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForCustomerContacts(excelData, tableMetadata);
        }

        // Check if this is VendorMaster table - use hardcoded mapping
        if (string.Equals(tableName, "VendorMaster", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForVendorMaster(excelData, tableMetadata);
        }

        // Check if this is BPAttachments table - use hardcoded mapping with dynamic selection based on AttachmentRecordType
        if (string.Equals(tableName, "BPAttachments", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForBPAttachments(excelData, tableMetadata, attachmentRecordType);
        }

        // Check if this is Project table - use hardcoded mapping
        if (string.Equals(tableName, "Project", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForProject(excelData, tableMetadata);
        }

        // Check if this is BPComments table - use hardcoded mapping
        if (string.Equals(tableName, "BPComments", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForBPComments(excelData, tableMetadata, attachmentRecordType);
        }

        // Check if table name starts with "Turbine" - use hardcoded mapping
        if (tableName.StartsWith("Turbine", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForTurbine(excelData, tableMetadata);
        }

        // Check if table name starts with "MechanicalDBO" - use hardcoded mapping
        if (tableName.StartsWith("MechanicalDBO", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForMechanicalDBO(excelData, tableMetadata);
        }

        // Check if table name starts with "OrderTransmittal" - use hardcoded mapping
        if (tableName.StartsWith("OrderTransmittal", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForOrderTransmittal(excelData, tableMetadata);
        }

        // Check if table name starts with "ElectricalInstrumentationDBO" - use hardcoded mapping
        if (tableName.StartsWith("ElectricalInstrumentationDBO", StringComparison.OrdinalIgnoreCase))
        {
            return MatchColumnsForElectricalInstrumentationDBO(excelData, tableMetadata);
        }

        // For other tables, use existing dynamic matching logic
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        foreach (var sqlColumn in tableMetadata)
        {
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(sqlColumn.ColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn != null)
            {
                mappings.Add(new ColumnMapping
                {
                    ExcelColumnName = excelColumn.ColumnName,
                    SqlColumnName = sqlColumn.ColumnName,
                    SqlDataType = sqlColumn.DataType,
                    IsIdentity = sqlColumn.IsIdentity,
                    IsNullable = sqlColumn.IsNullable,
                    ForeignKeyTableSchema = sqlColumn.ForeignKeyTableSchema,
                    ForeignKeyTableName = sqlColumn.ForeignKeyTableName,
                    ForeignKeyColumnName = sqlColumn.ForeignKeyColumnName,
                    ForeignKeyLookupColumnName = sqlColumn.ForeignKeyLookupColumnName
                });
            }
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForCommunicationProtocol(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in CommunicationProtocolColumnMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForBankGuarantee(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in BankGuaranteeMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForCustomerMaster(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in CustomerMasterMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Handle composite address field (contains "+")
            if (excelColumnName.Contains("+"))
            {
                // Split by "+" to get individual Excel column names
                var addressColumns = excelColumnName.Split('+', StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Trim())
                    .ToList();

                // Check if all address columns exist in Excel
                var allAddressColumnsExist = addressColumns.All(col =>
                    excelColumns.Any(ec => ec.ColumnName.Equals(col, StringComparison.OrdinalIgnoreCase)));

                // Check if SQL table has the Address column
                if (allAddressColumnsExist && sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                {
                    // Add a special mapping for composite address
                    // We'll use a special marker in ExcelColumnName to identify composite fields
                    // and handle concatenation in PrepareMappedDataTableAsync
                    mappings.Add(new ColumnMapping
                    {
                        ExcelColumnName = excelColumnName, // Store the full composite key for identification
                        SqlColumnName = sqlColumn.ColumnName,
                        SqlDataType = sqlColumn.DataType,
                        IsIdentity = sqlColumn.IsIdentity,
                        IsNullable = sqlColumn.IsNullable
                    });
                }
                continue;
            }

            // Regular single column mapping
            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumnRegular))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumnRegular.ColumnName,
                SqlDataType = sqlColumnRegular.DataType,
                IsIdentity = sqlColumnRegular.IsIdentity,
                IsNullable = sqlColumnRegular.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForCustomerContacts(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in CustomerContactMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Handle composite address field (contains "+")
            if (excelColumnName.Contains("+"))
            {
                // Split by "+" to get individual Excel column names
                var addressColumns = excelColumnName.Split('+', StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Trim())
                    .ToList();

                // Check if all address columns exist in Excel
                var allAddressColumnsExist = addressColumns.All(col =>
                    excelColumns.Any(ec => ec.ColumnName.Equals(col, StringComparison.OrdinalIgnoreCase)));

                // Check if SQL table has the Address column
                if (allAddressColumnsExist && sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                {
                    // Add a special mapping for composite address
                    // We'll use a special marker in ExcelColumnName to identify composite fields
                    // and handle concatenation in PrepareMappedDataTableAsync
                    mappings.Add(new ColumnMapping
                    {
                        ExcelColumnName = excelColumnName, // Store the full composite key for identification
                        SqlColumnName = sqlColumn.ColumnName,
                        SqlDataType = sqlColumn.DataType,
                        IsIdentity = sqlColumn.IsIdentity,
                        IsNullable = sqlColumn.IsNullable
                    });
                }
                continue;
            }

            // Regular single column mapping
            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumnRegular))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumnRegular.ColumnName,
                SqlDataType = sqlColumnRegular.DataType,
                IsIdentity = sqlColumnRegular.IsIdentity,
                IsNullable = sqlColumnRegular.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForVendorMaster(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in VendorMasterMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Handle composite address field (contains "+")
            if (excelColumnName.Contains("+"))
            {
                // Split by "+" to get individual Excel column names
                var addressColumns = excelColumnName.Split('+', StringSplitOptions.RemoveEmptyEntries)
                    .Select(c => c.Trim())
                    .ToList();

                // Check if all address columns exist in Excel
                var allAddressColumnsExist = addressColumns.All(col =>
                    excelColumns.Any(ec => ec.ColumnName.Equals(col, StringComparison.OrdinalIgnoreCase)));

                // Check if SQL table has the Address column
                if (allAddressColumnsExist && sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                {
                    // Add a special mapping for composite address
                    // We'll use a special marker in ExcelColumnName to identify composite fields
                    // and handle concatenation in PrepareMappedDataTableAsync
                    mappings.Add(new ColumnMapping
                    {
                        ExcelColumnName = excelColumnName, // Store the full composite key for identification
                        SqlColumnName = sqlColumn.ColumnName,
                        SqlDataType = sqlColumn.DataType,
                        IsIdentity = sqlColumn.IsIdentity,
                        IsNullable = sqlColumn.IsNullable
                    });
                }
                continue;
            }

            // Regular single column mapping
            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumnRegular))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumnRegular.ColumnName,
                SqlDataType = sqlColumnRegular.DataType,
                IsIdentity = sqlColumnRegular.IsIdentity,
                IsNullable = sqlColumnRegular.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForBPComments(DataTable excelData, List<ColumnMetadata> tableMetadata, string attachmentRecordType)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary

        foreach (var mappingEntry in BPCommentsMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Find the Excel column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column doesn't exist

            // Find the SQL column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column doesn't exist

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable
            });

            // Special handling for parent_object_id: map to other RecordIDs based on BP ID logic
            // Since we are inside the loop iterating BPCommentsMapping, and it contains "parent_object_id" -> "OrderTransmittalRecordID"
            // we check if this is that entry, then add the others.
            //if (excelColumnName.Equals("parent_object_id", StringComparison.OrdinalIgnoreCase))
            //{
            //    // List of other potential target columns
            //    var otherTargetColumns = new[] { "TurbineRecordID", "MechanicalDBORecordID", "ElectricalInstrumentationDBORecordID", "OrderTransmittalRecordID", "BankGuaranteeRecordID" };

            //    foreach (var targetCol in otherTargetColumns)
            //    {
            //        if (sqlColumnLookup.TryGetValue(targetCol, out var targetSqlColumn))
            //        {
            //            mappings.Add(new ColumnMapping
            //            {
            //                ExcelColumnName = excelColumn.ColumnName,
            //                SqlColumnName = targetSqlColumn.ColumnName,
            //                SqlDataType = targetSqlColumn.DataType,
            //                IsIdentity = targetSqlColumn.IsIdentity,
            //                IsNullable = targetSqlColumn.IsNullable
            //            });
            //        }
            //    }
            //}
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForTurbine(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in TurbineMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Find the Excel column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column doesn't exist

            // Find the SQL column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column doesn't exist

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForBPAttachments(DataTable excelData, List<ColumnMetadata> tableMetadata, string? attachmentRecordType = null)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Select the appropriate mapping dictionary based on AttachmentRecordType
        Dictionary<string, string> selectedMapping;
        if (!string.IsNullOrWhiteSpace(attachmentRecordType))
        {
            if (string.Equals(attachmentRecordType, "Comment", StringComparison.OrdinalIgnoreCase))
            {
                selectedMapping = BPAttachmentCommentMapping;
            }
            else if (string.Equals(attachmentRecordType, "OrderTransmittal", StringComparison.OrdinalIgnoreCase))
            {
                selectedMapping = BPAttachmentOTMapping;
            }
            else
            {
                // Default to BPAttachmentMapping for unknown types
                selectedMapping = BPAttachmentMapping;
            }
        }
        else
        {
            // Default to BPAttachmentMapping if no AttachmentRecordType is provided
            selectedMapping = BPAttachmentMapping;
        }

        // Check if Excel has parent_type column (needed for conditional mapping)
        var hasParentTypeColumn = excelColumns.Any(ec =>
            ec.ColumnName.Equals("parent_type", StringComparison.OrdinalIgnoreCase));

        // Iterate through the selected hardcoded mapping dictionary
        foreach (var mappingEntry in selectedMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Handle file_name mapping to both FileName and FilePath
            if (excelColumnName.Equals("file_name", StringComparison.OrdinalIgnoreCase))
            {
                // Map to FileName
                if (sqlColumnLookup.TryGetValue("FileName", out var fileNameColumn))
                {
                    var excelColumn = excelColumns.FirstOrDefault(
                        ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));
                    if (excelColumn != null)
                    {
                        mappings.Add(new ColumnMapping
                        {
                            ExcelColumnName = excelColumn.ColumnName,
                            SqlColumnName = fileNameColumn.ColumnName,
                            SqlDataType = fileNameColumn.DataType,
                            IsIdentity = fileNameColumn.IsIdentity,
                            IsNullable = fileNameColumn.IsNullable
                        });
                    }
                }

                // Map to FilePath
                if (sqlColumnLookup.TryGetValue("FilePath", out var filePathColumn))
                {
                    var excelColumn = excelColumns.FirstOrDefault(
                        ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));
                    if (excelColumn != null)
                    {
                        mappings.Add(new ColumnMapping
                        {
                            ExcelColumnName = excelColumn.ColumnName,
                            SqlColumnName = filePathColumn.ColumnName,
                            SqlDataType = filePathColumn.DataType,
                            IsIdentity = filePathColumn.IsIdentity,
                            IsNullable = filePathColumn.IsNullable
                        });
                    }
                }
                continue;
            }

            // Handle parent_id - conditionally map to OrderTransmittalRecordID and other RecordIDs
            // We'll add the mapping but handle the conditional logic in PrepareMappedDataTableAsync


            // Regular single column mapping
            var excelColumnRegular = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumnRegular == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumnRegular))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumnRegular.ColumnName,
                SqlColumnName = sqlColumnRegular.ColumnName,
                SqlDataType = sqlColumnRegular.DataType,
                IsIdentity = sqlColumnRegular.IsIdentity,
                IsNullable = sqlColumnRegular.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForProject(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in ProjectMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForMechanicalDBO(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in MechanicalDBOMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForOrderTransmittal(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in OrderTransmittalMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable,
                ForeignKeyTableSchema = sqlColumn.ForeignKeyTableSchema,
                ForeignKeyTableName = sqlColumn.ForeignKeyTableName,
                ForeignKeyColumnName = sqlColumn.ForeignKeyColumnName,
                ForeignKeyLookupColumnName = sqlColumn.ForeignKeyLookupColumnName
            });
        }

        return mappings;
    }

    private List<ColumnMapping> MatchColumnsForElectricalInstrumentationDBO(DataTable excelData, List<ColumnMetadata> tableMetadata)
    {
        var mappings = new List<ColumnMapping>();
        var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

        // Create a lookup for SQL column metadata by column name (case-insensitive)
        var sqlColumnLookup = tableMetadata.ToDictionary(
            m => m.ColumnName,
            m => m,
            StringComparer.OrdinalIgnoreCase);

        // Iterate through the hardcoded mapping dictionary
        foreach (var mappingEntry in ElectricalInstrumentationDBOMapping)
        {
            var excelColumnName = mappingEntry.Key;
            var sqlColumnName = mappingEntry.Value;

            // Check if Excel has this column
            var excelColumn = excelColumns.FirstOrDefault(
                ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

            if (excelColumn == null)
                continue; // Skip if Excel column not found

            // Check if SQL table has the mapped column
            if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
                continue; // Skip if SQL column not found in metadata

            // Add the mapping
            mappings.Add(new ColumnMapping
            {
                ExcelColumnName = excelColumn.ColumnName,
                SqlColumnName = sqlColumn.ColumnName,
                SqlDataType = sqlColumn.DataType,
                IsIdentity = sqlColumn.IsIdentity,
                IsNullable = sqlColumn.IsNullable,
                ForeignKeyTableSchema = sqlColumn.ForeignKeyTableSchema,
                ForeignKeyTableName = sqlColumn.ForeignKeyTableName,
                ForeignKeyColumnName = sqlColumn.ForeignKeyColumnName,
                ForeignKeyLookupColumnName = sqlColumn.ForeignKeyLookupColumnName
            });
        }

        return mappings;
    }



	private List<ColumnMapping> MatchColumnsForContractClearance(DataTable excelData, List<ColumnMetadata> tableMetadata)
	{
		var mappings = new List<ColumnMapping>();
		var excelColumns = excelData.Columns.Cast<DataColumn>().ToList();

		// Create a lookup for SQL column metadata by column name (case-insensitive)
		var sqlColumnLookup = tableMetadata.ToDictionary(
			m => m.ColumnName,
			m => m,
			StringComparer.OrdinalIgnoreCase);

		// Iterate through the hardcoded mapping dictionary
		foreach (var mappingEntry in ContractClearanceMapping)
		{
			var excelColumnName = mappingEntry.Key;
			var sqlColumnName = mappingEntry.Value;

			// Check if Excel has this column
			var excelColumn = excelColumns.FirstOrDefault(
				ec => ec.ColumnName.Equals(excelColumnName, StringComparison.OrdinalIgnoreCase));

			if (excelColumn == null)
				continue; // Skip if Excel column not found

			// Check if SQL table has the mapped column
			if (!sqlColumnLookup.TryGetValue(sqlColumnName, out var sqlColumn))
				continue; // Skip if SQL column not found in metadata

			// Add the mapping
			mappings.Add(new ColumnMapping
			{
				ExcelColumnName = excelColumn.ColumnName,
				SqlColumnName = sqlColumn.ColumnName,
				SqlDataType = sqlColumn.DataType,
				IsIdentity = sqlColumn.IsIdentity,
				IsNullable = sqlColumn.IsNullable
			});
		}

		return mappings;
	}

	private async Task CreateTempTableAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string tempTableName,
        List<ColumnMetadata> metadata,
        CancellationToken cancellationToken)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE {tempTableName} (");

        var columns = new List<string>();
        foreach (var col in metadata)
        {
            var columnDef = $"[{col.ColumnName}] {GetSqlTypeDefinition(col)}";
            columns.Add(columnDef);
        }

        sb.Append(string.Join(", ", columns));
        sb.Append(")");

        await using var command = new SqlCommand(sb.ToString(), connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private string GetSqlTypeDefinition(ColumnMetadata metadata)
    {
        var type = metadata.DataType.ToUpper();

        switch (type)
        {
            case "VARCHAR":
            case "NVARCHAR":
            case "CHAR":
            case "NCHAR":
                var length = metadata.MaxLength ?? 255;
                if (length == -1) length = 4000; // MAX
                return $"{type}({length})";

            case "DECIMAL":
            case "NUMERIC":
                var precision = metadata.NumericPrecision ?? 18;
                var scale = metadata.NumericScale ?? 0;
                return $"{type}({precision},{scale})";

            case "FLOAT":
                return metadata.NumericPrecision.HasValue
                    ? $"{type}({metadata.NumericPrecision.Value})"
                    : "FLOAT";

            default:
                return type;
        }
    }

    private async Task<(DataTable mappedTable, List<Models.RowErrorDetail> rowErrors)> PrepareMappedDataTableAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        DataTable excelData,
        List<ColumnMapping> mappings,
        string tableName,
        string schemaName,
        List<ColumnMetadata> tableMetadata,
        CancellationToken cancellationToken)
    {
        var mappedTable = new DataTable();
        var rowErrors = new List<Models.RowErrorDetail>();
        var isCommunicationProtocol = string.Equals(tableName, "CommunicationProtocol", StringComparison.OrdinalIgnoreCase);
        var isBankGuarantee = string.Equals(tableName, "BankGuarantee", StringComparison.OrdinalIgnoreCase);
        var isCustomerMaster = string.Equals(tableName, "CustomerMaster", StringComparison.OrdinalIgnoreCase);
        var isCustomerContacts = string.Equals(tableName, "CustomerContacts", StringComparison.OrdinalIgnoreCase);
        var isVendorMaster = string.Equals(tableName, "VendorMaster", StringComparison.OrdinalIgnoreCase);
        var isBPAttachments = string.Equals(tableName, "BPAttachments", StringComparison.OrdinalIgnoreCase);
        var isBPComments = string.Equals(tableName, "BPComments", StringComparison.OrdinalIgnoreCase);
        var isTurbine = tableName.StartsWith("Turbine", StringComparison.OrdinalIgnoreCase);
        var isProject = string.Equals(tableName, "Project", StringComparison.OrdinalIgnoreCase);
        var isMechanicalDBO = tableName.StartsWith("MechanicalDBO", StringComparison.OrdinalIgnoreCase);
        var isOrderTransmittal = tableName.StartsWith("OrderTransmittal", StringComparison.OrdinalIgnoreCase);
        var isElectricalInstrumentationDBO = tableName.StartsWith("ElectricalInstrumentationDBO", StringComparison.OrdinalIgnoreCase);

        // Performance optimization: Cache FK lookups to avoid repeated database queries
        var projectIdCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var unitIdCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var customerIdCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var orderTransmittalIdCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        var projectTypeMasterIdCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        // Add columns in the order of mappings
        foreach (var mapping in mappings)
        {
            // Handle composite address field (contains "+")
            if (mapping.ExcelColumnName.Contains("+"))
            {
                // For composite fields, determine type from SQL metadata
                var targetNetType = GetNetTypeFromSqlType(mapping.SqlDataType);
                if (targetNetType == null)
                {
                    targetNetType = typeof(string); // Default to string for composite address
                }

                var newColumn = new DataColumn(mapping.SqlColumnName, targetNetType)
                {
                    AllowDBNull = mapping.IsNullable
                };
                mappedTable.Columns.Add(newColumn);
                continue;
            }

            var excelColumn = excelData.Columns[mapping.ExcelColumnName];
            if (excelColumn == null)
                continue; // Skip if column not found (should not happen due to matching)

            // Determine the target .NET type based on SQL Server data type
            var targetNetTypeRegular = GetNetTypeFromSqlType(mapping.SqlDataType);
            if (targetNetTypeRegular == null)
            {
                // Fallback to Excel column type if SQL type mapping fails
                targetNetTypeRegular = excelColumn.DataType;
            }

            var newColumnRegular = new DataColumn(mapping.SqlColumnName, targetNetTypeRegular)
            {
                AllowDBNull = mapping.IsNullable
            };
            mappedTable.Columns.Add(newColumnRegular);
        }

        // Check if IsDeleted column exists in SQL table but not in mappings - add it if needed
        var isDeletedColumn = tableMetadata?.FirstOrDefault(m =>
            string.Equals(m.ColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase));
        if (isDeletedColumn != null && !mappings.Any(m =>
            string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase)))
        {
            var isDeletedNetType = GetNetTypeFromSqlType(isDeletedColumn.DataType);
            if (isDeletedNetType == null)
            {
                isDeletedNetType = typeof(bool); // Default to bool for BIT type
            }
            var isDeletedDataColumn = new DataColumn("IsDeleted", isDeletedNetType)
            {
                AllowDBNull = isDeletedColumn.IsNullable
            };
            mappedTable.Columns.Add(isDeletedDataColumn);
        }

        // Copy data
        int rowNumber = 1; // Excel row number (1-based, including header)
        foreach (DataRow excelRow in excelData.Rows)
        {
            rowNumber++; // Increment for data rows (header is row 1)
            var newRow = mappedTable.NewRow();

            // Set IsDeleted to false immediately if the column exists (before processing other columns)
            if (mappedTable.Columns.Contains("IsDeleted"))
            {
                var isDeletedDataColumn = mappedTable.Columns["IsDeleted"];
                if (isDeletedDataColumn != null && isDeletedDataColumn.DataType == typeof(bool))
                {
                    newRow["IsDeleted"] = false;
                }
                else if (isDeletedDataColumn != null)
                {
                    // For other types (int, bit as int, etc.), set to 0
                    newRow["IsDeleted"] = Convert.ChangeType(0, isDeletedDataColumn.DataType);
                }
            }

            bool skipRow = false;
            string? errorColumn = null;
            object? errorValue = null;
            string? errorMessage = null;
            var rowData = new Dictionary<string, object?>();

            // Collect all row data for error reporting
            foreach (var mapping in mappings)
            {
                try
                {
                    // Handle composite address field (contains "+")
                    if (mapping.ExcelColumnName.Contains("+"))
                    {
                        // For composite fields, collect all component columns
                        var addressColumns = mapping.ExcelColumnName.Split('+', StringSplitOptions.RemoveEmptyEntries)
                            .Select(c => c.Trim())
                            .ToList();
                        foreach (var addrCol in addressColumns)
                        {
                            if (excelData.Columns.Contains(addrCol))
                            {
                                rowData[addrCol] = excelRow[addrCol];
                            }
                        }
                    }
                    else
                    {
                        var value = excelRow[mapping.ExcelColumnName];
                        rowData[mapping.ExcelColumnName] = value;
                    }
                }
                catch
                {
                    // Ignore errors when collecting row data
                }
            }

            foreach (var mapping in mappings)
            {
                try
                {
                    object? value;

                    // Handle composite address field (contains "+")
                    if (mapping.ExcelColumnName.Contains("+"))
                    {
                        // Split by "+" to get individual Excel column names
                        var addressColumns = mapping.ExcelColumnName.Split('+', StringSplitOptions.RemoveEmptyEntries)
                            .Select(c => c.Trim())
                            .ToList();

                        // Concatenate address columns with space separator
                        var addressParts = new List<string>();
                        foreach (var addrCol in addressColumns)
                        {
                            if (excelData.Columns.Contains(addrCol))
                            {
                                var addrValue = excelRow[addrCol];
                                if (addrValue != null && addrValue != DBNull.Value)
                                {
                                    var addrStr = addrValue.ToString()?.Trim();
                                    if (!string.IsNullOrWhiteSpace(addrStr))
                                    {
                                        addressParts.Add(addrStr);
                                    }
                                }
                            }
                        }
                        value = string.Join(" ", addressParts);
                        if (string.IsNullOrWhiteSpace(value?.ToString()))
                        {
                            value = DBNull.Value;
                        }
                    }
                    else
                    {
                        value = excelRow[mapping.ExcelColumnName];
                    }



                    // Special handling for ProjectID column (FK to master.Project)
                    // Validate ProjectID exists in master.Project table
                    // Skip validation if table is "Project" itself (ProjectID is primary key, not FK)
                    if (!isProject &&
                        string.Equals(mapping.SqlColumnName, "ProjectID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        // If value is numeric 0, convert to NULL
                        if ((value is int intVal && intVal == 0) ||
                            (value is long longVal && longVal == 0) ||
                            (value is short shortVal && shortVal == 0) ||
                            (long.TryParse(value.ToString()?.Trim(), out var projectNumeric) && projectNumeric == 0))
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!projectIdCache.TryGetValue(valueKey, out var resolvedProjectId))
                            {
                                // Not in cache, resolve from database
                                resolvedProjectId = await ResolveProjectIdAsync(
                                    connection,
                                    transaction,
                                    value,
                                    cancellationToken);

                                // Cache the result (even if null)
                                projectIdCache[valueKey] = resolvedProjectId;
                            }

                            if (resolvedProjectId == null)
                            {
                                // For CommunicationProtocol, OrderTransmittal, BankGuarantee, Turbine, and ElectricalInstrumentationDBO, if ProjectID doesn't exist, set to NULL instead of skipping row
                                if (isCommunicationProtocol || isOrderTransmittal || isBankGuarantee || isTurbine || isElectricalInstrumentationDBO || isBPAttachments || isMechanicalDBO)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    // For other tables, skip row if ProjectID doesn't exist
                                    errorColumn = mapping.ExcelColumnName;
                                    errorValue = value;
                                    errorMessage = $"Foreign key constraint violation: ProjectID '{value}' does not exist in table 'master.Project'";
                                    skipRow = true;
                                    break;
                                }
                            }
                            else
                            {
                                value = resolvedProjectId;
                            }
                        }
                    }

                    // Special handling for CloneProjectId in MechanicalDBO (FK to master.Project)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CloneProjectId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                         // Reuse the ProjectID resolution logic
                         var valueKey = value.ToString()?.Trim() ?? string.Empty;
                         // Check cache first (using projectIdCache since it's the same target table)
                         if (!projectIdCache.TryGetValue(valueKey, out var resolvedProjectId))
                         {
                             resolvedProjectId = await ResolveProjectIdAsync(connection, transaction, value, cancellationToken);
                             projectIdCache[valueKey] = resolvedProjectId;
                         }

                         if (resolvedProjectId == null)
                         {
                             // If referenced project is missing, set to NULL (optional behavior, preventing failure)
                             value = DBNull.Value;
                         }
                         else
                         {
                             value = resolvedProjectId;
                         }
                    }

                    // Special handling for ProjectTypeMasterID column (FK to master.ProjectTypeMaster)
                    // Resolve by ProjectTypeMasterID (numeric) or by ProjectTypeName (string)
                    if (string.Equals(mapping.SqlColumnName, "ProjectTypeMasterID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        if (long.TryParse(value.ToString()?.Trim(), out var projectTypeNumeric) && projectTypeNumeric == 0)
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!projectTypeMasterIdCache.TryGetValue(valueKey, out var resolvedProjectTypeMasterId))
                            {
                                // Not in cache, resolve from database
                                resolvedProjectTypeMasterId = await ResolveProjectTypeMasterIdByNameAsync(
                                    connection,
                                    transaction,
                                    value,
                                    "master",
                                    "ProjectTypeMaster",
                                    "ProjectTypeName",
                                    cancellationToken);

                                // Cache the result (even if null)
                                projectTypeMasterIdCache[valueKey] = resolvedProjectTypeMasterId;
                            }

                            if (resolvedProjectTypeMasterId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: ProjectTypeMasterID '{value}' does not exist in table 'master.ProjectTypeMaster'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedProjectTypeMasterId;
                        }
                    }

                    var targetColumn = mappedTable.Columns[mapping.SqlColumnName];

                    // Special handling for status column in CommunicationProtocol
                    if (isCommunicationProtocol &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for status column in CustomerMaster
                    if (isCustomerMaster &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for status column in CustomerContacts
                    if (isCustomerContacts &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for status column in VendorMaster
                    if (isVendorMaster &&
                        string.Equals(mapping.SqlColumnName, "StatusID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for status column in Project
                    if (isProject &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformProjectStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for ProjectTemplateID column in Project
                    if (isProject &&
                        string.Equals(mapping.SqlColumnName, "ProjectTemplateID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformProjectTemplateIdValue(value, mapping.IsNullable);
                    }

                    // Special handling for status column in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for scope columns in MechanicalDBO (TTL→0, Customer→1, Existing→2, Not Applicable→3)
                    if (isMechanicalDBO &&
                        (string.Equals(mapping.SqlColumnName, "AdditionalBOPScope", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "CondenserScope", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "GlandVentCondenserScope", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "CondensateExtractionPumpScope", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "EjectionSystemScope", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "MSParameterGlandSealingEjectionSystemScope", StringComparison.OrdinalIgnoreCase)) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOScopeValue(value, mapping.IsNullable);
                    }

                    // Special handling for Type column in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Type", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOTypeValue(value, mapping.IsNullable);
                    }

                    // Special handling for TemperatureUnitID and AmbientTemperatureUnitID in MechanicalDBO (resolve by unit name)
                    if (isMechanicalDBO &&
                        (string.Equals(mapping.SqlColumnName, "TemperatureUnitID", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "AmbientTemperatureUnitID", StringComparison.OrdinalIgnoreCase)) &&
                        value != DBNull.Value && value != null)
                    {
                        if (long.TryParse(value.ToString()?.Trim(), out var unitNumeric) && unitNumeric == 0)
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!unitIdCache.TryGetValue(valueKey, out var resolvedUnitId))
                            {
                                // Not in cache, resolve from database
                                resolvedUnitId = await ResolveUnitIdByNameAsync(
                                    connection,
                                    transaction,
                                    value,
                                    "master",
                                    "UnitMaster",
                                    "UnitName",
                                    cancellationToken);

                                // Cache the result (even if null)
                                unitIdCache[valueKey] = resolvedUnitId;
                            }

                            if (resolvedUnitId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: Unit '{value}' does not exist in table 'master.UnitMaster'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedUnitId;
                        }
                    }

                    // Special handling for ExhaustPressureUnitID and PressureUnitID in MechanicalDBO (resolve by unit name)
                    if (isMechanicalDBO &&
                        (string.Equals(mapping.SqlColumnName, "ExhaustPressureUnitID", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.SqlColumnName, "PressureUnitID", StringComparison.OrdinalIgnoreCase)) &&
                        value != DBNull.Value && value != null)
                    {
                        if (long.TryParse(value.ToString()?.Trim(), out var unitNumeric) && unitNumeric == 0)
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!unitIdCache.TryGetValue(valueKey, out var resolvedUnitId))
                            {
                                // Not in cache, resolve from database
                                resolvedUnitId = await ResolveUnitIdByNameAsync(
                                    connection,
                                    transaction,
                                    value,
                                    "master",
                                    "UnitMaster",
                                    "UnitName",
                                    cancellationToken);

                                // Cache the result (even if null)
                                unitIdCache[valueKey] = resolvedUnitId;
                            }

                            if (resolvedUnitId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: Unit '{value}' does not exist in table 'master.UnitMaster'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedUnitId;
                        }
                    }

                    // Special handling for CleanlinessFactor in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CleanlinessFactor", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOCleanlinessFactorValue(value, mapping.IsNullable);
                    }

                    // Special handling for FoulingFactor in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "FoulingFactor", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOFoulingFactorValue(value, mapping.IsNullable);
                    }

                    // Special handling for PluggingMargin in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "PluggingMargin", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOPluggingMarginValue(value, mapping.IsNullable);
                    }

                    // Special handling for CWInletTemperature in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CWInletTemperature", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOCWInletTemperatureValue(value, mapping.IsNullable);
                    }

                    // Special handling for CWOutletTemperature in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CWOutletTemperature", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOCWOutletTemperatureValue(value, mapping.IsNullable);
                    }

                    // Special handling for CWSupplyPressure in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CWSupplyPressure", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOCWSupplyPressureValue(value, mapping.IsNullable);
                    }

                    // Special handling for CWDesignPressure in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CWDesignPressure", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOCWDesignPressureValue(value, mapping.IsNullable);
                    }

                    // Special handling for CWVelocity in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CWVelocity", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOCWVelocityValue(value, mapping.IsNullable);
                    }

                    // Special handling for VacuumBreakerValve in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "VacuumBreakerValve", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOVacuumBreakerValveValue(value, mapping.IsNullable);
                    }

                    // Special handling for Quantity in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Quantity", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOQuantityValue(value, mapping.IsNullable);
                    }

                    // Special handling for MaterialOfCasing in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "MaterialOfCasing", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOMaterialOfCasingValue(value, mapping.IsNullable);
                    }

                    // Special handling for AdditionalBOP in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "AdditionalBOP", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOAdditionalBOPValue(value, mapping.IsNullable);
                    }

                    // Special handling for RatedDifferentialHead in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "RatedDifferentialHead", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBORatedDifferentialHeadValue(value, mapping.IsNullable);
                    }

                    // Special handling for FlowRating in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "FlowRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOFlowRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for InterAfterCondenser in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "InterAfterCondenser", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOInterAfterCondenserValue(value, mapping.IsNullable);
                    }

                    // Special handling for StartupEjector in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "StartupEjector", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOStartupEjectorValue(value, mapping.IsNullable);
                    }

                    // Special handling for MainEjector in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "MainEjector", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOMainEjectorValue(value, mapping.IsNullable);
                    }

                    // Special handling for EjectorNozzle in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "EjectorNozzle", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOEjectorNozzleValue(value, mapping.IsNullable);
                    }

                    // Special handling for TubesOfInterAfterCondenser in MechanicalDBO (uses same conversion as EjectorNozzle)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "TubesOfInterAfterCondenser", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOEjectorNozzleValue(value, mapping.IsNullable);
                    }

                    // Special handling for TubesSheetOfInterAfterCondenser in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "TubesSheetOfInterAfterCondenser", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOTubesSheetOfInterAfterCondenserValue(value, mapping.IsNullable);
                    }

                    // Special handling for ShellOfInterAfterCondenser in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "ShellOfInterAfterCondenser", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOShellOfInterAfterCondenserValue(value, mapping.IsNullable);
                    }

                    // Special handling for GlandSealing in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "GlandSealing", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOGlandSealingValue(value, mapping.IsNullable);
                    }

                    // Special handling for EjectionSystemDuringStartup in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "EjectionSystemDuringStartup", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOEjectionSystemDuringStartupValue(value, mapping.IsNullable);
                    }

                    // Special handling for WaterBoxes in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "WaterBoxes", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOWaterBoxesValue(value, mapping.IsNullable);
                    }

                    // Special handling for Shell in MechanicalDBO (uses same conversion as WaterBoxes)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Shell", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOWaterBoxesValue(value, mapping.IsNullable);
                    }

                    // Special handling for HotelWellRetentionTime in MechanicalDBO (uses same conversion as WaterBoxes)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "HotelWellRetentionTime", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOWaterBoxesValue(value, mapping.IsNullable);
                    }

                    // Special handling for Tubes in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Tubes", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOTubesValue(value, mapping.IsNullable);
                    }

                    // Special handling for GlandVentShell in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "GlandVentShell", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOGlandVentShellValue(value, mapping.IsNullable);
                    }

                    // Special handling for GlandVentTubes in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "GlandVentTubes", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOGlandVentTubesValue(value, mapping.IsNullable);
                    }

                    // Special handling for TubeSheets in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "TubeSheets", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOTubeSheetsValue(value, mapping.IsNullable);
                    }

                    // Special handling for Baffles in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Baffles", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOBafflesValue(value, mapping.IsNullable);
                    }

                    // Special handling for SafetyDeviceForCondenser in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "SafetyDeviceForCondenser", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOSafetyDeviceForCondenserValue(value, mapping.IsNullable);
                    }

                    // Special handling for Blower in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Blower", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOBlowerValue(value, mapping.IsNullable);
                    }

                    // Special handling for EjectionSystemForContinuous in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "EjectionSystemForContinuous", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOEjectionSystemForContinuousValue(value, mapping.IsNullable);
                    }

                    // Special handling for AutoGlandSealingSystem in MechanicalDBO
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "AutoGlandSealingSystem", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOAutoGlandSealingSystemValue(value, mapping.IsNullable);
                    }

                    // Special handling for GlandVentTubesSheet in MechanicalDBO (uses same conversion as SafetyDeviceForCondenser)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "GlandVentTubesSheet", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBOSafetyDeviceForCondenserValue(value, mapping.IsNullable);
                    }

                    // Special handling for ReliefValve in MechanicalDBO (uses RequiredNotRequired conversion)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "ReliefValve", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBORequiredNotRequiredValue(value, mapping.IsNullable);
                    }

                    // Special handling for Rotometer in MechanicalDBO (uses RequiredNotRequired conversion)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "Rotometer", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBORequiredNotRequiredValue(value, mapping.IsNullable);
                    }

                    // Special handling for CrossOverduct in MechanicalDBO (uses RequiredNotRequired conversion)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "CrossOverduct", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBORequiredNotRequiredValue(value, mapping.IsNullable);
                    }

                    // Special handling for DumpProvision in MechanicalDBO (uses RequiredNotRequired conversion)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "DumpProvision", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBORequiredNotRequiredValue(value, mapping.IsNullable);
                    }

                    // Special handling for LPGlandSealingAndDesuperheater in MechanicalDBO (uses RequiredNotRequired conversion)
                    if (isMechanicalDBO &&
                        string.Equals(mapping.SqlColumnName, "LPGlandSealingAndDesuperheater", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformMechanicalDBORequiredNotRequiredValue(value, mapping.IsNullable);
                    }

                    // Special handling for OrderTransmittalID column in OrderTransmittal child tables and ElectricalInstrumentationDBO tables (FK to OrderTransmittal)
                    // Handle Excel column k__ot_sel_ot_rec_bpp, ot_sel_ot_rec_bpp, or SQL column OrderTransmittalID
                    // Resolve by OrderTransmittalID (numeric) or by RecordNo (string)
                    if (((isOrderTransmittal && !string.Equals(tableName, "OrderTransmittal", StringComparison.OrdinalIgnoreCase)) ||
                         isElectricalInstrumentationDBO) &&
                        (string.Equals(mapping.SqlColumnName, "OrderTransmittalID", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.ExcelColumnName, "k__ot_sel_ot_rec_bpp", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.ExcelColumnName, "ot_sel_ot_rec_bpp", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.ExcelColumnName, "id", StringComparison.OrdinalIgnoreCase)))
                    {
                        // If value is NULL or DBNull, keep it as NULL
                        if (value == DBNull.Value || value == null)
                        {
                            value = DBNull.Value;
                        }
                        // If value is numeric 0 or string "0", convert to NULL
                        else if ((value is int intVal && intVal == 0) ||
                                 (value is long longVal && longVal == 0) ||
                                 (value is short shortVal && shortVal == 0) ||
                                 (long.TryParse(value.ToString()?.Trim(), out var orderTransmittalNumeric) && orderTransmittalNumeric == 0))
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!orderTransmittalIdCache.TryGetValue(valueKey, out var resolvedOrderTransmittalId))
                            {
                                // Not in cache, resolve from database
                                resolvedOrderTransmittalId = await ResolveOrderTransmittalIdAsync(
                                    connection,
                                    transaction,
                                    value,
                                    cancellationToken);

                                // Cache the result (even if null)
                                orderTransmittalIdCache[valueKey] = resolvedOrderTransmittalId;
                            }

                            if (resolvedOrderTransmittalId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: OrderTransmittalID '{value}' does not exist in table 'bp.OrderTransmittal'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedOrderTransmittalId;
                        }
                    }

                    // Special handling for OrderTransmittalRecordID column in BPComments table (FK to OrderTransmittal)
                    // Resolve by OrderTransmittalID (numeric) or by RecordNo (string)
                    if (isBPComments &&
                        string.Equals(mapping.SqlColumnName, "OrderTransmittalRecordID", StringComparison.OrdinalIgnoreCase))
                    {
                        // If value is NULL or DBNull, keep it as NULL
                        if (value == DBNull.Value || value == null)
                        {
                            value = DBNull.Value;
                        }
                        // If value is numeric 0 or string "0", convert to NULL
                        else if ((value is int intVal && intVal == 0) ||
                                 (value is long longVal && longVal == 0) ||
                                 (value is short shortVal && shortVal == 0) ||
                                 (long.TryParse(value.ToString()?.Trim(), out var orderTransmittalNumeric) && orderTransmittalNumeric == 0))
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!orderTransmittalIdCache.TryGetValue(valueKey, out var resolvedOrderTransmittalId))
                            {
                                // Not in cache, resolve from database
                                resolvedOrderTransmittalId = await ResolveOrderTransmittalIdAsync(
                                    connection,
                                    transaction,
                                    value,
                                    cancellationToken);

                                // Cache the result (even if null)
                                orderTransmittalIdCache[valueKey] = resolvedOrderTransmittalId;
                            }

                            if (resolvedOrderTransmittalId == null)
                            {
                                // For BPComments, if OrderTransmittalRecordID doesn't exist, set to NULL instead of skipping row
                                value = DBNull.Value;
                            }
                            else
                            {
                                value = resolvedOrderTransmittalId;
                            }
                        }
                    }

                    // Special handling for FK columns in Turbine table
                    // Handle common FKs: ProjectID, OrderTransmittalID, etc.
                    if (isTurbine)
                    {
                        // Handle ProjectID FK
                        if (string.Equals(mapping.SqlColumnName, "ProjectID", StringComparison.OrdinalIgnoreCase))
                        {
                            // If value is NULL or DBNull, keep it as NULL
                            if (value == DBNull.Value || value == null)
                            {
                                value = DBNull.Value;
                            }
                            // If value is numeric 0 or string "0", convert to NULL
                            else if ((value is int intVal && intVal == 0) ||
                                     (value is long longVal && longVal == 0) ||
                                     (value is short shortVal && shortVal == 0) ||
                                     (long.TryParse(value.ToString()?.Trim(), out var projectNumeric) && projectNumeric == 0))
                            {
                                value = DBNull.Value;
                            }
                            else
                            {
                                var valueKey = value.ToString()?.Trim() ?? string.Empty;

                                // Check cache first
                                if (!projectIdCache.TryGetValue(valueKey, out var resolvedProjectId))
                                {
                                    // Not in cache, resolve from database
                                    resolvedProjectId = await ResolveProjectIdAsync(
                                        connection,
                                        transaction,
                                        value,
                                        cancellationToken);

                                    // Cache the result (even if null)
                                    projectIdCache[valueKey] = resolvedProjectId;
                                }

                                if (resolvedProjectId == null)
                                {
                                    // For Turbine, if ProjectID doesn't exist, set to NULL instead of skipping row
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    value = resolvedProjectId;
                                }
                            }
                        }
                        // Handle OrderTransmittalID FK
                        else if (string.Equals(mapping.SqlColumnName, "OrderTransmittalID", StringComparison.OrdinalIgnoreCase))
                        {
                            // If value is NULL or DBNull, keep it as NULL
                            if (value == DBNull.Value || value == null)
                            {
                                value = DBNull.Value;
                            }
                            // If value is numeric 0 or string "0", convert to NULL
                            else if ((value is int intVal && intVal == 0) ||
                                     (value is long longVal && longVal == 0) ||
                                     (value is short shortVal && shortVal == 0) ||
                                     (long.TryParse(value.ToString()?.Trim(), out var orderTransmittalNumeric) && orderTransmittalNumeric == 0))
                            {
                                value = DBNull.Value;
                            }
                            else
                            {
                                var valueKey = value.ToString()?.Trim() ?? string.Empty;

                                // Check cache first
                                if (!orderTransmittalIdCache.TryGetValue(valueKey, out var resolvedOrderTransmittalId))
                                {
                                    // Not in cache, resolve from database
                                    resolvedOrderTransmittalId = await ResolveOrderTransmittalIdAsync(
                                        connection,
                                        transaction,
                                        value,
                                        cancellationToken);

                                    // Cache the result (even if null)
                                    orderTransmittalIdCache[valueKey] = resolvedOrderTransmittalId;
                                }

                                if (resolvedOrderTransmittalId == null)
                                {
                                    // For Turbine, if OrderTransmittalID doesn't exist, set to NULL instead of skipping row
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    value = resolvedOrderTransmittalId;
                                }
                            }
                        }
                        // Handle other FK columns ending with "ID" - generic FK lookup
                        // This will handle columns like CustomerID, VendorID, etc. if they exist
                        else if (mapping.SqlColumnName.EndsWith("ID", StringComparison.OrdinalIgnoreCase) &&
                                 !mapping.IsIdentity && // Exclude primary key columns
                                 !string.Equals(mapping.SqlColumnName, "ProjectID", StringComparison.OrdinalIgnoreCase) &&
                                 !string.Equals(mapping.SqlColumnName, "OrderTransmittalID", StringComparison.OrdinalIgnoreCase))
                        {
                            // If value is NULL or DBNull, keep it as NULL
                            if (value == DBNull.Value || value == null)
                            {
                                value = DBNull.Value;
                            }
                            // If value is numeric 0 or string "0", convert to NULL
                            else if ((value is int intVal && intVal == 0) ||
                                     (value is long longVal && longVal == 0) ||
                                     (value is short shortVal && shortVal == 0) ||
                                     (long.TryParse(value.ToString()?.Trim(), out var fkNumeric) && fkNumeric == 0))
                            {
                                value = DBNull.Value;
                            }
                            // For other FK columns, we'll let SQL Server validate them
                            // If they fail, the row will be skipped with an error
                        }
                    }

                    // Special handling for Status column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "StatusId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for MaterialOfConstruction column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "MaterialOfConstruction", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineMaterialOfConstructionValue(value, mapping.IsNullable);
                    }

                    // Special handling for FootPrintReplacementId column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "FootPrintReplacementId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineFootPrintReplacementValue(value, mapping.IsNullable);
                    }

                    // Special handling for ExhaustOrientationId column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "ExhaustOrientationId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineExhaustOrientationValue(value, mapping.IsNullable);
                    }

                    // Special handling for InletOrientationId column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "InletOrientationId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineInletOrientationValue(value, mapping.IsNullable);
                    }

                    // Special handling for DrivenEquipmentId column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "DrivenEquipmentId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineDrivenEquipmentValue(value, mapping.IsNullable);
                    }

                    // Special handling for NoiseLevelID column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "NoiseLevelID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineNoiseLevelValue(value, mapping.IsNullable);
                    }

                    // Special handling for GearBox_NoiseLevelID column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "GearBox_NoiseLevelID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineNoiseLevelValue(value, mapping.IsNullable);
                    }

                    // Special handling for RotationDirectionID column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "RotationDirectionID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineRotationDirectionValue(value, mapping.IsNullable);
                    }

                    // Special handling for HMBDSubmittedId column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "HMBDSubmittedId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineHMBDValue(value, mapping.IsNullable);
                    }

                    // Special handling for TypeOfTurbineId column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "TypeOfTurbineId", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineTypeValue(value, mapping.IsNullable);
                    }

                    // Special handling for ManufacturingStandardID column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "ManufacturingStandardID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineManufacturingStandardValue(value, mapping.IsNullable);
                    }

                    // Special handling for GovernorScope column in Turbine table
                    if (isTurbine &&
                        string.Equals(mapping.SqlColumnName, "GovernorScope", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformTurbineGovernorScopeValue(value, mapping.IsNullable);
                    }

                    // Generic handling for Turbine columns that might use YesNoRequiredMap, SingleDoubleMap, or StandardOthersMap
                    // These can be applied to specific columns as needed based on their Excel values
                    // For now, we'll apply them based on common column name patterns
                    if (isTurbine && value != DBNull.Value && value != null)
                    {
                        var valueStr = value.ToString()?.Trim() ?? string.Empty;

                        // Check if value matches YesNoRequired pattern and column name suggests it
                        if (!string.IsNullOrWhiteSpace(valueStr) &&
                            (string.Equals(valueStr, "Required", StringComparison.OrdinalIgnoreCase) ||
                             string.Equals(valueStr, "Not Required", StringComparison.OrdinalIgnoreCase) ||
                             string.Equals(valueStr, "Yes", StringComparison.OrdinalIgnoreCase) ||
                             string.Equals(valueStr, "No", StringComparison.OrdinalIgnoreCase)) &&
                            (mapping.SqlColumnName.Contains("Required", StringComparison.OrdinalIgnoreCase) ||
                             mapping.SqlColumnName.Contains("IfRequired", StringComparison.OrdinalIgnoreCase)))
                        {
                            value = TransformTurbineYesNoRequiredValue(value, mapping.IsNullable);
                        }
                        // Check if value matches SingleDouble pattern
                        else if (!string.IsNullOrWhiteSpace(valueStr) &&
                                 (string.Equals(valueStr, "Single", StringComparison.OrdinalIgnoreCase) ||
                                  string.Equals(valueStr, "Double", StringComparison.OrdinalIgnoreCase)) &&
                                 (mapping.SqlColumnName.Contains("Type", StringComparison.OrdinalIgnoreCase) ||
                                  mapping.SqlColumnName.Contains("Coupling", StringComparison.OrdinalIgnoreCase)))
                        {
                            value = TransformTurbineSingleDoubleValue(value, mapping.IsNullable);
                        }
                        // Check if value matches StandardOthers pattern
                        else if (!string.IsNullOrWhiteSpace(valueStr) &&
                                 (string.Equals(valueStr, "Standard", StringComparison.OrdinalIgnoreCase) ||
                                  string.Equals(valueStr, "Others", StringComparison.OrdinalIgnoreCase)) &&
                                 (mapping.SqlColumnName.Contains("Standard", StringComparison.OrdinalIgnoreCase) ||
                                  mapping.SqlColumnName.Contains("Type", StringComparison.OrdinalIgnoreCase) ||
                                  mapping.SqlColumnName.Contains("Orientation", StringComparison.OrdinalIgnoreCase)))
                        {
                            value = TransformTurbineStandardOthersValue(value, mapping.IsNullable);
                        }
                    }

                    // Special handling for OrderTransmittalID column in MechanicalDBO tables (FK to OrderTransmittal)
                    // Handle Excel column k__ot_sel_ot_rec_bpp or SQL column OrderTransmittalID
                    // Resolve by OrderTransmittalID (numeric) or by RecordNo (string)
                    if (isMechanicalDBO &&
                        (string.Equals(mapping.SqlColumnName, "OrderTransmittalID", StringComparison.OrdinalIgnoreCase) ||
                         string.Equals(mapping.ExcelColumnName, "k__ot_sel_ot_rec_bpp", StringComparison.OrdinalIgnoreCase)))
                    {
                        // If value is NULL or DBNull, keep it as NULL
                        if (value == DBNull.Value || value == null)
                        {
                            value = DBNull.Value;
                        }
                        // If value is numeric 0 or string "0", convert to NULL
                        else if ((value is int intVal && intVal == 0) ||
                                 (value is long longVal && longVal == 0) ||
                                 (value is short shortVal && shortVal == 0) ||
                                 (long.TryParse(value.ToString()?.Trim(), out var orderTransmittalNumeric) && orderTransmittalNumeric == 0))
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!orderTransmittalIdCache.TryGetValue(valueKey, out var resolvedOrderTransmittalId))
                            {
                                // Not in cache, resolve from database
                                resolvedOrderTransmittalId = await ResolveOrderTransmittalIdAsync(
                                    connection,
                                    transaction,
                                    value,
                                    cancellationToken);

                                // Cache the result (even if null)
                                orderTransmittalIdCache[valueKey] = resolvedOrderTransmittalId;
                            }

                            if (resolvedOrderTransmittalId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: OrderTransmittalID '{value}' does not exist in table 'bp.OrderTransmittal'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedOrderTransmittalId;
                        }
                    }

                    // Special handling for status column in BankGuarantee
                    if (isBankGuarantee &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformBankGuaranteeStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for status column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "Status", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalStatusValue(value, mapping.IsNullable);
                    }

                    // Special handling for OrderType column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "OrderType", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalOrderTypeValue(value, mapping.IsNullable);
                    }

                    // Special handling for Frequency column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "Frequency", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalFrequencyValue(value, mapping.IsNullable);
                    }

                    // Special handling for ServiceType column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ServiceType", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalServiceTypeValue(value, mapping.IsNullable);
                    }

                    // Special handling for INCOTerms column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "INCOTerms", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalINCOTermsValue(value, mapping.IsNullable);
                    }

                    // Special handling for ScopeOfSpares column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ScopeOfSpares", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalScopeOfSparesValue(value, mapping.IsNullable);
                    }

                    // Special handling for ScopeOfSeaworthyPacking column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ScopeOfSeaworthyPacking", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalScopeOfSeaworthyPackingValue(value, mapping.IsNullable);
                    }

                    // Special handling for SiteInsurance column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "SiteInsurance", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalSiteInsuranceValue(value, mapping.IsNullable);
                    }

                    // Special handling for MarineInsurance column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "MarineInsurance", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalMarineInsuranceValue(value, mapping.IsNullable);
                    }

                    // Special handling for TransitInsurance column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TransitInsurance", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTransitInsuranceValue(value, mapping.IsNullable);
                    }

                    // Special handling for ComprehensiveInsurance column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ComprehensiveInsurance", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalComprehensiveInsuranceValue(value, mapping.IsNullable);
                    }

                    // Special handling for StatutoryApproval column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "StatutoryApproval", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalStatutoryApprovalValue(value, mapping.IsNullable);
                    }

                    // Special handling for TransmittalTypeID column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TransmittalTypeID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTransmittalTypeIDValue(value, mapping.IsNullable);
                    }

                    // Special handling for TypesOfServicesEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TypesOfServicesEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTypesOfServicesEandCValue(value, mapping.IsNullable);
                    }

                    // Special handling for EotCraneFacilityEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "EotCraneFacilityEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalEotCraneFacilityEandCValue(value, mapping.IsNullable);
                    }

                    // Special handling for ErectionCraneEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ErectionCraneEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalErectionCraneEandCValue(value, mapping.IsNullable);
                    }

                    // Special handling for MobileCraneFacilityEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "MobileCraneFacilityEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for ConveyanceForEngineerEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ConveyanceForEngineerEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for UnloadingAtSiteEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "UnloadingAtSiteEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for GroutingEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "GroutingEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for GroutingMaterialSupplyEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "GroutingMaterialSupplyEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for StorageAtSiteEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "StorageAtSiteEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for ConstructionPowerWaterEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ConstructionPowerWaterEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for ErectionCableAndBaseEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ErectionCableAndBaseEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformByTTLOrCustomerValue(value, mapping.IsNullable);
                    }

                    // Special handling for TypeOfSparesEandC column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TypeOfSparesEandC", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTypeOfSparesEandCValue(value, mapping.IsNullable);
                    }

                    // Special handling for TypeOfWarranty column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TypeOfWarranty", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTypeOfWarrantyValue(value, mapping.IsNullable);
                    }

                    // Special handling for ReplacedPartsWarranty column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ReplacedPartsWarranty", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalReplacedPartsWarrantyValue(value, mapping.IsNullable);
                    }

                    // Special handling for EarthquakeZone column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "EarthquakeZone", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalEarthquakeZoneValue(value, mapping.IsNullable);
                    }

                    // Special handling for CoolingWater column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CoolingWater", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCoolingWaterValue(value, mapping.IsNullable);
                    }

                    // Special handling for MotorEfficiency column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "MotorEfficiency", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalMotorEfficiencyValue(value, mapping.IsNullable);
                    }

                    // Special handling for GeneratedVoltageRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "GeneratedVoltageRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalGeneratedVoltageRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for AuxiliaryVoltageRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "AuxiliaryVoltageRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalAuxiliaryVoltageRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for Environment column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "Environment", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalEnvironmentValue(value, mapping.IsNullable);
                    }

                    // Special handling for ScopeForCivil column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ScopeForCivil", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalScopeForCivilValue(value, mapping.IsNullable);
                    }

                    // Special handling for EPCorDirect column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "EPCorDirect", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalEPCorDirectValue(value, mapping.IsNullable);
                    }

                    // Special handling for TypeOfOrder column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TypeOfOrder", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTypeOfOrderValue(value, mapping.IsNullable);
                    }

                    // Special handling for CostOverrunRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CostOverrunRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCostOverrunRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for ContractualDeliveryRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ContractualDeliveryRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalContractualDeliveryRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for CommercialTermsRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CommercialTermsRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCommercialTermsRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for CustomerRelationshipRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CustomerRelationshipRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCustomerRelationshipRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for FinancialHealthRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "FinancialHealthRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalFinancialHealthRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for AgreedPerformanceRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "AgreedPerformanceRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalAgreedPerformanceRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for WarrantyTermsRiskRating column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "WarrantyTermsRiskRating", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalWarrantyTermsRiskRatingValue(value, mapping.IsNullable);
                    }

                    // Special handling for CostOverrunImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CostOverrunImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCostOverrunImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for ContractualDeliveryImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ContractualDeliveryImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalContractualDeliveryImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for CommercialTermsImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CommercialTermsImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCommercialTermsImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for BusinessSector column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "BusinessSector", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalOthersBusinessSectorValue(value, mapping.IsNullable);
                    }

                    // Special handling for OthersBusinessSector column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "OthersBusinessSector", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalOthersBusinessSectorValue(value, mapping.IsNullable);
                    }

                    // Special handling for CustomerRelationshipImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "CustomerRelationshipImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCustomerRelationshipImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for FinancialHealthImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "FinancialHealthImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalFinancialHealthImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for AgreedPerformanceImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "AgreedPerformanceImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalAgreedPerformanceImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for WarrantyTermsImpact column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "WarrantyTermsImpact", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalWarrantyTermsImpactValue(value, mapping.IsNullable);
                    }

                    // Special handling for Currency column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "Currency", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalCurrencyValue(value, mapping.IsNullable);
                    }

                    // Special handling for Unit ID columns (resolve by unit name from UnitMaster)
                    if (isOrderTransmittal &&
                        UnitIdColumns.Contains(mapping.SqlColumnName) &&
                        value != DBNull.Value && value != null)
                    {
                        // If value is numeric 0, treat as NULL
                        if (long.TryParse(value.ToString()?.Trim(), out var unitNumeric) && unitNumeric == 0)
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!unitIdCache.TryGetValue(valueKey, out var resolvedUnitId))
                            {
                                // Not in cache, resolve from database
                                resolvedUnitId = await ResolveUnitIdByNameAsync(
                                    connection,
                                    transaction,
                                    value,
                                    "master",
                                    "UnitMaster",
                                    "UnitName",
                                    cancellationToken);

                                // Cache the result (even if null)
                                unitIdCache[valueKey] = resolvedUnitId;
                            }

                            if (resolvedUnitId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: Unit '{value}' does not exist in table 'master.UnitMaster'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedUnitId;
                        }
                    }

                    // Special handling for TaxesDutiesSpecify column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "TaxesDutiesSpecify", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalTaxesDutiesSpecifyValue(value, mapping.IsNullable);
                    }

                    // Special handling for ScopeOfFrieght column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ScopeOfFrieght", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalScopeOfFrieghtValue(value, mapping.IsNullable);
                    }

                    // Special handling for ScopeOfOptions column in OrderTransmittal
                    if (isOrderTransmittal &&
                        string.Equals(mapping.SqlColumnName, "ScopeOfOptions", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformOrderTransmittalScopeOfOptionsValue(value, mapping.IsNullable);
                    }

                    // Special handling for TypeOfGuarantee column in BankGuarantee
                    if (isBankGuarantee &&
                        string.Equals(mapping.SqlColumnName, "TypeOfGuarantee", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformBankGuaranteeTypeOfGuaranteeValue(value, mapping.IsNullable);
                    }

                    // Special handling for WarrantyClause column in BankGuarantee
                    if (isBankGuarantee &&
                        string.Equals(mapping.SqlColumnName, "WarrantyClause", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformBankGuaranteeWarrantyClauseValue(value, mapping.IsNullable);
                    }

                    // Special handling for GuaranteeAgainst column in BankGuarantee
                    if (isBankGuarantee &&
                        string.Equals(mapping.SqlColumnName, "GuaranteeAgainst", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformBankGuaranteeGuaranteeAgainstValue(value, mapping.IsNullable);
                    }

                    // Special handling for DraftFormat column in BankGuarantee
                    if (isBankGuarantee &&
                        string.Equals(mapping.SqlColumnName, "DraftFormat", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformBankGuaranteeDraftFormatValue(value, mapping.IsNullable);
                    }

                    // Special handling for BankGuaranteeType column in BankGuarantee
                    if (isBankGuarantee &&
                        string.Equals(mapping.SqlColumnName, "BankGuaranteeType", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        value = TransformBankGuaranteeTypeValue(value, mapping.IsNullable);
                    }

                    // Special handling for foreign key columns with int data type - directly convert Excel value to int
                    // Assume Excel already contains the ID value (not the name/description)
                    if (!string.IsNullOrWhiteSpace(mapping.ForeignKeyTableSchema) &&
                        !string.IsNullOrWhiteSpace(mapping.ForeignKeyTableName) &&
                        !string.IsNullOrWhiteSpace(mapping.ForeignKeyColumnName) &&
                        value != DBNull.Value && value != null)
                    {
                        // Check if the target column is int or bigint type
                        var isIntType = string.Equals(mapping.SqlDataType, "int", StringComparison.OrdinalIgnoreCase) ||
                                       string.Equals(mapping.SqlDataType, "bigint", StringComparison.OrdinalIgnoreCase) ||
                                       string.Equals(mapping.SqlDataType, "smallint", StringComparison.OrdinalIgnoreCase);

                        if (isIntType)
                        {
                            // Try to convert Excel value directly to int/bigint
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr))
                            {
                                if (long.TryParse(valueStr, out var longId))
                                {
                                    object? convertedValue = null;
                                    if (string.Equals(mapping.SqlDataType, "bigint", StringComparison.OrdinalIgnoreCase))
                                    {
                                        convertedValue = longId;
                                    }
                                    else if (string.Equals(mapping.SqlDataType, "int", StringComparison.OrdinalIgnoreCase))
                                    {
                                        convertedValue = (int)longId;
                                    }
                                    else if (string.Equals(mapping.SqlDataType, "smallint", StringComparison.OrdinalIgnoreCase))
                                    {
                                        convertedValue = (short)longId;
                                    }

                                    if (convertedValue != null)
                                    {
                                        // If value is 0, convert to NULL (0 often means "no value" for FK columns)
                                        if (IsZeroValue(convertedValue))
                                        {
                                            value = DBNull.Value;
                                        }
                                        else
                                        {
                                            // Validate that the FK value exists in the referenced table
                                            var fkExists = await ValidateForeignKeyValueAsync(
                                                connection,
                                                transaction,
                                                mapping.ForeignKeyTableSchema!,
                                                mapping.ForeignKeyTableName!,
                                                mapping.ForeignKeyColumnName!,
                                                convertedValue,
                                                cancellationToken);

                                            if (!fkExists)
                                            {
                                                errorColumn = mapping.ExcelColumnName;
                                                errorValue = value;
                                                errorMessage = $"Foreign key constraint violation: Value '{convertedValue}' does not exist in table '{mapping.ForeignKeyTableSchema}.{mapping.ForeignKeyTableName}.{mapping.ForeignKeyColumnName}'";
                                                skipRow = true;
                                                break;
                                            }

                                            value = convertedValue;
                                        }
                                    }
                                }
                                // If parsing fails, let the normal conversion handle it (might throw error)
                            }
                        }
                    }
                    // Also validate known FK columns for OrderTransmittal table (even if FK metadata not populated)
                    else if (isOrderTransmittal && value != DBNull.Value && value != null)
                    {
                        // Check for CustomerContactID
                        if (string.Equals(mapping.SqlColumnName, "CustomerContactID", StringComparison.OrdinalIgnoreCase))
                        {
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr) && long.TryParse(valueStr, out var contactId))
                            {
                                // If value is 0, convert to NULL
                                if (contactId == 0)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    var fkExists = await ValidateForeignKeyValueAsync(
                                        connection,
                                        transaction,
                                        "master",
                                        "CustomerContacts",
                                        "CustomerContactID",
                                        contactId,
                                        cancellationToken);

                                    if (!fkExists)
                                    {
                                        errorColumn = mapping.ExcelColumnName;
                                        errorValue = value;
                                        errorMessage = $"Foreign key constraint violation: CustomerContactID '{contactId}' does not exist in table 'master.CustomerContacts'";
                                        skipRow = true;
                                        break;
                                    }
                                }
                            }
                        }
                        // Check for CustomerContactID2
                        else if (string.Equals(mapping.SqlColumnName, "CustomerContactID2", StringComparison.OrdinalIgnoreCase))
                        {
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr) && long.TryParse(valueStr, out var contactId))
                            {
                                // If value is 0, convert to NULL
                                if (contactId == 0)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    var fkExists = await ValidateForeignKeyValueAsync(
                                        connection,
                                        transaction,
                                        "master",
                                        "CustomerContacts",
                                        "CustomerContactID",
                                        contactId,
                                        cancellationToken);

                                    if (!fkExists)
                                    {
                                        errorColumn = mapping.ExcelColumnName;
                                        errorValue = value;
                                        errorMessage = $"Foreign key constraint violation: CustomerContactID2 '{contactId}' does not exist in table 'master.CustomerContacts'";
                                        skipRow = true;
                                        break;
                                    }
                                }
                            }
                        }
                        // Check for EndUserContactID
                        else if (string.Equals(mapping.SqlColumnName, "EndUserContactID", StringComparison.OrdinalIgnoreCase))
                        {
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr) && long.TryParse(valueStr, out var contactId))
                            {
                                // If value is 0, convert to NULL
                                if (contactId == 0)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    var fkExists = await ValidateForeignKeyValueAsync(
                                        connection,
                                        transaction,
                                        "master",
                                        "CustomerContacts",
                                        "CustomerContactID",
                                        contactId,
                                        cancellationToken);

                                    if (!fkExists)
                                    {
                                        errorColumn = mapping.ExcelColumnName;
                                        errorValue = value;
                                        errorMessage = $"Foreign key constraint violation: EndUserContactID '{contactId}' does not exist in table 'master.CustomerContacts'";
                                        skipRow = true;
                                        break;
                                    }
                                }
                            }
                        }
                        // Check for EndUserContactID2
                        else if (string.Equals(mapping.SqlColumnName, "EndUserContactID2", StringComparison.OrdinalIgnoreCase))
                        {
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr) && long.TryParse(valueStr, out var contactId))
                            {
                                // If value is 0, convert to NULL
                                if (contactId == 0)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    var fkExists = await ValidateForeignKeyValueAsync(
                                        connection,
                                        transaction,
                                        "master",
                                        "CustomerContacts",
                                        "CustomerContactID",
                                        contactId,
                                        cancellationToken);

                                    if (!fkExists)
                                    {
                                        errorColumn = mapping.ExcelColumnName;
                                        errorValue = value;
                                        errorMessage = $"Foreign key constraint violation: EndUserContactID2 '{contactId}' does not exist in table 'master.CustomerContacts'";
                                        skipRow = true;
                                        break;
                                    }
                                }
                            }
                        }
                        // Check for CustomerMasterID
                        else if (string.Equals(mapping.SqlColumnName, "CustomerMasterID", StringComparison.OrdinalIgnoreCase))
                        {
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr) && long.TryParse(valueStr, out var customerId))
                            {
                                // If value is 0, convert to NULL
                                if (customerId == 0)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    var fkExists = await ValidateForeignKeyValueAsync(
                                        connection,
                                        transaction,
                                        "master",
                                        "CustomerMaster",
                                        "CustomerID",
                                        customerId,
                                        cancellationToken);

                                    if (!fkExists)
                                    {
                                        errorColumn = mapping.ExcelColumnName;
                                        errorValue = value;
                                        errorMessage = $"Foreign key constraint violation: CustomerMasterID '{customerId}' does not exist in table 'master.CustomerMaster'";
                                        skipRow = true;
                                        break;
                                    }
                                }
                            }
                        }
                        // Check for EndUserID
                        else if (string.Equals(mapping.SqlColumnName, "EndUserID", StringComparison.OrdinalIgnoreCase))
                        {
                            var valueStr = value.ToString()?.Trim();
                            if (!string.IsNullOrWhiteSpace(valueStr) && long.TryParse(valueStr, out var endUserId))
                            {
                                // If value is 0, convert to NULL
                                if (endUserId == 0)
                                {
                                    value = DBNull.Value;
                                }
                                else
                                {
                                    var fkExists = await ValidateForeignKeyValueAsync(
                                        connection,
                                        transaction,
                                        "master",
                                        "CustomerMaster",
                                        "CustomerID",
                                        endUserId,
                                        cancellationToken);

                                    if (!fkExists)
                                    {
                                        errorColumn = mapping.ExcelColumnName;
                                        errorValue = value;
                                        errorMessage = $"Foreign key constraint violation: EndUserID '{endUserId}' does not exist in table 'master.CustomerMaster'";
                                        skipRow = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    // Special handling for CustomerID column in CustomerContacts (FK to CustomerMaster)
                    // Resolve by CustomerID (numeric) or by CompanyCode/CompanyName (string)
                    if (isCustomerContacts &&
                        string.Equals(mapping.SqlColumnName, "CustomerID", StringComparison.OrdinalIgnoreCase) &&
                        value != DBNull.Value && value != null)
                    {
                        // If value is numeric 0, treat as NULL
                        if (long.TryParse(value.ToString()?.Trim(), out var customerNumeric) && customerNumeric == 0)
                        {
                            value = DBNull.Value;
                        }
                        else
                        {
                            var valueKey = value.ToString()?.Trim() ?? string.Empty;

                            // Check cache first
                            if (!customerIdCache.TryGetValue(valueKey, out var resolvedCustomerId))
                            {
                                // Not in cache, resolve from database
                                resolvedCustomerId = await ResolveCustomerIdByNameAsync(
                                    connection,
                                    transaction,
                                    value,
                                    cancellationToken);

                                // Cache the result (even if null)
                                customerIdCache[valueKey] = resolvedCustomerId;
                            }

                            if (resolvedCustomerId == null)
                            {
                                errorColumn = mapping.ExcelColumnName;
                                errorValue = value;
                                errorMessage = $"Foreign key constraint violation: Customer '{value}' does not exist in table 'master.CustomerMaster'";
                                skipRow = true;
                                break;
                            }

                            value = resolvedCustomerId;
                        }
                    }

                    if (value == DBNull.Value || value == null)
                    {
                        if (mapping.IsNullable)
                        {
                            newRow[mapping.SqlColumnName] = DBNull.Value;
                        }
                        else
                        {
                            // Skip rows with null values for non-nullable columns
                            errorColumn = mapping.ExcelColumnName;
                            errorValue = value;
                            errorMessage = $"Null value not allowed for non-nullable column '{mapping.SqlColumnName}'";
                            skipRow = true;
                            break;
                        }
                    }
                    else
                    {
                        // Convert value to match target column type
                        if (targetColumn != null)
                        {
                            newRow[mapping.SqlColumnName] = ConvertValue(value, targetColumn.DataType);
                        }
                    }
                }
                catch (Exception ex)
                {
                    // If conversion fails for a column, capture error details
                    errorColumn = mapping.ExcelColumnName;
                    try
                    {
                        errorValue = excelRow[mapping.ExcelColumnName];
                    }
                    catch
                    {
                        errorValue = "Unable to read value";
                    }
                    errorMessage = $"Type conversion error: {ex.Message}";
                    skipRow = true;
                    break;
                }
            }

            if (skipRow)
            {
                // Add row error detail
                rowErrors.Add(new Models.RowErrorDetail
                {
                    RowNumber = rowNumber,
                    ColumnName = errorColumn ?? "Unknown",
                    Value = errorValue,
                    ErrorMessage = errorMessage ?? "Unknown error",
                    RowData = rowData
                });
            }
            else
            {
                // Ensure IsDeleted is set to false (override if it was in Excel mappings)
                if (mappedTable.Columns.Contains("IsDeleted"))
                {
                    var isDeletedDataColumn = mappedTable.Columns["IsDeleted"];
                    if (isDeletedDataColumn != null && isDeletedDataColumn.DataType == typeof(bool))
                    {
                        newRow["IsDeleted"] = false;
                    }
                    else if (isDeletedDataColumn != null)
                    {
                        // For other types (int, bit as int, etc.), set to 0
                        newRow["IsDeleted"] = Convert.ChangeType(0, isDeletedDataColumn.DataType);
                    }
                }
                mappedTable.Rows.Add(newRow);
            }
        }

        return (mappedTable, rowErrors);
    }

    private Type GetNetTypeFromSqlType(string sqlDataType)
    {
        if (string.IsNullOrWhiteSpace(sqlDataType))
            return typeof(object);

        var type = sqlDataType.ToUpper().Trim();

        // Map SQL Server types to .NET types
        switch (type)
        {
            case "INT":
            case "INTEGER":
                return typeof(int);

            case "BIGINT":
                return typeof(long);

            case "SMALLINT":
                return typeof(short);

            case "TINYINT":
                return typeof(byte);

            case "BIT":
                return typeof(bool);

            case "DECIMAL":
            case "NUMERIC":
            case "MONEY":
            case "SMALLMONEY":
                return typeof(decimal);

            case "FLOAT":
            case "REAL":
                return typeof(double);

            case "DATE":
            case "DATETIME":
            case "DATETIME2":
            case "SMALLDATETIME":
                return typeof(DateTime);

            case "DATETIMEOFFSET":
                return typeof(DateTimeOffset);

            case "TIME":
                return typeof(TimeSpan);

            case "VARCHAR":
            case "NVARCHAR":
            case "CHAR":
            case "NCHAR":
            case "TEXT":
            case "NTEXT":
                return typeof(string);

            case "UNIQUEIDENTIFIER":
                return typeof(Guid);

            case "BINARY":
            case "VARBINARY":
            case "IMAGE":
                return typeof(byte[]);

            default:
                // For unknown types, return object to allow conversion
                return typeof(object);
        }
    }

    private async Task<string?> GetPrimaryKeyColumnNameAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string schemaName,
        string tableName,
        CancellationToken cancellationToken)
    {
        var query = @"
            SELECT TOP 1 ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                AND tc.TABLE_NAME = ku.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = @SchemaName
                AND tc.TABLE_NAME = @TableName";

        try
        {
            await using var command = new SqlCommand(query, connection, transaction);
            command.CommandTimeout = SqlCommandTimeout;
            command.Parameters.AddWithValue("@SchemaName", schemaName);
            command.Parameters.AddWithValue("@TableName", tableName);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result != null && result != DBNull.Value)
            {
                return result.ToString();
            }
        }
        catch
        {
            // If lookup fails, return null
        }

        return null;
    }

    private async Task<object?> ResolveUnitIdByNameAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        object excelValue,
        string unitTableSchema,
        string unitTableName,
        string unitNameColumnName,
        CancellationToken cancellationToken)
    {
        if (excelValue == null || excelValue == DBNull.Value)
            return null;

        var valueStr = excelValue.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(valueStr))
            return null;

        // Get the primary key column name dynamically
        var unitIdColumnName = await GetPrimaryKeyColumnNameAsync(
            connection,
            transaction,
            unitTableSchema,
            unitTableName,
            cancellationToken);

        if (string.IsNullOrWhiteSpace(unitIdColumnName))
        {
            // Fallback to common column names if primary key lookup fails
            unitIdColumnName = "UnitMasterID";
        }

        // If numeric, try to validate directly
        if (long.TryParse(valueStr, out var numericId))
        {
            var fkExists = await ValidateForeignKeyValueAsync(
                connection,
                transaction,
                unitTableSchema,
                unitTableName,
                unitIdColumnName,
                numericId,
                cancellationToken);

            return fkExists ? numericId : null;
        }

        // Otherwise, lookup by unit name (case-insensitive)
        var query = $@"
            SELECT TOP 1 [{unitIdColumnName}]
            FROM [{unitTableSchema}].[{unitTableName}]
            WHERE [{unitNameColumnName}] = @UnitName";

        await using var command = new SqlCommand(query, connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        command.Parameters.AddWithValue("@UnitName", valueStr);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        if (result != null && result != DBNull.Value)
        {
            return result;
        }

        return null;
    }

    private async Task<object?> ResolveProjectTypeMasterIdByNameAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        object excelValue,
        string projectTypeTableSchema,
        string projectTypeTableName,
        string projectTypeNameColumnName,
        CancellationToken cancellationToken)
    {
        if (excelValue == null || excelValue == DBNull.Value)
            return null;

        var valueStr = excelValue.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(valueStr))
            return null;

        // Get the primary key column name dynamically
        var projectTypeMasterIdColumnName = await GetPrimaryKeyColumnNameAsync(
            connection,
            transaction,
            projectTypeTableSchema,
            projectTypeTableName,
            cancellationToken);

        if (string.IsNullOrWhiteSpace(projectTypeMasterIdColumnName))
        {
            // Fallback to common column names if primary key lookup fails
            projectTypeMasterIdColumnName = "ProjectTypeMasterID";
        }

        // If numeric, try to validate directly
        if (long.TryParse(valueStr, out var numericId))
        {
            var fkExists = await ValidateForeignKeyValueAsync(
                connection,
                transaction,
                projectTypeTableSchema,
                projectTypeTableName,
                projectTypeMasterIdColumnName,
                numericId,
                cancellationToken);

            return fkExists ? numericId : null;
        }

        // Otherwise, lookup by project type name (case-insensitive)
        var query = $@"
            SELECT TOP 1 [{projectTypeMasterIdColumnName}]
            FROM [{projectTypeTableSchema}].[{projectTypeTableName}]
            WHERE [{projectTypeNameColumnName}] = @ProjectTypeName";

        await using var command = new SqlCommand(query, connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        command.Parameters.AddWithValue("@ProjectTypeName", valueStr);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        if (result != null && result != DBNull.Value)
        {
            return result;
        }

        return null;
    }

    private async Task<object?> ResolveCustomerIdByNameAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        object excelValue,
        CancellationToken cancellationToken)
    {
        if (excelValue == null || excelValue == DBNull.Value)
            return null;

        var valueStr = excelValue.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(valueStr))
            return null;

        // Get the primary key column name dynamically
        var customerIdColumnName = await GetPrimaryKeyColumnNameAsync(
            connection,
            transaction,
            "master",
            "CustomerMaster",
            cancellationToken);

        if (string.IsNullOrWhiteSpace(customerIdColumnName))
        {
            // Fallback to common column names if primary key lookup fails
            customerIdColumnName = "CustomerID";
        }

        // If numeric, try to validate directly
        if (long.TryParse(valueStr, out var numericId))
        {
            var fkExists = await ValidateForeignKeyValueAsync(
                connection,
                transaction,
                "master",
                "CustomerMaster",
                customerIdColumnName,
                numericId,
                cancellationToken);

            return fkExists ? numericId : null;
        }

        // Otherwise, lookup by CompanyCode first, then CompanyName (case-insensitive)
        // Try CompanyCode first
        var queryByCode = $@"
            SELECT TOP 1 [{customerIdColumnName}]
            FROM [master].[CustomerMaster]
            WHERE [CompanyCode] = @LookupValue";

        await using var commandByCode = new SqlCommand(queryByCode, connection, transaction);
        commandByCode.CommandTimeout = SqlCommandTimeout;
        commandByCode.Parameters.AddWithValue("@LookupValue", valueStr);

        var resultByCode = await commandByCode.ExecuteScalarAsync(cancellationToken);
        if (resultByCode != null && resultByCode != DBNull.Value)
        {
            return resultByCode;
        }

        // If not found by CompanyCode, try CompanyName
        var queryByName = $@"
            SELECT TOP 1 [{customerIdColumnName}]
            FROM [master].[CustomerMaster]
            WHERE [CompanyName] = @LookupValue";

        await using var commandByName = new SqlCommand(queryByName, connection, transaction);
        commandByName.CommandTimeout = SqlCommandTimeout;
        commandByName.Parameters.AddWithValue("@LookupValue", valueStr);

        var resultByName = await commandByName.ExecuteScalarAsync(cancellationToken);
        if (resultByName != null && resultByName != DBNull.Value)
        {
            return resultByName;
        }

        return null;
    }

    private async Task<object?> ResolveOrderTransmittalIdAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        object excelValue,
        CancellationToken cancellationToken)
    {
        if (excelValue == null || excelValue == DBNull.Value)
            return null;

        var valueStr = excelValue.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(valueStr))
            return null;

        // Get the primary key column name dynamically
        var orderTransmittalIdColumnName = await GetPrimaryKeyColumnNameAsync(
            connection,
            transaction,
            "bp",
            "OrderTransmittal",
            cancellationToken);

        if (string.IsNullOrWhiteSpace(orderTransmittalIdColumnName))
        {
            // Fallback to common column names if primary key lookup fails
            orderTransmittalIdColumnName = "OrderTransmittalID";
        }

        // If numeric, try to validate directly
        if (long.TryParse(valueStr, out var numericId))
        {
            var fkExists = await ValidateForeignKeyValueAsync(
                connection,
                transaction,
                "bp",
                "OrderTransmittal",
                orderTransmittalIdColumnName,
                numericId,
                cancellationToken);

            return fkExists ? numericId : null;
        }

        // Otherwise, lookup by RecordNo (case-insensitive)
        var query = $@"
            SELECT TOP 1 [{orderTransmittalIdColumnName}]
            FROM [bp].[OrderTransmittal]
            WHERE [RecordNo] = @RecordNo";

        await using var command = new SqlCommand(query, connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        command.Parameters.AddWithValue("@RecordNo", valueStr);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        if (result != null && result != DBNull.Value)
        {
            return result;
        }

        return null;
    }

    private async Task<object?> ResolveProjectIdAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        object excelValue,
        CancellationToken cancellationToken)
    {
        if (excelValue == null || excelValue == DBNull.Value)
            return null;

        var valueStr = excelValue.ToString()?.Trim();
        if (string.IsNullOrWhiteSpace(valueStr))
            return null;

        // Get the primary key column name dynamically
        var projectIdColumnName = await GetPrimaryKeyColumnNameAsync(
            connection,
            transaction,
            "master",
            "Project",
            cancellationToken);

        if (string.IsNullOrWhiteSpace(projectIdColumnName))
        {
            // Fallback to common column names if primary key lookup fails
            projectIdColumnName = "ProjectID";
        }

        // If numeric, try to validate directly
        if (long.TryParse(valueStr, out var numericId))
        {
            var fkExists = await ValidateForeignKeyValueAsync(
                connection,
                transaction,
                "master",
                "Project",
                projectIdColumnName,
                numericId,
                cancellationToken);

            return fkExists ? numericId : null;
        }

        // Otherwise, lookup by ProjectName (case-insensitive)
        // Assume 'ProjectName' is the column if looking up by name
        var query = $@"
            SELECT TOP 1 [{projectIdColumnName}]
            FROM [master].[Project]
            WHERE [ProjectName] = @ProjectName";

        try
        {
            await using var command = new SqlCommand(query, connection, transaction);
            command.CommandTimeout = SqlCommandTimeout;
            command.Parameters.AddWithValue("@ProjectName", valueStr);

            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result != null && result != DBNull.Value)
            {
                return result;
            }
        }
        catch
        {
            // If lookup fails (e.g. column doesn't exist or other error), return null implies project not found
        }

        return null;
    }

    private object TransformStatusValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var statusStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        if (string.Equals(statusStr, "Active", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(statusStr, "Terminated", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformProjectStatusValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var statusStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        if (string.Equals(statusStr, "Approved", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformProjectTemplateIdValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var templateStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        if (string.Equals(templateStr, "C Project Template -01", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformBankGuaranteeStatusValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var statusStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel status string → SQL integer value
        if (string.Equals(statusStr, "Amended", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(statusStr, "Approved", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(statusStr, "Expired", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(statusStr, "Invoked", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(statusStr, "Notify_Concern", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(statusStr, "Pending", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(statusStr, "Send_for_auth_aprvl", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(statusStr, "send_for_bg_approval", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(statusStr, "Send_for_fin_acknow", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else if (string.Equals(statusStr, "Send_for__fin_rev", StringComparison.OrdinalIgnoreCase))
        {
            return 10;
        }
        else if (string.Equals(statusStr, "Sent_init_clarifi", StringComparison.OrdinalIgnoreCase))
        {
            return 11;
        }
        else if (string.Equals(statusStr, "Sent_Clarification", StringComparison.OrdinalIgnoreCase))
        {
            return 12;
        }
        else if (string.Equals(statusStr, "Sent_for_Revision", StringComparison.OrdinalIgnoreCase))
        {
            return 13;
        }
        else if (string.Equals(statusStr, "Terminated", StringComparison.OrdinalIgnoreCase))
        {
            return 14;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalStatusValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var statusStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel status string → SQL integer value
        if (string.Equals(statusStr, "Approved", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(statusStr, "CC_Pending", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(statusStr, "Convert_PROT_OT", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(statusStr, "Pending", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(statusStr, "Pending_Add_Info", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(statusStr, "PH_Apprval_Pending", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(statusStr, "PPC_in_P6_Pending", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(statusStr, "Proj_Head_Rev_Pend", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(statusStr, "Prop_Head_Rev_Pend", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else if (string.Equals(statusStr, "Sent_Clarification", StringComparison.OrdinalIgnoreCase))
        {
            return 10;
        }
        else if (string.Equals(statusStr, "Terminated", StringComparison.OrdinalIgnoreCase))
        {
            return 11;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalOrderTypeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var orderTypeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel order type string → SQL integer value
        if (string.Equals(orderTypeStr, "Domestic", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(orderTypeStr, "Export", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(orderTypeStr, "Deemed Export", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(orderTypeStr, "Third Party Export", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalFrequencyValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var frequencyStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel frequency string → SQL integer value
        if (string.Equals(frequencyStr, "50 Hz", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(frequencyStr, "60 Hz", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalServiceTypeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var serviceTypeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel service type string → SQL integer value
        if (string.Equals(serviceTypeStr, "Turnkey", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(serviceTypeStr, "Supervision", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(serviceTypeStr, "Third party supervision", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalINCOTermsValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var incotermsStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel INCOTerms string → SQL integer value
        if (string.Equals(incotermsStr, "Ex-works", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(incotermsStr, "FCA", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(incotermsStr, "CPT", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(incotermsStr, "CIP", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(incotermsStr, "DAP", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(incotermsStr, "DPU", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(incotermsStr, "DDP", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(incotermsStr, "FAS", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(incotermsStr, "FOB", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(incotermsStr, "CFR", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else if (string.Equals(incotermsStr, "CIF", StringComparison.OrdinalIgnoreCase))
        {
            return 10;
        }
        else if (string.Equals(incotermsStr, "FOR", StringComparison.OrdinalIgnoreCase))
        {
            return 11;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalScopeOfSparesValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var scopeOfSparesStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel scope of spares string → SQL integer value
        if (string.Equals(scopeOfSparesStr, "Included in Order Value", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(scopeOfSparesStr, "Not in Scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(scopeOfSparesStr, "Separate Price", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalScopeOfSeaworthyPackingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var scopeOfSeaworthyPackingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel scope of seaworthy packing string → SQL integer value
        if (string.Equals(scopeOfSeaworthyPackingStr, "Included in the order value", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(scopeOfSeaworthyPackingStr, "Not in Scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(scopeOfSeaworthyPackingStr, "Separate Price", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalMarineInsuranceValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var marineInsuranceStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel marine insurance string → SQL integer value
        if (string.Equals(marineInsuranceStr, "TTL scope", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(marineInsuranceStr, "Purchaser scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(marineInsuranceStr, "Not applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalSiteInsuranceValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var siteInsuranceStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel site insurance string → SQL integer value
        if (string.Equals(siteInsuranceStr, "TTL scope", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(siteInsuranceStr, "Purchaser scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(siteInsuranceStr, "Not applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTransitInsuranceValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var transitInsuranceStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel transit insurance string → SQL integer value
        if (string.Equals(transitInsuranceStr, "TTL scope", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(transitInsuranceStr, "Purchaser scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(transitInsuranceStr, "Not applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalComprehensiveInsuranceValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var comprehensiveInsuranceStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel comprehensive insurance string → SQL integer value
        if (string.Equals(comprehensiveInsuranceStr, "TTL scope", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(comprehensiveInsuranceStr, "Purchaser scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(comprehensiveInsuranceStr, "Not applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalStatutoryApprovalValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var statutoryApprovalStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel statutory approval string → SQL integer value
        if (string.Equals(statutoryApprovalStr, "TTL scope", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(statutoryApprovalStr, "Purchaser scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(statutoryApprovalStr, "Not applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTransmittalTypeIDValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var transmittalTypeIDStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel transmittal type string → SQL integer value
        if (string.Equals(transmittalTypeIDStr, "Order Transmittal", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(transmittalTypeIDStr, "Provisional Order Transmittal", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTypesOfServicesEandCValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var typesOfServicesEandCStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel types of services E&C string → SQL integer value
        if (string.Equals(typesOfServicesEandCStr, "Only supervision of E & C", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(typesOfServicesEandCStr, "Erection & Commissioning", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(typesOfServicesEandCStr, "Third party supervision", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTypeOfSparesEandCValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var typeOfSparesEandCStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel type of spares E&C string → SQL integer value
        if (string.Equals(typeOfSparesEandCStr, "Commissioning (Standard)", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(typeOfSparesEandCStr, "2 Years Spares", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(typeOfSparesEandCStr, "Additional Spares", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(typeOfSparesEandCStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTypeOfWarrantyValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var typeOfWarrantyStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel type of warranty string → SQL integer value
        if (string.Equals(typeOfWarrantyStr, "2 Crushing seasons", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(typeOfWarrantyStr, "12 months from the date of commissioning / 18 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(typeOfWarrantyStr, "12 months from the date of commissioning / 24 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(typeOfWarrantyStr, "18 months from the date of commissioning / 24 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(typeOfWarrantyStr, "18 months from the date of commissioning / 36 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(typeOfWarrantyStr, "18 months from the date of commissioning / 42 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(typeOfWarrantyStr, "24 months from the date of commissioning / 30 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(typeOfWarrantyStr, "24 months from the date of commissioning / 36 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(typeOfWarrantyStr, "30 months from the date of commissioning / 36 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(typeOfWarrantyStr, "36 months from the date of commissioning / 42 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else if (string.Equals(typeOfWarrantyStr, "36 months from the date of commissioning / 60 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 10;
        }
        else if (string.Equals(typeOfWarrantyStr, "42 months from the date of commissioning / 48 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 11;
        }
        else if (string.Equals(typeOfWarrantyStr, "42 months from the date of commissioning / 60 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 12;
        }
        else if (string.Equals(typeOfWarrantyStr, "48 months from the date of commissioning / 54 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 13;
        }
        else if (string.Equals(typeOfWarrantyStr, "54 months from the date of commissioning / 60 months from the date of dispatch (whichever is earlier)", StringComparison.OrdinalIgnoreCase))
        {
            return 14;
        }
        else if (string.Equals(typeOfWarrantyStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 15;
        }
        else if (string.Equals(typeOfWarrantyStr, "Under Warranty", StringComparison.OrdinalIgnoreCase))
        {
            return 16;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalReplacedPartsWarrantyValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var replacedPartsWarrantyStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel replaced parts warranty string → SQL integer value
        if (string.Equals(replacedPartsWarrantyStr, "Original Warranty", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(replacedPartsWarrantyStr, "12 Months from Replacement", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalEarthquakeZoneValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var earthquakeZoneStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel earthquake zone string → SQL integer value
        if (string.Equals(earthquakeZoneStr, "Safe Zone", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(earthquakeZoneStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCoolingWaterValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var coolingWaterStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel cooling water string → SQL integer value
        if (string.Equals(coolingWaterStr, "Normal", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(coolingWaterStr, "Treated", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(coolingWaterStr, "Industrial", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(coolingWaterStr, "Sea Water", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalMotorEfficiencyValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var motorEfficiencyStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel motor efficiency string → SQL integer value
        if (string.Equals(motorEfficiencyStr, "IE2 (Std)", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(motorEfficiencyStr, "IE3", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(motorEfficiencyStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalGeneratedVoltageRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;

        if (string.Equals(str, "380 V", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "400 V", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "415 V", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "660 V", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "3.3 KV", StringComparison.OrdinalIgnoreCase))
            return 4;
        else if (string.Equals(str, "6.6 KV", StringComparison.OrdinalIgnoreCase))
            return 5;
        else if (string.Equals(str, "11 KV", StringComparison.OrdinalIgnoreCase))
            return 6;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 7;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformOrderTransmittalAuxiliaryVoltageRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;

        if (string.Equals(str, "380 V", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "400 V", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "415 V", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "660 V", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "3.3 KV", StringComparison.OrdinalIgnoreCase))
            return 4;
        else if (string.Equals(str, "6.6 KV", StringComparison.OrdinalIgnoreCase))
            return 5;
        else if (string.Equals(str, "11 KV", StringComparison.OrdinalIgnoreCase))
            return 6;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 7;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformOrderTransmittalEnvironmentValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;

        if (string.Equals(str, "Dusty", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Acidic", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Other", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformOrderTransmittalScopeForCivilValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;

        if (string.Equals(str, "Yes", StringComparison.OrdinalIgnoreCase) || str == "1")
            return 1;
        else if (string.Equals(str, "No", StringComparison.OrdinalIgnoreCase) || str == "0")
            return 0;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformOrderTransmittalEPCorDirectValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var epcOrDirectStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel EPC or Direct string → SQL integer value
        if (string.Equals(epcOrDirectStr, "EPC", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(epcOrDirectStr, "Direct", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTypeOfOrderValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var typeOfOrderStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel type of order string → SQL integer value
        if (string.Equals(typeOfOrderStr, "Contract", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(typeOfOrderStr, "Agreement", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(typeOfOrderStr, "LOI", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(typeOfOrderStr, "Purchase Order", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCostOverrunRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var costOverrunRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel cost overrun risk rating string → SQL integer value
        if (string.Equals(costOverrunRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(costOverrunRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(costOverrunRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCustomerRelationshipRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var customerRelationshipRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel customer relationship risk rating string → SQL integer value
        // Same mapping as CostOverrunRiskRating: R1→0, R2→1, R3→2
        if (string.Equals(customerRelationshipRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(customerRelationshipRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(customerRelationshipRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalFinancialHealthRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var financialHealthRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel financial health risk rating string → SQL integer value
        // Same mapping as CostOverrunRiskRating: R1→0, R2→1, R3→2
        if (string.Equals(financialHealthRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(financialHealthRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(financialHealthRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalAgreedPerformanceRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var agreedPerformanceRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel agreed performance risk rating string → SQL integer value
        // Same mapping as CostOverrunRiskRating: R1→0, R2→1, R3→2
        if (string.Equals(agreedPerformanceRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(agreedPerformanceRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(agreedPerformanceRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalWarrantyTermsRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var warrantyTermsRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel warranty terms risk rating string → SQL integer value
        // Same mapping as CostOverrunRiskRating: R1→0, R2→1, R3→2
        if (string.Equals(warrantyTermsRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(warrantyTermsRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(warrantyTermsRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCommercialTermsRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var commercialTermsRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel commercial terms risk rating string → SQL integer value
        // Same mapping as CostOverrunRiskRating: R1→0, R2→1, R3→2
        if (string.Equals(commercialTermsRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(commercialTermsRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(commercialTermsRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalContractualDeliveryRiskRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var contractualDeliveryRiskRatingStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel contractual delivery risk rating string → SQL integer value
        // Same mapping as CostOverrunRiskRating: R1→0, R2→1, R3→2
        if (string.Equals(contractualDeliveryRiskRatingStr, "R1", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(contractualDeliveryRiskRatingStr, "R2", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(contractualDeliveryRiskRatingStr, "R3", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCostOverrunImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var costOverrunImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel cost overrun impact string → SQL integer value
        if (string.Equals(costOverrunImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(costOverrunImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(costOverrunImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCommercialTermsImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var commercialTermsImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel commercial terms impact string → SQL integer value
        // Same mapping as CostOverrunImpact: Low→0, Medium→1, High→2
        if (string.Equals(commercialTermsImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(commercialTermsImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(commercialTermsImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalOthersBusinessSectorValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var othersBusinessSectorStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel others business sector string → SQL integer value
        if (string.Equals(othersBusinessSectorStr, "Sugar", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(othersBusinessSectorStr, "Palm Oil", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(othersBusinessSectorStr, "Biomass", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(othersBusinessSectorStr, "Distillery", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(othersBusinessSectorStr, "Oil & Gas", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(othersBusinessSectorStr, "Cement", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(othersBusinessSectorStr, "Pulp & Paper", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(othersBusinessSectorStr, "Textile", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(othersBusinessSectorStr, "Waste to Energy", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(othersBusinessSectorStr, "Food & Beverage", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else if (string.Equals(othersBusinessSectorStr, "Chemical & Fertilizers", StringComparison.OrdinalIgnoreCase))
        {
            return 10;
        }
        else if (string.Equals(othersBusinessSectorStr, "Steel", StringComparison.OrdinalIgnoreCase))
        {
            return 11;
        }
        else if (string.Equals(othersBusinessSectorStr, "IPP", StringComparison.OrdinalIgnoreCase))
        {
            return 12;
        }
        else if (string.Equals(othersBusinessSectorStr, "Carbon Black", StringComparison.OrdinalIgnoreCase))
        {
            return 13;
        }
        else if (string.Equals(othersBusinessSectorStr, "District Heating", StringComparison.OrdinalIgnoreCase))
        {
            return 14;
        }
        else if (string.Equals(othersBusinessSectorStr, "Pharmaceutical", StringComparison.OrdinalIgnoreCase))
        {
            return 15;
        }
        else if (string.Equals(othersBusinessSectorStr, "CHP", StringComparison.OrdinalIgnoreCase))
        {
            return 16;
        }
        else if (string.Equals(othersBusinessSectorStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 17;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCustomerRelationshipImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var customerRelationshipImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel customer relationship impact string → SQL integer value
        // Same mapping as CostOverrunImpact: Low→0, Medium→1, High→2
        if (string.Equals(customerRelationshipImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(customerRelationshipImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(customerRelationshipImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalFinancialHealthImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var financialHealthImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel financial health impact string → SQL integer value
        // Same mapping as CostOverrunImpact: Low→0, Medium→1, High→2
        if (string.Equals(financialHealthImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(financialHealthImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(financialHealthImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalAgreedPerformanceImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var agreedPerformanceImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel agreed performance impact string → SQL integer value
        // Same mapping as CostOverrunImpact: Low→0, Medium→1, High→2
        if (string.Equals(agreedPerformanceImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(agreedPerformanceImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(agreedPerformanceImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalWarrantyTermsImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var warrantyTermsImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel warranty terms impact string → SQL integer value
        // Same mapping as CostOverrunImpact: Low→0, Medium→1, High→2
        if (string.Equals(warrantyTermsImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(warrantyTermsImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(warrantyTermsImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalContractualDeliveryImpactValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var contractualDeliveryImpactStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel contractual delivery impact string → SQL integer value
        // Same mapping as CostOverrunImpact: Low→0, Medium→1, High→2
        if (string.Equals(contractualDeliveryImpactStr, "Low", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(contractualDeliveryImpactStr, "Medium", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(contractualDeliveryImpactStr, "High", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalCurrencyValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;

        if (string.Equals(str, "INR", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "USD", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "EUR", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "GBP", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "AUD", StringComparison.OrdinalIgnoreCase))
            return 4;
        else if (string.Equals(str, "NZD", StringComparison.OrdinalIgnoreCase))
            return 5;
        else if (string.Equals(str, "CAD", StringComparison.OrdinalIgnoreCase))
            return 6;
        else if (string.Equals(str, "CHF", StringComparison.OrdinalIgnoreCase))
            return 7;
        else if (string.Equals(str, "JPY", StringComparison.OrdinalIgnoreCase))
            return 8;
        else if (string.Equals(str, "ZAR", StringComparison.OrdinalIgnoreCase))
            return 9;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformOrderTransmittalEotCraneFacilityEandCValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var eotCraneFacilityEandCStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel EOT crane facility E&C string → SQL integer value
        if (string.Equals(eotCraneFacilityEandCStr, "Available", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(eotCraneFacilityEandCStr, "Not Available", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalErectionCraneEandCValue(object value, bool isNullable)
    {
        return TransformByTTLOrCustomerValue(value, isNullable);
    }

    // Common transformation method for columns with "By TTL"/"TTL" → 0 and "By Customer"/"Customer" → 1 mapping
    private object TransformByTTLOrCustomerValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var valueStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: "By TTL", "TTL", "By Triveni" → 0
        //          "By Customer", "Customer" → 1
        if (string.Equals(valueStr, "By TTL", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(valueStr, "TTL", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(valueStr, "By Triveni", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(valueStr, "By Customer", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(valueStr, "Customer", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalTaxesDutiesSpecifyValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var taxesDutiesSpecifyStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel taxes duties specify string → SQL integer value
        if (string.Equals(taxesDutiesSpecifyStr, "Included in the PO value", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(taxesDutiesSpecifyStr, "Extra as per Actual", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalScopeOfFrieghtValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var scopeOfFrieghtStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel scope of freight string → SQL integer value
        if (string.Equals(scopeOfFrieghtStr, "Included in the order value", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(scopeOfFrieghtStr, "In Purchaser scope", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(scopeOfFrieghtStr, "To be arranged by TTL on \"To Pay\" basis", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(scopeOfFrieghtStr, "To be arranged by TTL on 'To Pay' basis", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(scopeOfFrieghtStr, "Separate Price", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformOrderTransmittalScopeOfOptionsValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var scopeOfOptionsStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel scope of options string → SQL integer value
        if (string.Equals(scopeOfOptionsStr, "Included in the PO value", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(scopeOfOptionsStr, "Extra as per Actual", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformBankGuaranteeTypeOfGuaranteeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var typeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel TypeOfGuarantee string → SQL integer value
        if (string.Equals(typeStr, "Advance Bank Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(typeStr, "Perfomance Bank Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(typeStr, "Corporate Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(typeStr, "Corporate Performance Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(typeStr, "Counter Bank Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(typeStr, "Financial Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(typeStr, "Foreign Bank Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(typeStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(typeStr, "Corporate Bank Guarantee", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformBankGuaranteeWarrantyClauseValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var warrantyStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel WarrantyClause string → SQL integer value
        if (string.Equals(warrantyStr, "12/18 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(warrantyStr, "12/24 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(warrantyStr, "18/24 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(warrantyStr, "18/36 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(warrantyStr, "18/42 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(warrantyStr, "24/36 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else if (string.Equals(warrantyStr, "36/60 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 7;
        }
        else if (string.Equals(warrantyStr, "2 Crushing Season", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }
        else if (string.Equals(warrantyStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 9;
        }
        else if (string.Equals(warrantyStr, "12/36 months from the date of Commissioning or Dispatch whichever is earlier", StringComparison.OrdinalIgnoreCase))
        {
            return 10;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformBankGuaranteeGuaranteeAgainstValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var guaranteeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel GuaranteeAgainst string → SQL integer value
        if (string.Equals(guaranteeStr, "Contract", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(guaranteeStr, "E&C", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(guaranteeStr, "Performance", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(guaranteeStr, "Supply", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(guaranteeStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformBankGuaranteeDraftFormatValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var draftFormatStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel DraftFormat string → SQL integer value
        if (string.Equals(draftFormatStr, "Customer", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(draftFormatStr, "Not Applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(draftFormatStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(draftFormatStr, "TTL", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformBankGuaranteeTypeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var typeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel BankGuaranteeType string → SQL integer value
        if (string.Equals(typeStr, "New", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(typeStr, "Amendment", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    // MechanicalDBO Transformation Methods
    private object TransformMechanicalDBOScopeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "TTL", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Customer", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Existing", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOTypeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Water Cooled", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Air Cooled", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOPressureUnitValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "kg/cm²", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "kg/cm²g", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "barg", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "bara", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "ata", StringComparison.OrdinalIgnoreCase))
            return 4;
        else if (string.Equals(str, "kPa", StringComparison.OrdinalIgnoreCase))
            return 5;
        else if (string.Equals(str, "MPa", StringComparison.OrdinalIgnoreCase))
            return 6;
        else if (string.Equals(str, "PSI", StringComparison.OrdinalIgnoreCase))
            return 7;
        else if (string.Equals(str, "kg/cm²a", StringComparison.OrdinalIgnoreCase))
            return 8;
        else if (string.Equals(str, "kg/m²", StringComparison.OrdinalIgnoreCase))
            return 9;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOCleanlinessFactorValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "0.85(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOFoulingFactorValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std(0.00015)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOPluggingMarginValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "0% std", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOCWInletTemperatureValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "32", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "33", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "34", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "35", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 4;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 5;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOCWOutletTemperatureValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "40", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "41", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "42", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 4;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOCWSupplyPressureValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std(3 Ata)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOCWDesignPressureValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std(6 Ata)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOCWVelocityValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "SS34(2.13 m/s)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Adm Brass(1.8 m/s)", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOVacuumBreakerValveValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "TTL Scope", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Not Required", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOQuantityValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "2 (std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "3", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOMaterialOfCasingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std - Cast Iron", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "CS", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOAdditionalBOPValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Cooling Tower", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Air Compressor", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "EOT", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Fire fighting system", StringComparison.OrdinalIgnoreCase))
            return 3;
        else if (string.Equals(str, "Deaerator", StringComparison.OrdinalIgnoreCase))
            return 4;
        else if (string.Equals(str, "SWAS", StringComparison.OrdinalIgnoreCase))
            return 5;
        else if (string.Equals(str, "BFWP", StringComparison.OrdinalIgnoreCase))
            return 6;
        else if (string.Equals(str, "Pumps", StringComparison.OrdinalIgnoreCase))
            return 7;
        else if (string.Equals(str, "Grouting Cement", StringComparison.OrdinalIgnoreCase))
            return 8;
        else if (string.Equals(str, "HVAC", StringComparison.OrdinalIgnoreCase))
            return 9;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 10;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBORatedDifferentialHeadValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std(80 mtrs)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOFlowRatingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std - 1.1 times condensor flow", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOInterAfterCondenserValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "1 x 100% (Std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "2 x 100%", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOStartupEjectorValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std 1 x 100%", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOMainEjectorValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Std 1 x 100%", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOEjectorNozzleValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "SA 479 TP 304 (std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOTubesSheetOfInterAfterCondenserValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "IS 2002(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "SA 516 Gr.70", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOShellOfInterAfterCondenserValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "SA 106 Gr.B(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOGlandSealingValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Auxillary steam line (through PRDS)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 1;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOEjectionSystemDuringStartupValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Auxillary Steams", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOWaterBoxesValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "IS2062(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOTubesValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "SS 304 ERW (std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOGlandVentShellValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "SA106Gr.B(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOGlandVentTubesValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "SA106Gr.B(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOTubeSheetsValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Standard", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOBafflesValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "IS2062 (std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOSafetyDeviceForCondenserValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "IS 2002(std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "IS 2062(std)", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 2;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOBlowerValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "1x100% (std)", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "2x100%", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOEjectionSystemForContinuousValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Auxilary steam", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Others", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 3;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBOAutoGlandSealingSystemValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 0;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object TransformMechanicalDBORequiredNotRequiredValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var str = value.ToString()?.Trim() ?? string.Empty;
        if (string.Equals(str, "Required", StringComparison.OrdinalIgnoreCase))
            return 0;
        else if (string.Equals(str, "Not Required (std)", StringComparison.OrdinalIgnoreCase))
            return 1;
        else if (string.Equals(str, "Not Applicable", StringComparison.OrdinalIgnoreCase))
            return 2;
        else
            return isNullable ? DBNull.Value : 0;
    }

    private object ConvertValue(object value, Type targetType)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // If types match, return as-is
        if (value.GetType() == targetType || targetType.IsAssignableFrom(value.GetType()))
            return value;

        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // Check if value is a string that might contain scientific notation
        var valueString = value as string ?? value.ToString() ?? string.Empty;
        var isScientificNotation = !string.IsNullOrWhiteSpace(valueString) &&
            (valueString.Contains('E', StringComparison.OrdinalIgnoreCase) ||
             valueString.Contains('e', StringComparison.OrdinalIgnoreCase));

        // Handle numeric conversions with scientific notation support
        if (underlyingType == typeof(int))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                return (int)double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            }
            return Convert.ToInt32(value);
        }
        if (underlyingType == typeof(long))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                return (long)double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            }
            return Convert.ToInt64(value);
        }
        if (underlyingType == typeof(short))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                return (short)double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            }
            return Convert.ToInt16(value);
        }
        if (underlyingType == typeof(byte))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                return (byte)double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            }
            return Convert.ToByte(value);
        }
        if (underlyingType == typeof(decimal))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                // Parse as double first, then convert to decimal to handle scientific notation
                var doubleValue = double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
                return (decimal)doubleValue;
            }
            // Try parsing as decimal, but if it fails and contains 'E', try scientific notation
            try
            {
                return Convert.ToDecimal(value);
            }
            catch
            {
                // If standard conversion fails, try parsing with scientific notation support
                if (!string.IsNullOrWhiteSpace(valueString))
                {
                    var doubleValue = double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
                    return (decimal)doubleValue;
                }
                return Convert.ToDecimal(value);
            }
        }
        if (underlyingType == typeof(double))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                return double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            }
            return Convert.ToDouble(value);
        }
        if (underlyingType == typeof(float))
        {
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString))
            {
                return (float)double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
            }
            return Convert.ToSingle(value);
        }
        if (underlyingType == typeof(bool))
        {
            // Handle text-based boolean values (Yes/No, Required/Not Required, etc.)
            if (value is string stringValue && !string.IsNullOrWhiteSpace(stringValue))
            {
                var trimmedValue = stringValue.Trim();

                // Check for "true" values (should convert to 1/true)
                if (string.Equals(trimmedValue, "Yes", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "Y", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "True", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "1", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "Required", StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }

                // Check for "false" values (should convert to 0/false)
                if (string.Equals(trimmedValue, "No", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "N", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "False", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "0", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "Not Required", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(trimmedValue, "NotRequired", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            // Try standard conversion for other cases
            return Convert.ToBoolean(value);
        }
        if (underlyingType == typeof(DateTime))
        {
            return Convert.ToDateTime(value);
        }
        if (underlyingType == typeof(string))
        {
            return value.ToString() ?? string.Empty;
        }

        // Try direct conversion
        try
        {
            return Convert.ChangeType(value, underlyingType);
        }
        catch
        {
            // If direct conversion fails and value is a string with scientific notation, try parsing as double first
            if (isScientificNotation && !string.IsNullOrWhiteSpace(valueString) && (underlyingType == typeof(decimal) || underlyingType == typeof(double) || underlyingType == typeof(float)))
            {
                var doubleValue = double.Parse(valueString, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture);
                return Convert.ChangeType(doubleValue, underlyingType);
            }
            throw;
        }
    }

    private async Task<int> BulkCopyToTempTableAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string tempTableName,
        DataTable dataTable,
        List<ColumnMapping> mappings,
        bool hasIdentityInExcel,
        CancellationToken cancellationToken)
    {
        if (dataTable.Rows.Count == 0)
            return 0;

        var options = SqlBulkCopyOptions.Default;
        if (hasIdentityInExcel)
        {
            options |= SqlBulkCopyOptions.KeepIdentity;
        }

        using var bulkCopy = new SqlBulkCopy(connection, options, transaction);
        bulkCopy.DestinationTableName = tempTableName;
        bulkCopy.BulkCopyTimeout = SqlCommandTimeout; // 10 minutes for large datasets

        // Map columns
        foreach (var mapping in mappings)
        {
            bulkCopy.ColumnMappings.Add(mapping.SqlColumnName, mapping.SqlColumnName);
        }

        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);

        return dataTable.Rows.Count;
    }

    private async Task<(int rowsInserted, int rowsUpdated)> MergeFromTempToTargetAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string schemaName,
        string tableName,
        string tempTableName,
        List<ColumnMapping> mappings,
        List<string> primaryKeyColumns,
        ColumnMetadata? identityColumn,
        bool hasIdentityInExcel,
        CancellationToken cancellationToken)
    {
        // Check if IsDeleted column exists in target table
        var hasIsDeletedColumn = await CheckColumnExistsAsync(connection, transaction, schemaName, tableName, "IsDeleted", cancellationToken);
        return await MergeFromTempToTargetAsyncInternal(connection, transaction, schemaName, tableName, tempTableName, mappings, primaryKeyColumns, identityColumn, hasIdentityInExcel, hasIsDeletedColumn, cancellationToken);
    }

    private async Task<bool> CheckColumnExistsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string schemaName,
        string tableName,
        string columnName,
        CancellationToken cancellationToken)
    {
        var query = @"
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @SchemaName
                AND TABLE_NAME = @TableName
                AND COLUMN_NAME = @ColumnName";

        await using var command = new SqlCommand(query, connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        command.Parameters.AddWithValue("@SchemaName", schemaName);
        command.Parameters.AddWithValue("@TableName", tableName);
        command.Parameters.AddWithValue("@ColumnName", columnName);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result != null && Convert.ToInt32(result) > 0;
    }

    private async Task<(int rowsInserted, int rowsUpdated)> MergeFromTempToTargetAsyncInternal(
        SqlConnection connection,
        SqlTransaction transaction,
        string schemaName,
        string tableName,
        string tempTableName,
        List<ColumnMapping> mappings,
        List<string> primaryKeyColumns,
        ColumnMetadata? identityColumn,
        bool hasIdentityInExcel,
        bool hasIsDeletedColumn,
        CancellationToken cancellationToken)
    {
        var identityColumnName = identityColumn?.ColumnName;
        var enableIdentityInsert = hasIdentityInExcel && identityColumn != null;

        // If no primary key exists, fall back to INSERT (with potential duplicates)
        if (primaryKeyColumns.Count == 0)
        {
            // Fallback to simple INSERT if no primary key
            // Exclude IsDeleted from regular mappings and handle it separately
            var insertMappingsNoPk = mappings
                .Where(m => !string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var columnList = string.Join(", ", insertMappingsNoPk.Select(m => $"[{m.SqlColumnName}]"));
            var selectList = string.Join(", ", insertMappingsNoPk.Select(m => $"source.[{m.SqlColumnName}]"));

            // Always set IsDeleted to 0/false if the column exists
            if (hasIsDeletedColumn)
            {
                columnList = columnList + ", [IsDeleted]";
                selectList = selectList + ", 0";
            }

            var insertQuery = $"INSERT INTO [{schemaName}].[{tableName}] ({columnList}) SELECT {selectList} FROM {tempTableName} AS source";

            try
            {
                if (enableIdentityInsert)
                {
                    var enableIdentityCmd = $"SET IDENTITY_INSERT [{schemaName}].[{tableName}] ON";
                    await using var cmd1 = new SqlCommand(enableIdentityCmd, connection, transaction);
                    cmd1.CommandTimeout = SqlCommandTimeout;
                    await cmd1.ExecuteNonQueryAsync(cancellationToken);
                }

                await using var command = new SqlCommand(insertQuery, connection, transaction);
                command.CommandTimeout = SqlCommandTimeout;
                var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);
                return (rowsAffected, 0);
            }
            finally
            {
                if (enableIdentityInsert)
                {
                    var disableIdentityCmd = $"SET IDENTITY_INSERT [{schemaName}].[{tableName}] OFF";
                    await using var cmd2 = new SqlCommand(disableIdentityCmd, connection, transaction);
                    cmd2.CommandTimeout = SqlCommandTimeout;
                    await cmd2.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        // Build MERGE statement for UPSERT
        var sb = new StringBuilder();

        // Create table variable to capture merge results
        sb.AppendLine("DECLARE @MergeResults TABLE (Action NVARCHAR(10));");
        sb.AppendLine();

        sb.AppendLine($"MERGE [{schemaName}].[{tableName}] AS target");
        sb.AppendLine($"USING {tempTableName} AS source");

        // Build ON clause for primary key matching
        // Handle NULL values properly: use ISNULL to convert NULL to a sentinel value for comparison
        // This ensures that NULL = NULL evaluates to TRUE in the MERGE ON clause
        var matchConditions = primaryKeyColumns
            .Where(pk => mappings.Any(m => m.SqlColumnName.Equals(pk, StringComparison.OrdinalIgnoreCase)))
            .Select(pk =>
            {
                // For proper NULL handling, use a pattern that works for all data types
                // Use COALESCE with a type-appropriate sentinel value, or use ISNULL
                // For numeric types, use -1 or 0; for strings, use empty string; for dates, use a far future date
                // However, since PKs typically can't be NULL, we'll use a simpler approach:
                // Use ISNULL to handle potential NULLs, but also ensure exact matching
                return $"(target.[{pk}] = source.[{pk}] OR (target.[{pk}] IS NULL AND source.[{pk}] IS NULL))";
            });

        if (!matchConditions.Any())
        {
            // If primary key columns are not in mappings, fall back to INSERT
            // Exclude IsDeleted from regular mappings and handle it separately
            var insertMappingsFallback = mappings
                .Where(m => !string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase))
                .ToList();

            var columnList = string.Join(", ", insertMappingsFallback.Select(m => $"[{m.SqlColumnName}]"));
            var selectList = string.Join(", ", insertMappingsFallback.Select(m => $"source.[{m.SqlColumnName}]"));

            // Always set IsDeleted to 0/false if the column exists
            if (hasIsDeletedColumn)
            {
                columnList = columnList + ", [IsDeleted]";
                selectList = selectList + ", 0";
            }

            var insertQuery = $"INSERT INTO [{schemaName}].[{tableName}] ({columnList}) SELECT {selectList} FROM {tempTableName} AS source";

            try
            {
                if (enableIdentityInsert)
                {
                    var enableIdentityCmd = $"SET IDENTITY_INSERT [{schemaName}].[{tableName}] ON";
                    await using var cmd1 = new SqlCommand(enableIdentityCmd, connection, transaction);
                    cmd1.CommandTimeout = SqlCommandTimeout;
                    await cmd1.ExecuteNonQueryAsync(cancellationToken);
                }

                await using var command = new SqlCommand(insertQuery, connection, transaction);
                command.CommandTimeout = SqlCommandTimeout;
                var rowsAffected = await command.ExecuteNonQueryAsync(cancellationToken);
                return (rowsAffected, 0);
            }
            finally
            {
                if (enableIdentityInsert)
                {
                    var disableIdentityCmd = $"SET IDENTITY_INSERT [{schemaName}].[{tableName}] OFF";
                    await using var cmd2 = new SqlCommand(disableIdentityCmd, connection, transaction);
                    cmd2.CommandTimeout = SqlCommandTimeout;
                    await cmd2.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }

        sb.AppendLine($"ON {string.Join(" AND ", matchConditions)}");

        // Build WHEN MATCHED clause - UPDATE all non-PK columns
        // Get all columns that are in mappings but NOT in primary key and NOT IsDeleted
        var nonPkColumns = mappings
            .Where(m => !primaryKeyColumns.Any(pk => pk.Equals(m.SqlColumnName, StringComparison.OrdinalIgnoreCase)) &&
                       !string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase))
            .ToList();

        // Ensure we have columns to update
        if (nonPkColumns.Count == 0)
        {
            // If all columns are primary keys, we can't update anything
            // This shouldn't happen in practice, but handle it gracefully
            nonPkColumns = mappings
                .Where(m => !m.IsIdentity && !string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        // Always include WHEN MATCHED clause to update all non-PK columns
        // Ensure we update even if values are the same (MERGE will still count as UPDATE)
        sb.AppendLine("WHEN MATCHED THEN");
        sb.AppendLine("UPDATE SET");

        if (nonPkColumns.Any())
        {
            // Update ALL non-PK columns from source to target (excluding IsDeleted)
            // This will update columns even if source value is NULL (to set target to NULL)
            var updateClauses = nonPkColumns.Select(m => $"[{m.SqlColumnName}] = source.[{m.SqlColumnName}]");

            // Always set IsDeleted to 0/false if the column exists
            if (hasIsDeletedColumn)
            {
                updateClauses = updateClauses.Append("[IsDeleted] = 0");
            }

            sb.AppendLine(string.Join(",\n", updateClauses));
        }
        else
        {
            // If somehow no non-PK columns, update all columns except PK and IsDeleted (fallback)
            var allUpdateColumns = mappings
                .Where(m => !primaryKeyColumns.Contains(m.SqlColumnName, StringComparer.OrdinalIgnoreCase) &&
                           !string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase))
                .Select(m => $"[{m.SqlColumnName}] = source.[{m.SqlColumnName}]");

            if (allUpdateColumns.Any())
            {
                var updateClauses = allUpdateColumns.ToList();
                // Always set IsDeleted to 0/false if the column exists
                if (hasIsDeletedColumn)
                {
                    updateClauses.Add("[IsDeleted] = 0");
                }
                sb.AppendLine(string.Join(",\n", updateClauses));
            }
            else
            {
                // If no columns to update, we still need a valid UPDATE statement
                // This shouldn't happen in practice, but add a dummy update to prevent SQL syntax error
                // Try to find UpdatedAt or similar timestamp column, otherwise use a no-op update
                var timestampColumn = mappings.FirstOrDefault(m =>
                    m.SqlColumnName.Equals("UpdatedAt", StringComparison.OrdinalIgnoreCase) ||
                    m.SqlColumnName.Equals("LastUpdated", StringComparison.OrdinalIgnoreCase) ||
                    m.SqlColumnName.Equals("ModifiedDate", StringComparison.OrdinalIgnoreCase));

                if (timestampColumn != null)
                {
                    sb.AppendLine($"[{timestampColumn.SqlColumnName}] = ISNULL(source.[{timestampColumn.SqlColumnName}], GETDATE())");
                }
                else
                {
                    // Last resort: update the first non-PK column (shouldn't reach here)
                    var firstNonPk = mappings.FirstOrDefault(m =>
                        !primaryKeyColumns.Contains(m.SqlColumnName, StringComparer.OrdinalIgnoreCase) &&
                        !m.IsIdentity);
                    if (firstNonPk != null)
                    {
                        sb.AppendLine($"[{firstNonPk.SqlColumnName}] = source.[{firstNonPk.SqlColumnName}]");
                    }
                }
            }
        }

        // Build WHEN NOT MATCHED clause - INSERT new rows
        sb.AppendLine("WHEN NOT MATCHED BY TARGET THEN");

        // Exclude IsDeleted from regular mappings and handle it separately
        var insertMappings = mappings
            .Where(m => !string.Equals(m.SqlColumnName, "IsDeleted", StringComparison.OrdinalIgnoreCase))
            .ToList();

        var insertColumnsList = insertMappings.Select(m => $"[{m.SqlColumnName}]").ToList();
        var insertValuesList = insertMappings.Select(m => $"source.[{m.SqlColumnName}]").ToList();

        // Always set IsDeleted to 0/false if the column exists
        if (hasIsDeletedColumn)
        {
            insertColumnsList.Add("[IsDeleted]");
            insertValuesList.Add("0");
        }

        var insertColumns = string.Join(", ", insertColumnsList);
        var insertValues = string.Join(", ", insertValuesList);
        sb.AppendLine($"INSERT ({insertColumns}) VALUES ({insertValues})");

        // Output clause to track inserted/updated rows
        sb.AppendLine("OUTPUT $action INTO @MergeResults;");
        sb.AppendLine();
        sb.AppendLine("SELECT ");
        sb.AppendLine("    SUM(CASE WHEN Action = 'INSERT' THEN 1 ELSE 0 END) AS InsertedCount,");
        sb.AppendLine("    SUM(CASE WHEN Action = 'UPDATE' THEN 1 ELSE 0 END) AS UpdatedCount");
        sb.AppendLine("FROM @MergeResults;");

        try
        {
            if (enableIdentityInsert)
            {
                var enableIdentityCmd = $"SET IDENTITY_INSERT [{schemaName}].[{tableName}] ON";
                await using var cmd1 = new SqlCommand(enableIdentityCmd, connection, transaction);
                cmd1.CommandTimeout = SqlCommandTimeout;
                await cmd1.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var command = new SqlCommand(sb.ToString(), connection, transaction);
            command.CommandTimeout = SqlCommandTimeout;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            int insertedCount = 0;
            int updatedCount = 0;

            // The MERGE statement produces one result set (the OUTPUT into table variable)
            // Then the SELECT statement produces the second result set with counts
            // We need to skip the first result set (MERGE OUTPUT) and read the second (SELECT counts)
            // Note: If no rows were processed, @MergeResults will be empty and SUM will return NULL
            if (await reader.NextResultAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    // Handle NULL values from SUM (when no rows processed)
                    insertedCount = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                    updatedCount = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                }
            }
            else
            {
                // If NextResultAsync returns false, there's no second result set
                // This shouldn't happen, but handle it gracefully
                // The MERGE should have executed, so we'll return 0,0
                // In practice, this might indicate an error, but we'll let the transaction handle it
            }

            return (insertedCount, updatedCount);
        }
        finally
        {
            if (enableIdentityInsert)
            {
                var disableIdentityCmd = $"SET IDENTITY_INSERT [{schemaName}].[{tableName}] OFF";
                await using var cmd2 = new SqlCommand(disableIdentityCmd, connection, transaction);
                cmd2.CommandTimeout = SqlCommandTimeout;
                await cmd2.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    private async Task DropTempTableAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string tempTableName,
        CancellationToken cancellationToken)
    {
        var dropQuery = $"IF OBJECT_ID('tempdb..{tempTableName}') IS NOT NULL DROP TABLE {tempTableName}";
        await using var command = new SqlCommand(dropQuery, connection, transaction);
        command.CommandTimeout = SqlCommandTimeout;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private class ColumnMetadata
    {
        public string ColumnName { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
        public int? MaxLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsPrimaryKey { get; set; }
        public string? ForeignKeyTableSchema { get; set; }
        public string? ForeignKeyTableName { get; set; }
        public string? ForeignKeyColumnName { get; set; }
        public string? ForeignKeyLookupColumnName { get; set; } // Column in parent table to search by (usually a name/description column)
    }

    private class ColumnMapping
    {
        public string ExcelColumnName { get; set; } = string.Empty;
        public string SqlColumnName { get; set; } = string.Empty;
        public string SqlDataType { get; set; } = string.Empty;
        public bool IsIdentity { get; set; }
        public bool IsNullable { get; set; }
        public string? ForeignKeyTableSchema { get; set; }
        public string? ForeignKeyTableName { get; set; }
        public string? ForeignKeyColumnName { get; set; }
        public string? ForeignKeyLookupColumnName { get; set; }
    }

    private object TransformTurbineMaterialOfConstructionValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var materialStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel MaterialOfConstruction string → SQL integer value
        if (string.Equals(materialStr, "Select", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(materialStr, "TTL Standards", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(materialStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineStatusValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var statusStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel Status string → SQL integer value
        if (string.Equals(statusStr, "Draft", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(statusStr, "Active", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(statusStr, "Inactive", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineFootPrintReplacementValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var footPrintStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel FootPrintReplacement string → SQL integer value
        if (string.Equals(footPrintStr, "Yes", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(footPrintStr, "No", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineExhaustOrientationValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var exhaustStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel ExhaustOrientation string → SQL integer value
        if (string.Equals(exhaustStr, "Bottom", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(exhaustStr, "Top", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(exhaustStr, "Axial", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(exhaustStr, "Side", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineInletOrientationValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var inletStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel InletOrientation string → SQL integer value
        if (string.Equals(inletStr, "Standard", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(inletStr, "Top", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(inletStr, "Bottom", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(inletStr, "Side", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineDrivenEquipmentValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var drivenStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel DrivenEquipment string → SQL integer value
        if (string.Equals(drivenStr, "Alternator", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(drivenStr, "Compressor", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(drivenStr, "Fan", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(drivenStr, "Pump", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(drivenStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineNoiseLevelValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var noiseStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel NoiseLevel string → SQL integer value
        if (string.Equals(noiseStr, "90dBA", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(noiseStr, "85dBA", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(noiseStr, "109dBA(SPDP LT)", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(noiseStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(noiseStr, "Not Applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineRotationDirectionValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var rotationStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel RotationDirection string → SQL integer value
        if (string.Equals(rotationStr, "Standard", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(rotationStr, "Clock wise", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(rotationStr, "Clockwise", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(rotationStr, "Counter Clock wise", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(rotationStr, "Counter Clockwise", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineHMBDValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var hmbdStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel HMBD string → SQL integer value
        if (string.Equals(hmbdStr, "Not Enclosed", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(hmbdStr, "Enclosed", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(hmbdStr, "Not Submitted", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineYesNoRequiredValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var yesNoStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel Yes/No/Required string → SQL integer value
        if (string.Equals(yesNoStr, "Required", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(yesNoStr, "Yes", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(yesNoStr, "Not Required", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(yesNoStr, "No", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineSingleDoubleValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var singleDoubleStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel Single/Double string → SQL integer value
        if (string.Equals(singleDoubleStr, "Single", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(singleDoubleStr, "Double", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineStandardOthersValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var standardOthersStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel Standard/Others string → SQL integer value
        if (string.Equals(standardOthersStr, "Standard", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(standardOthersStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineTypeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var turbineTypeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel TurbineType string → SQL integer value
        if (string.Equals(turbineTypeStr, "Back Pressure", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(turbineTypeStr, "Condensing", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(turbineTypeStr, "Not Applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private async Task<bool> RecordExistsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string schemaName,
        string tableName,
        string idColumn,
        string idValue,
        CancellationToken cancellationToken)
    {
        // Try to parse the ID as a long
        if (!long.TryParse(idValue, out long parsedId))
        {
            return false; // Invalid ID format (non-numeric), so it doesn't exist
        }

        var query = $"SELECT COUNT(1) FROM [{schemaName}].[{tableName}] WHERE [{idColumn}] = @Id";
        await using var command = new SqlCommand(query, connection, transaction);
        command.Parameters.AddWithValue("@Id", parsedId);
        command.CommandTimeout = SqlCommandTimeout;

        var count = (int?)(await command.ExecuteScalarAsync(cancellationToken)) ?? 0;
        return count > 0;
    }

    private object TransformTurbineManufacturingStandardValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var manufacturingStandardStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel ManufacturingStandard string → SQL integer value
        if (string.Equals(manufacturingStandardStr, "TTL", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(manufacturingStandardStr, "API", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(manufacturingStandardStr, "API-611", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(manufacturingStandardStr, "API-612", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else if (string.Equals(manufacturingStandardStr, "IEC", StringComparison.OrdinalIgnoreCase))
        {
            return 5;
        }
        else if (string.Equals(manufacturingStandardStr, "Others", StringComparison.OrdinalIgnoreCase))
        {
            return 6;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }

    private object TransformTurbineGovernorScopeValue(object value, bool isNullable)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        // Convert to string and trim
        var governorScopeStr = value.ToString()?.Trim() ?? string.Empty;

        // Case-insensitive comparison and transform
        // Mapping: Excel GovernorScope string → SQL integer value
        if (string.Equals(governorScopeStr, "Select", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
        else if (string.Equals(governorScopeStr, "TTL", StringComparison.OrdinalIgnoreCase))
        {
            return 1;
        }
        else if (string.Equals(governorScopeStr, "Customer", StringComparison.OrdinalIgnoreCase))
        {
            return 2;
        }
        else if (string.Equals(governorScopeStr, "Existing", StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }
        else if (string.Equals(governorScopeStr, "Not Applicable", StringComparison.OrdinalIgnoreCase))
        {
            return 4;
        }
        else
        {
            // Default to NULL if column is nullable, otherwise 0
            return isNullable ? DBNull.Value : 0;
        }
    }
}

