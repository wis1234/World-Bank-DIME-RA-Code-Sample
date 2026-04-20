/*============================================================================
  Project  : Tuition Payments – Baseline
  Author  : Ronaldo Agbohou
  Date     : 23 February 2026
  Purpose  : Extract screening variables from survey versions 2–4,
             backfill *_ao variables when the main respondent is female,
             and produce a single pooled screening dataset.
  Software : Stata 17
============================================================================*/


*------------------------------------------------------------------------------
* 0. Initialisation
*------------------------------------------------------------------------------
version 17.0
clear all
set more off
set varabbrev off
capture log close


*------------------------------------------------------------------------------
* 1. Global paths
*------------------------------------------------------------------------------
global project "C:\RONALDO\CKDEV\DIGITALISATION\Data_analysis\Explanation with Dr Mahounan\extracted screening datasets"

global v2_path "$project\Initial datasets\v2\zip"
global v3_path "$project\Initial datasets\v3\zip"
global v4_path "$project\Initial datasets\v4\zip"
global out     "$project\screening_datasets"

capture mkdir "$out"

log using "$out\screening_log.smcl", replace


*------------------------------------------------------------------------------
* 2. Variable lists (defined once, reused everywhere)
*------------------------------------------------------------------------------

* Part-1 variables collected directly from the respondent
global part1_vars ///
    resp_name resp_age_dec2025 ///
    edu1 edu2 edu2_other edu3 ///
    occ1 occ1_other ///
    v101 v101_O v102 v102_O v109 v109_O ///
    v142 v142_O v143 v143_O v144 v144_O ///
    phone_own borrow_phone borrow_phone_other ///
    type_phone phone_use_dec phone_use_dec_others ///
    prim_phone sec_phone

* Screening variables kept in the final output
global screen_vars ///
    consent_ao resp_name_ao resp_age_dec2025_ao ///
    edu1_ao edu2_ao edu2_other_ao edu3_ao ///
    occ1_ao occ1_other_ao ///
    phone_own_ao borrow_phone_ao borrow_phone_other_ao ///
    type_phone_ao phone_use_dec_ao phone_use_dec_others_ao ///
    prim_phone_ao sec_phone_ao ///
    ph_disrupt_12m ph_disrupt_last ph_disrupt_freq ///
    int_ever_used int_last_use ///
    int_disrupt_12m int_disrupt_last int_disrupt_freq ///
    mobile_bank_access ///
    health_info_phone_use ///
    health_recv__1 health_recv__2 health_recv__3 ///
    health_recv__4 health_recv__5 health_recv__6 ///
    disrup_1 disrup_2 disrup_3 disrup_4 ///
    disrup_5 disrup_6 disrup_7 disrup_8 ///
    phone_lock app_lock_any phone_block_ever ///
    harass_content_ever harass_sexual_ever privacy_breach_ever ///
    fraud_self_ever ///
    fraud_self_type__1 fraud_self_type__2 fraud_self_type__3 ///
    fraud_self_type__4 fraud_self_type__5 fraud_self_type__6 ///
    fraud_self_type__7 fraud_self_type__8 fraud_self_type__98 ///
    other_fraud_self_type ///
    fraud_other_known other_fraud_other_type ///
    misinfo_exposed ///
    info_verify_ever info_verify_last ///
    info_verify_method__1 info_verify_method__2 info_verify_method__3 ///
    info_verify_method__4 info_verify_method__5 info_verify_method__98 ///
    VIIId_others


*------------------------------------------------------------------------------
* 3. Helper program: backfill *_ao variables for female main respondents
*
*    Usage: backfill_female_ao using "<path/to/PDB.dta>", save("<out.dta>")
*
*    Logic:
*      (a) Extract part-1 values for female respondents (resp_gender == 0).
*      (b) Merge back into the full dataset.
*      (c) Copy those values into the corresponding *_ao variables.
*      (d) Save the updated dataset.
*------------------------------------------------------------------------------
program define backfill_female_ao
    syntax using/, save(string)

    * --- (a) Extract female part-1 values ---
    use `"`using'"', clear
    keep if resp_gender == 0
    keep interview__id $part1_vars
    tempfile female_vals
    save `female_vals'

    * --- (b) Reload full dataset and merge ---
    use `"`using'"', clear
    merge 1:1 interview__id using `female_vals', keep(master match) nogenerate

    * --- (c) Backfill *_ao variables for female observations only ---
    foreach v of global part1_vars {
        capture confirm variable `v'_ao
        if !_rc replace `v'_ao = `v' if resp_gender == 0
    }

    * --- (d) Save ---
    save `"`save'"', replace
end


*------------------------------------------------------------------------------
* 4. Process version 2  (3 servers)
*------------------------------------------------------------------------------
foreach s in 1 2 3 {
    backfill_female_ao                          ///
        using "$v2_path\v2_server`s'\PDB.dta"  ///
        , save("$v2_path\v2_server`s'_clean.dta")
}

use          "$v2_path\v2_server1_clean.dta", clear
append using "$v2_path\v2_server2_clean.dta"
append using "$v2_path\v2_server3_clean.dta"
save "$v2_path\v2_final.dta", replace

keep $screen_vars
save "$v2_path\v2_final_screening.dta", replace


*------------------------------------------------------------------------------
* 5. Process version 3  (3 servers)
*------------------------------------------------------------------------------
foreach s in 1 2 3 {
    backfill_female_ao                          ///
        using "$v3_path\v3_server`s'\PDB.dta"  ///
        , save("$v3_path\v3_server`s'_clean.dta")
}

use          "$v3_path\v3_server1_clean.dta", clear
append using "$v3_path\v3_server2_clean.dta"
append using "$v3_path\v3_server3_clean.dta"
save "$v3_path\v3_final.dta", replace

keep $screen_vars
save "$v3_path\v3_final_screening.dta", replace


*------------------------------------------------------------------------------
* 6. Process version 4  (5 servers – no female backfill needed)
*------------------------------------------------------------------------------
use          "$v4_path\v4_server1\PDB.dta", clear
forvalues s = 2/5 {
    append using "$v4_path\v4_server`s'\PDB.dta"
}
save "$v4_path\v4_final.dta", replace

keep $screen_vars
save "$v4_path\v4_final_screening.dta", replace


*------------------------------------------------------------------------------
* 7. Pool all versions into one screening dataset
*------------------------------------------------------------------------------
use          "$v2_path\v2_final_screening.dta", clear
append using "$v3_path\v3_final_screening.dta"
append using "$v4_path\v4_final_screening.dta"

save "$out\final_screening.dta", replace


*------------------------------------------------------------------------------
* 8. Close log
*------------------------------------------------------------------------------
log close
