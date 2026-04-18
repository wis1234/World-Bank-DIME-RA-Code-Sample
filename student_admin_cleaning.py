# ============================================================================
#  Project  : Tuition Payments – Baseline
#  Authors  : KCDEV RAs
#  Purpose  : Clean and merge student administrative and survey data
#  Software : Python 3.10+
#  Requires : pip install pandas pyreadstat openpyxl
# ============================================================================

import os
import re
import sys
import logging
import pandas as pd
import pyreadstat


# ----------------------------------------------------------------------------
# 0. Paths and logging
# ----------------------------------------------------------------------------
ADMIN_PATH = (
    r"C:\RONALDO\CKDEV\DIGITALISATION\Data_analysis"
    r"\Datasets\student_outputs\student_identification"
)
OUTPUT_PATH = (
    r"C:\RONALDO\CKDEV\DIGITALISATION\Data_analysis"
    r"\Datasets\student_outputs"
)

os.makedirs(ADMIN_PATH, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(ADMIN_PATH, "student_clean_log.txt"), encoding="utf-8"
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger()
log.info("=== Student cleaning pipeline started ===")


# ----------------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------------
def load(path: str) -> pd.DataFrame:
    df, _ = pyreadstat.read_dta(path, apply_value_formats=True)
    log.info(f"Loaded  {path}  ({len(df):,} rows)")
    return df


def save(df: pd.DataFrame, path: str) -> None:
    pyreadstat.write_dta(df, path)
    log.info(f"Saved   {path}  ({len(df):,} rows)")


def p(filename: str) -> str:
    """Return full path inside ADMIN_PATH."""
    return os.path.join(ADMIN_PATH, filename)


def clean_school_name(name) -> str:
    if pd.isna(name):
        return name
    name = str(name).lower().strip()
    name = re.sub(r"\s+", "", name)
    name = name.replace("-", "")
    return name


# ----------------------------------------------------------------------------
# Core record reconciliation helper
#
# Some students appear under two IDs because their names were entered
# slightly differently across sources (e.g. "Brigitte" vs "Brigite").
# This function copies admin columns from the source row into the target
# row and removes the now-redundant source row.
# ----------------------------------------------------------------------------
ADMIN_COLS = [
    "stud_name", "sch_name_str", "stud_gender",
    "study_level", "status", "tuition_payment",
    "annual_grade", "prim_phone", "sec_phone", "verification",
]


def transfer_and_drop(df: pd.DataFrame, source_id: str, target_id: str) -> pd.DataFrame:
    src = df[df["student_id"] == source_id]
    tgt = df["student_id"] == target_id

    if src.empty:
        log.warning(f"  Source not found: {source_id}")
        return df
    if tgt.sum() == 0:
        log.warning(f"  Target not found: {target_id}")
        return df

    for col in ADMIN_COLS:
        if col in df.columns:
            val = src.iloc[0][col]
            if pd.notna(val):
                df.loc[tgt, col] = val

    return df[df["student_id"] != source_id].copy()


# ----------------------------------------------------------------------------
# 1. Prepare administrative dataset
# ----------------------------------------------------------------------------
log.info("--- 1. Administrative data preparation ---")

admin = load(p("final_student_admin_data.dta"))

# Encode gender as a numeric category
admin["stud_gender"] = (
    admin["stud_gender"].astype(str).str.strip().replace({"nan": pd.NA})
)
gender_map = {
    g: i
    for i, g in enumerate(sorted(admin["stud_gender"].dropna().unique()), start=1)
}
admin["stud_gender"] = admin["stud_gender"].map(gender_map)

# Standardise school names: lower-case, no spaces, no hyphens
admin["sch_name_str"] = admin["sch_name_str"].apply(clean_school_name)

save(admin, p("student_admin_data_gender_encoded.dta"))


# ----------------------------------------------------------------------------
# 2. Standardise school names in the survey dataset
# ----------------------------------------------------------------------------
log.info("--- 2. Survey school name standardisation ---")

survey = load(p("student_data_with_id_harmonized.dta"))

direct_fixes = {
    "complexescolairefaucon-secondaire":       "complexescolairefauconsecondaire",
    "complexescolairesaintkisito-secondaire":  "complexescolairesaintkisitosecondaire",
    "cpegxabbe-pierre":                        "cpegexabbepierre",
    "csbelavenir-secondaire":                  "csbelavenirsecondaire",
    "cssecondairesecondairenotredamedevict..": "cssecondairesecondairenotredamedevictoire",
    "sainttheresedel'enfantjesusoash-ong":     "sainttheresedel'enfantjesusoashong",
}
survey["sch_name_str"] = survey["sch_name_str"].replace(direct_fixes)


def fix_college(name):
    if pd.isna(name):
        return name
    norm = re.sub(r"[\s']", "", str(name)).lower()
    if re.search(r"college.*professionnel", norm):
        return "collegeleprofessionnel"
    return name


def fix_elisee(name):
    if pd.isna(name):
        return name
    sl = str(name).lower()
    if "elis" in sl and "csp" in sl:
        return "cspstelisee"
    return name


survey["sch_name_str"] = survey["sch_name_str"].apply(fix_college)
survey["sch_name_str"] = survey["sch_name_str"].apply(fix_elisee)

unmatched = survey[
    ~survey["sch_name_str"].isin(set(admin["sch_name_str"].dropna()))
]["sch_name_str"].value_counts()
log.info(
    f"Unmatched school names after fixes:\n"
    f"{unmatched.to_string() if len(unmatched) else '  None'}"
)

save(survey, p("student_data_with_id_harmonized.dta"))


# ----------------------------------------------------------------------------
# 3. Merge administrative and survey records
# ----------------------------------------------------------------------------
log.info("--- 3. Merging admin and survey data ---")

df = pd.merge(
    admin, survey,
    on="student_id",
    how="outer",
    suffixes=("", "_survey"),
    indicator=True,
)
log.info(f"Match summary:\n{df['_merge'].value_counts().to_string()}")

save(df, p("student_survey_admin_data_merge.dta"))
df.to_excel(p("student_survey_admin_data_merge.xlsx"), index=False)


# ----------------------------------------------------------------------------
# 4. Resolve duplicate student records across all schools
# ----------------------------------------------------------------------------
log.info("--- 4. Resolving duplicate records ---")

pairs = [
    # ── collegeleprofessionnel ──────────────────────────────────────────────
    ("collegeleprofessionnel_cocoevan_YO6",                         "collegeleprofessionnel_cocoevan_JHA"),
    ("collegeleprofessionnel_counoudjivianney_ZTD",                 "collegeleprofessionnel_counondjivianney_PY4"),
    ("collegeleprofessionnel_houngbedjianath_7DF",                  "collegeleprofessionnel_houngbedjianath_UW0"),
    ("collegeleprofessionnel_kintokodÉo-gratias_9SP",               "collegeleprofessionnel_kintokoafi-deo-gracias_6NY"),
    ("collegeleprofessionnel_n`tchachasnel_C19",                    "collegeleprofessionnel_n'tchachasnel_JAL"),
    # ── complexescolairefauconsecondaire ────────────────────────────────────
    ("complexescolairefaucon-secondaire_amadoumariam_6C7",          "complexescolairefaucon-secondaire_amadoumariam_7ER"),
    ("complexescolairefaucon-secondaire_dramaneamar_L5C",           "complexescolairefaucon-secondaire_dramaneamar_HTK"),
    ("complexescolairefaucon-secondaire_dramaneroufeidah_HTK",      "complexescolairefaucon-secondaire_dramaneroufeidah_L5C"),
    ("complexescolairefaucon-secondaire_zaÏzonoujoanes_BWS",        "complexescolairefaucon-secondaire_zaÏzonoujoanes_V02"),
    # ── cpegantoinelaurentlavoisier ─────────────────────────────────────────
    ("cpegantoinelaurentlavoisier_amouzounbrigitte_4NF",            "cpegantoinelaurentlavoisier_amounzounbrigitte_TJB"),
    ("cpegantoinelaurentlavoisier_kourashouahib_V1M",               "cpegantoinelaurentlavoisier_kourashouahib_CHC"),
    ("cpegantoinelaurentlavoisier_kourateslim_CHC",                 "cpegantoinelaurentlavoisier_kourateslim_V1M"),
    ("cpegantoinelaurentlavoisier_togbeviraymond_AX9",              "cpegantoinelaurentlavoisier_togbeviraymond_KMH"),
    ("cpegantoinelaurentlavoisier_zountounnoumahougnonezekiel_2TS", "cpegantoinelaurentlavoisier_zoutounouezechiel_I7B"),
    # ── cpegenagnon2013okounseme ────────────────────────────────────────────
    ("cpegenagnon2013okounseme_lassokpehamdalathforlachade_OR0",    "cpegenagnon2013okounseme_lassokpehamdalathforlachade_IY9"),
    # ── cpeglamethode ───────────────────────────────────────────────────────
    ("cpeglamethode_agbessiosias_6J2",                              "cpeglamethode_agbessiozias_VQJ"),
    ("cpeglamethode_aguessyagbanprince_3F1",                        "cpeglamethode_aguessiagbanprince_M4R"),
    ("cpeglamethode_edouhemeck_9LN",                                "cpeglamethode_edouhemeck_X7X"),
    # ── cpeglavoixdeleternel ────────────────────────────────────────────────
    ("cpeglavoixdel'eternel_agouchibrunelle_0MS",                   "cpeglavoixdel'eternel_agouchibrunelle_UTC"),
    ("cpeglavoixdel'eternel_agouchijaelle_UTC",                     "cpeglavoixdel'eternel_agouchijaelle_0MS"),
    ("cpeglavoixdel'eternel_hamaamina_40J",                         "cpeglavoixdel'eternel_hamaamina_3GM"),
    ("cpeglavoixdel'eternel_hamaaminou_3GM",                        "cpeglavoixdel'eternel_hamaaminou_40J"),
    # ── cpegleflambeaudes3s ─────────────────────────────────────────────────
    ("cpegleflambeaudes3s_danviemmanuel_3X4",                       "cpegleflambeaudes3s_danviemmanuel_H79"),
    ("cpegleflambeaudes3s_diakitefarid_0WL",                        "cpegleflambeaudes3s_diakitefarid_VHJ"),
    ("cpegleflambeaudes3s_idrissouhayerath_VHJ",                    "cpegleflambeaudes3s_idrissouhayerath_0WL"),
    ("cpegleflambeaudes3s_tokplonoudelmas_30Z",                     "cpegleflambeaudes3s_tokplonoudelmas_NA1"),
    ("cpegleflambeaudes3s_tokplonouraphaËl_NA1",                    "cpegleflambeaudes3s_tokplonouraphaËl_30Z"),
    # ── cpeglerepere ────────────────────────────────────────────────────────
    ("cpeglerepere_agassoussiosias_WU7",                            "cpeglerepere_agassoussiosias_YR9"),
    ("cpeglerepere_alohanemmanuel_DCP",                             "cpeglerepere_alohanemmanuel_LX8"),
    ("cpeglerepere_amalipeace_VMU",                                 "cpeglerepere_amalipeace_Y7I"),
    ("cpeglerepere_blakouassitrifene_Y86",                          "cpeglerepere_blakouassitryphene_GGD"),
    ("cpeglerepere_dakpoadoree_Q2W",                                "cpeglerepere_dakpoadoree_NR7"),
    ("cpeglerepere_houegnounmetolinel_4EH",                         "cpeglerepere_houegnounmetolinel_EC8"),
    ("cpeglerepere_kakpourielle_34X",                               "cpeglerepere_kakpourielle_EAV"),
    ("cpeglerepere_kakpotoundeshalom_8ET",                          "cpeglerepere_kakposhalom_11E"),
    ("cpeglerepere_koussemiprince_L9F",                             "cpeglerepere_koussemiprince_BMJ"),
    ("cpeglerepere_langanfinisnel_JKT",                             "cpeglerepere_langanfinisnelle_3VC"),
    ("cpeglerepere_tokpolumiere_LX8",                               "cpeglerepere_tokpolumiere_DCP"),
    # ── cpegomsdjeffa ───────────────────────────────────────────────────────
    ("cpegomsdjeffa_dah-momkponemilienne_RPG",                      "cpegomsdjeffa_dah-momkponemilienne_FPX"),
    ("cpegomsdjeffa_dedewanouepiphane_PTM",                         "cpegomsdjeffa_dedewanouepiphane_KUV"),
    ("cpegomsdjeffa_dedewanoumarie-madelaine_KUV",                  "cpegomsdjeffa_dedewanoumarie-madelaine_PTM"),
    ("cpegomsdjeffa_djossougracia_ENV",                             "cpegomsdjeffa_djossougracia_695"),
    ("cpegomsdjeffa_djossoumelane_695",                             "cpegomsdjeffa_djossoumelane_ENV"),
    ("cpegomsdjeffa_gbedemefalcao_T0I",                             "cpegomsdjeffa_gbedemefalcao_XLL"),
    ("cpegomsdjeffa_gbedemejulius_XLL",                             "cpegomsdjeffa_gbedemejulius_T0I"),
    ("cpegomsdjeffa_orou-gnaousamsiya_FPX",                         "cpegomsdjeffa_orou-gnaousamsiya_RPG"),
    ("cpegomsdjeffa_wenonmaoulouck_XKL",                            "cpegomsdjeffa_wenonmaoulouck_KGJ"),
    # ── cpegsaintbernard ────────────────────────────────────────────────────
    ("cpegsaintbernard_agbowaÏangemarie_HIH",                       "cpegsaintbernard_agbowaÏannemarie_MNV"),
    ("cpegsaintbernard_djikpet0isnele_N93",                         "cpegsaintbernard_djikpetoisnel_E7A"),
    ("cpegsaintbernard_hounhogbedaria_S5N",                         "cpegsaintbernard_hounhogbedaria_IZ9"),
    ("cpegsaintbernard_hounhomelassia_IZ9",                         "cpegsaintbernard_hounhomelassia_S5N"),
    ("cpegsaintbernard_tossoumariette_B5A",                         "cpegsaintbernard_tossoumariette_L1A"),
    # ── cpegwoleshoyinka ────────────────────────────────────────────────────
    ("cpegwoleshoyinka_adjallaexaucÉ_ECK",                          "cpegwoleshoyinka_adjallaexaucÉ_3OP"),
    ("cpegwoleshoyinka_ahouanganssijuvenal_3OP",                    "cpegwoleshoyinka_ahouanganssijuvenal_ECK"),
    # ── cplareussitebohicon ─────────────────────────────────────────────────
    ("cplareussitebohicon_atekpamitrinitÉ_D2A",                     "cplareussitebohicon_atekpamitrinitÉ_HGA"),
    ("cplareussitebohicon_atekpamitriomphe_HGA",                    "cplareussitebohicon_atekpamitriomphe_D2A"),
    ("cplareussitebohicon_assahisaac_HGA",                          "cplareussitebohicon_asaahisaac_XAM"),
    ("cplareussitebohicon_bossaqueen_79I",                          "cplareussitebohicon_bossaken_SSX"),
    ("cplareussitebohicon_chodatonaureole_7LZ",                     "cplareussitebohicon_chodatinaureole_PEF"),
    ("cplareussitebohicon_guedezoumeulrich_OL0",                    "cplareussitebohicon_guedezounmeulrich_V0L"),
    ("cplareussitebohicon_nandjiewis_XTR",                          "cplareussitebohicon_nadjielvis_L8Q"),
    # ── csbelavenirsecondaire ───────────────────────────────────────────────
    ("csbelavenir-secondaire_kombettoyannn'nakabayabi_V1M",         "csbelavenir-secondaire_kombettoyannn'nakabayabi_J74"),
    ("csbelavenir-secondaire_koumbettokouatchaangefelix_J74",       "csbelavenir-secondaire_koumbettokouatchaangefelix_V1M"),
    ("csbelavenir-secondaire_linkponfifameblandine_V4L",            "csbelavenir-secondaire_linkponfifameblandine_ZSA"),
    ("csbelavenir-secondaire_linkponmahugnonbernice_ZSA",           "csbelavenir-secondaire_linkponmahugnonbernice_V4L"),
    ("csbelavenir-secondaire_yessouffoufridaoÇa_OXS",              "csbelavenir-secondaire_yessouffoufridaoÇa_KTU"),
    ("csbelavenir-secondaire_yessouffouromziath_KTU",               "csbelavenir-secondaire_yessouffouromziath_OXS"),
    # ── cscoeurd'or ─────────────────────────────────────────────────────────
    ("cscoeurd'or_hounmenouhermanne_P5Q",                           "cscoeurd'or_hounmenouhermanne_KH8"),
    ("cscoeurd'or_hounmenoujordan_KH8",                             "cscoeurd'or_hounmenoujordan_88G"),
    ("cscoeurd'or_hounmenoujordanie_88G",                           "cscoeurd'or_hounmenoujordanie_P5Q"),
    ("cscoeurd'or_lokojesse_AFS",                                   "cscoeurd'or_lokojesse_3QZ"),
    ("cscoeurd'or_lokoobed_3QZ",                                    "cscoeurd'or_lokoobed_AFS"),
    # ── csfilslepere ────────────────────────────────────────────────────────
    ("csfilslepere_amouzou-houessoushalom_1C2",                     "csfilslepere_amoussou-houessoushalom_77G"),
    ("csfilslepere_kanfonhoueamour_HFM",                            "csfilslepere_kanfonhoueamour_Y4X"),
    ("csfilslepere_missihounangia_8HB",                             "csfilslepere_missihounangia_UST"),
    ("csfilslepere_missihounjeandedieu_5WN",                        "csfilslepere_missihounjeandedieu_2GZ"),
    # ── cslarbredesucces ────────────────────────────────────────────────────
    ("csl'arbredesucces_sononestelle_6B1",                          "csl'arbredesucces_sononestelle_O2F"),
    ("csl'arbredesucces_sononhermione_O2F",                         "csl'arbredesucces_sononhermione_6B1"),
    # ── csleselitesdehevie ──────────────────────────────────────────────────
    ("csleselitesdehevie_noumonespoir_KXT",                         "csleselitesdehevie_noumonespoir_QFD"),
    # ── cspsthoreb ──────────────────────────────────────────────────────────
    ("cspsthoreb_danmadouashley_UIM",                               "cspsthoreb_danmadouashley_2IP"),
    ("cspsthoreb_danmadoueloick_2IP",                               "cspsthoreb_danmadoueloick_UIM"),
    ("cspsthoreb_kpatinvohespera_M09",                              "cspsthoreb_kpatinvohespera_VIG"),
    ("cspsthoreb_kpatinvohespero_VIG",                              "cspsthoreb_kpatinvohespero_M09"),
    # ── cssaintevolonte ─────────────────────────────────────────────────────
    ("cssaintevolonte_accrobessiruth_PQ9",                          "cssaintevolonte_accrobessiruth_OGO"),
    ("cssaintevolonte_accrombessiestelle_OGO",                      "cssaintevolonte_accrombessiestelle_PQ9"),
    ("cssaintevolonte_eddjrokintomuriella_UO1",                     "cssaintevolonte_eddjrokintomuriella_2DZ"),
    ("cssaintevolonte_makoubenoit_4JF",                             "cssaintevolonte_makoubenoit_HNZ"),
    ("cssaintevolonte_makoubenoit_HNZ",                             "cssaintevolonte_makouerwan_E48"),
    ("cssaintevolonte_makouerwan_HNZ",                              "cssaintevolonte_makouerwan_E48"),
    ("cssaintevolonte_makougustave_E48",                            "cssaintevolonte_makougustave_4JF"),
    # ── csstfrancoisxavier ──────────────────────────────────────────────────
    ("csstfrancoisxavier_alladeobed_CIY",                           "csstfrancoisxavier_alladeobed_K6D"),
    ("csstfrancoisxavier_anagonougajunior_VYC",                     "csstfrancoisxavier_anagonougajunior_QUL"),
    ("cssaintevolonte_eddjrokintomuriella_UO1",                     "cssaintevolonte_eddjrokintomuriella_2DZ"),
    ("csstfrancoisxavier_gomezalegria_YZM",                         "csstfrancoisxavier_gomezalegria_WQL"),
    ("csstfrancoisxavier_gomezonesime_WQL",                         "csstfrancoisxavier_gomezonesime_YZM"),
    ("csstfrancoisxavier_hazoumegeorgia_VI7",                       "csstfrancoisxavier_hazoumegeorgia_IYU"),
    ("csstfrancoisxavier_simbattiben_GIP",                          "csstfrancoisxavier_simbattiben_IHS"),
    ("csstfrancoisxavier_simbattisandra_IHS",                       "csstfrancoisxavier_simbattisandra_GIP"),
    ("csstfrancoisxavier_soglogboregina_ZQH",                       "csstfrancoisxavier_soglongberegina_UP9"),
    ("csstfrancoisxavier_tchokodoairnicha_60I",                     "csstfrancoisxavier_tchokodoairnicha_44X"),
    ("csstfrancoisxavier_tchokodomeislline_HM4",                    "csstfrancoisxavier_tchokodomeislline_FLV"),
    # ── cswottoschool ───────────────────────────────────────────────────────
    ("cswottoschool_abdouramdane_A1B",                              "cswottoschool_abdouramdane_VND"),
    ("cswottoschool_addadesmonde_KDH",                              "cswottoschool_addadesmonde_I8R"),
    ("cswottoschool_affogbehabib_9VT",                              "cswottoschool_affogbehabib_TCK"),
    ("cswottoschool_affogbelahaqq_TCK",                             "cswottoschool_affogbelahaqq_9VT"),
    ("cswottoschool_batoumgeradine_KBF",                            "cswottoschool_batoumgeraldine_ERG"),
    ("cswottoschool_bonikoraseraphine_I8R",                         "cswottoschool_bonikoraseraphine_KDH"),
    ("cswottoschool_hamidoubassime_VND",                            "cswottoschool_hamidoubassime_A1B"),
    ("cswottoschool_hamidouhamaou_H8L",                             "cswottoschool_hamidouhasnaou_A2P"),
    ("cswottoschool_hounnourodolphe_J8N",                           "cswottoschool_hounnourodolpho_MP6"),
    # ── lelaurier ───────────────────────────────────────────────────────────
    ("lelaurier_azonbakinchristelle_U2H",                           "lelaurier_azonbakinchristelle_NL1"),
    ("lelaurier_mamasannioumiyath_2RL",                             "lelaurier_mamasannioumiyath_K4L"),
    # ── santatheresa ────────────────────────────────────────────────────────
    ("santatheresa_avagbomarie-ella_MI8",                           "santatheresa_avagbomarie-ella_WTL"),
    ("santatheresa_dossoureginedossi_MI8",                          "santatheresa_dossoureginedossi_WTL"),
    # ── stfelicitegodomey ───────────────────────────────────────────────────
    ("stfelicitegodomey_ahouanhitolovelarissa_ZW4",                 "stfelicitegodomey_ahouanhitolovelarissa_72O"),
    ("stfelicitegodomey_houedanmelissajuliana_HVU",                 "stfelicitegodomey_houedanmelissajuliana_GL2"),
    ("stfelicitegodomey_hounfodjijosue_27Z",                        "stfelicitegodomey_hounfodjijosue_8OF"),
    ("stfelicitegodomey_issaachraf_1JJ",                            "stfelicitegodomey_issaachraf_FRT"),
    ("stfelicitegodomey_issamalikatou_FRT",                         "stfelicitegodomey_issamalikatou_1JJ"),
    ("stfelicitegodomey_tonehessigracemarina_G21",                  "stfelicitegodomey_tonehessigracemarina_LUL"),
    ("stfelicitegodomey_tonehessijoseeannick_LUL",                  "stfelicitegodomey_tonehessijoseeannick_G21"),
]

for source, target in pairs:
    df = transfer_and_drop(df, source, target)

# Drop confirmed exact duplicates (same student entered twice with no survey link)
exact_duplicates = [
    "complexescolairefaucon-secondaire_atikaprÉcieux_U33",
    "cpeglamethode_edouhemeck_V0G",
    "cpeglamethode_kouetohousabine_9W6",
    "cspsthoreb_danmadoueloik_K5F",
]
df = df[~df["student_id"].isin(exact_duplicates)].copy()

# Drop admin-only placeholder rows (no name, no survey record)
for school in [
    "collegeleprofessionnel", "cpegsaintbernard",
    "csstfrancoisxavier", "cswottoschool", "stfelicitegodomey",
]:
    df = df[~df["student_id"].str.contains(f"{school}_unknown", na=False)].copy()

# Correct one school name recorded inconsistently across files
df["sch_name_str"] = df["sch_name_str"].replace(
    {"csplaprovidenceikmsecondaire": "cpslaprovidenceikmsecondaire"}
)


# ----------------------------------------------------------------------------
# 5. Load consolidated survey file and remove records with no student name
# ----------------------------------------------------------------------------
log.info("--- 5. Consolidated survey file ---")

consolidated_path = os.path.join(
    OUTPUT_PATH, "student_identification", "student_survey_consolidated.xlsx"
)
df = pd.read_excel(consolidated_path)
pyreadstat.write_dta(df, p("admin_survey_data_sch_level_stanbilized.dta"))

before = len(df)
df = df[df["stud_name"].notna()].copy()
log.info(f"Removed {before - len(df)} rows with no student name  ({len(df):,} remaining)")


# ----------------------------------------------------------------------------
# 6. Verification field
# ----------------------------------------------------------------------------
log.info("--- 6. Verification field ---")

df["_ver"] = df["verification"].astype(str).str.lower().str.strip()
df["verification_num"] = pd.NA
df.loc[df["_ver"].isin(["yes", "oui"]), "verification_num"] = 1
df.loc[df["_ver"].isin(["no",  "non"]), "verification_num"] = 0
df.loc[df["verification"] == "C",       "verification_num"] = 1   # "C" recorded as Yes

still_open = df[df["verification_num"].isna() & df["verification"].notna()]
if len(still_open):
    log.info(f"Unmapped verification values:\n"
             f"{still_open['verification'].value_counts().to_string()}")

df = df.drop(columns=["verification", "_ver"])
df = df.rename(columns={"verification_num": "verification"})
log.info(f"Verification:\n{df['verification'].value_counts(dropna=False).to_string()}")


# ----------------------------------------------------------------------------
# 7. Gender standardisation
# ----------------------------------------------------------------------------
log.info("--- 7. Gender ---")

df["stud_gender"] = df["stud_gender"].astype(str).str.strip()
df.loc[df["stud_gender"].str.contains(r"(?i)female", na=False), "stud_gender"] = "Female"
df.loc[df["stud_gender"].str.contains(r"(?i)\bmale\b", na=False), "stud_gender"] = "Male"
df.loc[df["stud_gender"].str.fullmatch(r"0|\s*|nan", na=False), "stud_gender"] = pd.NA

log.info(f"Gender:\n{df['stud_gender'].value_counts(dropna=False).to_string()}")
log.info(
    f"Missing gender: {df['stud_gender'].isna().sum()} — "
    "records also missing other admin fields, likely new enrolments not yet in admin data."
)


# ----------------------------------------------------------------------------
# 8. Study level
# ----------------------------------------------------------------------------
missing_study   = df["study_level"].isna().sum()
matched_missing = df[df["study_level"].isna() & df["interview__id"].notna()].shape[0]
log.info(
    f"Missing study_level: {missing_study} total, "
    f"{matched_missing} have a survey record — likely recent enrolments."
)


# ----------------------------------------------------------------------------
# 9. Status
# ----------------------------------------------------------------------------
log.info("--- 9. Status ---")

df["status"] = df["status"].astype(str).str.strip()
df.loc[df["status"].str.fullmatch(r"N\s+|A", na=False), "status"] = "N"
df.loc[df["status"].isin([".", "nan", ""]), "status"] = pd.NA

log.info(f"Status:\n{df['status'].value_counts(dropna=False).to_string()}")


# ----------------------------------------------------------------------------
# 10. Tuition payment
# ----------------------------------------------------------------------------
log.info("--- 10. Tuition payment ---")

df["tuition_payment"] = df["tuition_payment"].astype(str).str.strip()
df.loc[df["tuition_payment"].str.contains(r"NON SOLDE\s*|IMPAYE", na=False), "tuition_payment"] = "NON SOLDE"
df.loc[df["tuition_payment"].str.contains(r"SOLDE\s+",             na=False), "tuition_payment"] = "SOLDE"
df.loc[df["tuition_payment"].isin(["nan", ""]), "tuition_payment"] = pd.NA

log.info(f"Tuition payment:\n{df['tuition_payment'].value_counts(dropna=False).to_string()}")


# ----------------------------------------------------------------------------
# 11. Annual grade
# ----------------------------------------------------------------------------
log.info(f"Missing annual_grade: {df['annual_grade'].isna().sum()}")


# ----------------------------------------------------------------------------
# 12. Save final outputs
# ----------------------------------------------------------------------------
log.info("--- 12. Final save ---")

pyreadstat.write_dta(df, p("final_student_clean.dta"))
df.to_excel(p("final_student_clean.xlsx"), index=False)

log.info(f"Final dataset: {len(df):,} rows across {df['sch_name_str'].nunique()} schools")
log.info("=== Pipeline completed successfully ===")
