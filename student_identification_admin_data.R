# ============================================================================
#  Project  : Tuition Payments – Baseline
#  Authors  : Ronaldo Agbohou
#  Date     : 26 March 2026
#  Purpose  : Extract student dataset and generate student IDs
#  Software : R (haven, dplyr, stringr, writexl)
# ============================================================================


# ----------------------------------------------------------------------------
# 0. Initialisation
# ----------------------------------------------------------------------------
library(haven)    # read/write .dta files
library(dplyr)    # data manipulation
library(stringr)  # string cleaning
library(writexl)  # export to .xlsx


# ----------------------------------------------------------------------------
# 1. Project directory and folders
# ----------------------------------------------------------------------------
project <- "C:/RONALDO/CKDEV/DIGITALISATION/Data_analysis/Datasets/student_outputs"
setwd(project)

dir.create("student_identification",                        showWarnings = FALSE)
dir.create("student_identification/66_missing_v2",          showWarnings = FALSE)

# Open log
sink("student_identification/student_log.txt", split = TRUE)
cat("Log started:", format(Sys.time()), "\n\n")


# ----------------------------------------------------------------------------
# 2. Load data
# ----------------------------------------------------------------------------
df <- read_dta("clean_student_harmonized_matching_sample_all.dta")

cat("Variables:\n"); print(names(df))
cat("\nMissing stud_name:", sum(is.na(df$stud_name)), "\n")


# ----------------------------------------------------------------------------
# 3. Decode school name (Stata labelled integer → string)
# ----------------------------------------------------------------------------
df <- df |>
  mutate(school_name_str = as.character(as_factor(sch_name)))


# ----------------------------------------------------------------------------
# 4. Clean school name  (lower-case, remove spaces, cap at 200 chars)
# ----------------------------------------------------------------------------
df <- df |>
  mutate(school_name_str = str_to_lower(school_name_str),
         school_name_str = str_remove_all(school_name_str, " "),
         school_name_str = str_sub(school_name_str, 1, 200))


# ----------------------------------------------------------------------------
# 5. Clean student name
# ----------------------------------------------------------------------------
df <- df |>
  mutate(
    stud_name_clean = str_to_lower(stud_name),
    stud_name_clean = str_remove_all(stud_name_clean, " "),
    stud_name_clean = if_else(is.na(stud_name_clean) | stud_name_clean == "",
                              "unknown", stud_name_clean)
  )


# ----------------------------------------------------------------------------
# 6. Random alphanumeric suffix  (3 characters, seed-reproducible)
# ----------------------------------------------------------------------------
set.seed(12345)

rand_char <- function(n) {
  # Each character is a digit (0-9) or uppercase letter (A-Z)
  x <- floor(runif(n) * 36)
  ifelse(x < 10, as.character(x), LETTERS[x - 9])
}

df <- df |>
  mutate(rand_part = paste0(rand_char(n()), rand_char(n()), rand_char(n())))


# ----------------------------------------------------------------------------
# 7. Assemble student ID:  <school>_<name>_<rand>
# ----------------------------------------------------------------------------
df <- df |>
  mutate(student_id = paste(school_name_str, stud_name_clean, rand_part, sep = "_"))


# ----------------------------------------------------------------------------
# 8. Check duplicates
# ----------------------------------------------------------------------------
n_dup <- sum(duplicated(df$student_id))
cat("\nDuplicate student IDs:", n_dup, "\n")
if (n_dup > 0) {
  cat("Duplicated values:\n")
  print(df$student_id[duplicated(df$student_id)])
}


# ----------------------------------------------------------------------------
# 9. Create empty placeholder columns for Excel
# ----------------------------------------------------------------------------
df <- df |>
  mutate(
    Study_level      = NA_character_,
    status           = NA_character_,
    Tuition_payment  = NA_character_,
    Annual_grade     = NA_real_,
    prim_phone       = NA_character_,
    sec_phone        = NA_character_,
    verification     = NA_character_
  )


# ----------------------------------------------------------------------------
# 10. Decode school name for export (readable label, no quotes)
# ----------------------------------------------------------------------------
df <- df |>
  mutate(
    sch_name_str = as.character(as_factor(sch_name)),
    sch_name_str = str_remove_all(sch_name_str, '"')
  )


# ----------------------------------------------------------------------------
# 11. Select export columns
# ----------------------------------------------------------------------------
export_cols <- c(
  "student_id", "stud_name", "sch_name_str", "stud_gender",
  "Study_level", "status", "Tuition_payment", "Annual_grade",
  "prim_phone", "sec_phone", "verification"
)

df_export <- df |> select(all_of(export_cols))


# ----------------------------------------------------------------------------
# 12. Export global file
# ----------------------------------------------------------------------------
write_xlsx(df_export,
           "student_identification/digitalization_student.xlsx")
cat("\nSaved: digitalization_student.xlsx  (", nrow(df_export), "rows )\n")


# ----------------------------------------------------------------------------
# 13. Export one .xlsx per school
# ----------------------------------------------------------------------------
schools <- unique(df$sch_name_str)
cat("\nExporting", length(schools), "school files...\n")

for (school in schools) {

  school_df <- df_export |> filter(sch_name_str == school)

  # Build a safe filename (mirrors Stata's subinstr cleaning)
  clean_name <- school |>
    str_replace_all(" ", "_") |>
    str_remove_all("[/\\\\'\"]") |>
    str_replace_all("-", "_")

  out_path <- paste0("student_identification/66_missing_v2/", clean_name, ".xlsx")
  write_xlsx(school_df, out_path)
  cat("  →", out_path, "(", nrow(school_df), "rows )\n")
}


# ----------------------------------------------------------------------------
# 14. Save full dataset with IDs
# ----------------------------------------------------------------------------
write_dta(df, "student_identification/student_data_with_id.dta")
cat("\nSaved: student_data_with_id.dta\n")


# ----------------------------------------------------------------------------
# Close log
# ----------------------------------------------------------------------------
cat("\n✔ Student extraction complete.\n")
cat("Log closed:", format(Sys.time()), "\n")
sink()
