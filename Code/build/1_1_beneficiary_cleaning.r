# Beneficiary data cleaning

# ====================================================================
# Script Name: 1_beneficiary_data_cleaning.R
# Author: Nikilesh Anusha
# Last Updated: May 19, 2025
# Description:
# This script performs comprehensive loading, cleaning, merging, and profiling
# of household-wise beneficiary data from multiple departments. The key steps
# include:
#
# 0. Boilerplate setup: Loads dependencies and sets the working environment.
# 1. Load beneficiary CSVs: Reads all CSVs in a specified folder, names them
#    dynamically, and loads them as data.tables into the global environment.
# 2. Extract common columns: Identifies common columns across all loaded data.tables
#    to enable safe merging.
# 3. Merge data.tables one-to-one: Performs strict one-to-one merges across all
#    beneficiary data.tables using common columns.
# 4. Hash PII fields: Hashes personally identifiable information (PII) using SHA-256
#    to anonymize sensitive fields such as name, Aadhar number, etc.
# 5. Rename columns: Applies consistent and descriptive names to variables for
#    clarity and downstream analysis.
# 6. Generate profile: Produces a detailed HTML summary report of the cleaned
#    and merged beneficiary dataset using the `summarytools` package.
#
# Output:
# - A cleaned and merged `merge_ben` data.table saved to `data_cleaned_path/ben_cleaned.csv`.
# - An HTML data profile summary saved to `output_path/ben_profile.html`.
# ====================================================================

# 0. Boilerplate -----------------------------------------------------------------
# Ensure packages are installed (run once if needed)
# install.packages(c("this.path", "fs"))

# Load necessary libraries
library(fs) # For path manipulation and file checks

# 1. Get the directory of the current script (falls back to working directory)
current_script_dir <- this.path::this.dir()
message(paste("Script directory:", current_script_dir))

# 2. Construct the normalized path to the setup file
#    (script_dir -> parent -> global_setups -> 0_setup_r.r)
setup_file_path <- path_norm(path(current_script_dir, "..", "global_setups", "0_setup_r.r"))
message(paste("Attempting to source setup file:", setup_file_path))

# 3. Source the setup file if it exists
if (file_exists(setup_file_path)) {
  message("File found. Sourcing...")
  source(setup_file_path, chdir = FALSE) # chdir=FALSE keeps CWD unchanged
  message("Sourcing complete.")
} else {
  warning(paste("Setup file NOT FOUND at:", setup_file_path))
}

# 1. Load beneficiary data------------------------------------------------------------------
# Loads all CSVs in the specified directory path and assigns them as data.tables
# to the global environment. The object name is determined by the first 4
# characters of the CSV file name, converted to lower case and prefixed with
# "ben_". Assigns NA to any CSV file that cannot be loaded as a data.table.
ben_wise <- file.path(raw_data_path, "HH Wise Data 29-4-25")

#' Load CSV files in a directory path as data.tables in the global environment.
#' @param directory_path A character string of the directory path containing the CSV files.
#' @return A character vector of the object names of the data.tables created.
#' @export
read_csvs_to_ben_objects <- function(directory_path) {
  if (!dir.exists(directory_path)) stop("Directory not found: ", directory_path)

  csv_files <- list.files(
    path = directory_path,
    pattern = "\\.csv$",
    full.names = TRUE,
    ignore.case = TRUE
  )

  if (length(csv_files) == 0) {
    return(invisible(character(0)))
  }

  created_names <- purrr::map_chr(csv_files, function(file) {
    obj_name <- file %>%
      basename() %>%
      str_remove("\\.csv$") %>%
      str_remove_all(" ") %>%
      str_sub(1, 4) %>%
      tolower() %>%
      paste0("ben_", .)

    if (nchar(obj_name) < 4) {
      return(NA_character_)
    }

    result <- purrr::safely(fread)(file, showProgress = FALSE)

    if (is.null(result$error)) {
      assign(obj_name, result$result, envir = .GlobalEnv)
      obj_name
    } else {
      NA_character_
    }
  })

  invisible(created_names[!is.na(created_names)])
}

# Load all CSVs as data.table
load_ben <- read_csvs_to_ben_objects(directory_path = ben_wise)

# Remove user_setting
rm(user_settings)

# 2. Get common columns ------------------------------------------------------------------
# Get common columns across all data.tables in the global environment
# that match the pattern "ben_". These are the columns that will be
# used to merge the data.tables.
common_columns <- mget(ls(pattern = "^ben_"), .GlobalEnv) %>%
  Filter(is.data.frame, .) %>%
  {
    # If there is more than one data.table, intersect the column names
    # to find the common columns.
    if (length(.) > 1) reduce(map(., names), intersect) else character(0)
  }

# Keep only valid data.tables
ben_ <- mget(ls(pattern = "^ben_"), .GlobalEnv) %>%
  purrr::keep(~ is.data.frame(.x))

# 3. Merge all data.tables strictly one-to-one ------------------------------------------------------------------
#' Ensure a one-to-one join by checking the uniqueness of keys
#'
#' @param dt1 A data.table to be merged.
#' @param dt2 A data.table to be merged.
#' @param by_cols A character vector of column names to join by.
#' @return A data.table resulting from a one-to-one merge of dt1 and dt2.
merge_one_to_one <- function(dt1, dt2, by_cols) {
  # Identify non-unique keys in each data.table
  key1 <- dt1[, .N, by = by_cols][N > 1]
  key2 <- dt2[, .N, by = by_cols][N > 1]

  if (nrow(key1) > 0 | nrow(key2) > 0) {
    # Determine unique keys to keep for merging
    keys_to_keep <- fsetdiff(
      dt1[, ..by_cols],
      rbindlist(list(key1[, ..by_cols], key2[, ..by_cols]), use.names = TRUE)
    )
    # Filter data.tables to only include rows with unique keys
    dt1 <- dt1[keys_to_keep, on = by_cols, nomatch = 0]
    dt2 <- dt2[keys_to_keep, on = by_cols, nomatch = 0]
  }

  # Perform the merge using the unique keys
  merge(dt1, dt2, by = by_cols, all = FALSE)
}

# Merge beneficiary data.tables one-to-one on common columns
if (length(ben_) == 0) {
  merge_ben <- data.table() # Initialize an empty data.table if no data is available
} else {
  merge_ben <- ben_[[1]] # Start with the first data.table
  for (i in 2:length(ben_)) {
    # Iteratively merge each subsequent data.table
    merge_ben <- merge_one_to_one(merge_ben, ben_[[i]], common_columns)
  }
}

# Clean up the environment by removing different dept beneficiary data.tables
rm(list = ls(pattern = "^ben_"))

merge_ben <- merge_ben %>%
  rename_with(tolower) %>% # Convert column names to lowercase
  janitor::clean_names(case = "snake") %>% # Format column names in snake_case
  mutate(across(everything(), ~ {
    x <- as.character(.)
    x[x == "\\N"] <- NA # Replace literal \N with NA
    x
  }))


# 4. Hash Personally Identifiable Information (PII) ----------------------------

# Function to hash specified columns using SHA-256
hash_columns <- function(df, cols_to_hash) {
  #' @param df A data.frame or data.table containing the data to be processed.
  #' @param cols_to_hash A character vector of column names to hash.
  #' @return A data.frame or data.table with the specified columns hashed.

  df %>%
    mutate(across(all_of(cols_to_hash), ~ {
      x <- as.character(.)
      vapply(x, function(val) {
        if (is.na(val) || val == "") NA_character_ else digest(val, algo = "sha256")
      }, character(1))
    }))
}
# Hash the PII columns in the merged data
merge_ben <- hash_columns(
  merge_ben,
  c("ben_name", "father_husband_name", "aadhar_no", "ration_card_no")
)

# 5. Rename columns ------------------------------------------------------------------
var_names <- names(merge_ben) %>%
  as.data.table()

renames <- c(
  "ag_soilcard", "ag_cmkisan", "ag_pmkisan", "coop_kisancreditcard", "eit_aadhar", "energy_electricty", "finance_jandhan",
  "fscw_ration", "fscw_lpg", "forest_kenduleaf", "forest_amajungle", "handloom_weaverpension", "handloom_pohiloom", "health_universalimmunization",
  "health_jananisurakshya", "health_jananisishusurakshya", "health_suffertb", "health_anaemia", "health_sicklecell", "health_noncommunicable",
  "health_medicatedmosquitonet", "health_birthdeathcert", "health_permanentdisabilitycert", "health_pmjaygjay", "hed_scholarshipugpg", "hed_scholarshipugpgno", "hed_scholarshipugpgnospec",
  "lesi_issuelabourcard", "lesi_nsky", "shakti_shg", "msme_vishwakarma", "odia_kalakar", "ben_id", "prdw_ruralhousing", "prdw_mgnregs", "prdw_pipedwater", "prdw_pipedwater_no",
  "prdw_pipedwater_noother", "prdw_indlatrine", "prdw_fra", "prdw_fra_yes", "prdw_yes_landdevmgnregs", "rdm_homesteadland", "rdm_agland", "rdm_misccert", "rdm_castecert", "sme_primaryschool",
  "sdte_sdyy", "ssepd_disabilities", "ssepd_disabilitypension", "ssepd_ssepd_disabilitycard", "ssepd_implantsaids", "ssepd_oldagepension", "ssepd_widowpension",
  "scst_frapatta", "scst_scholarship", "scst_residentialschool", "wcd_subhadra", "scd_anganwadi", "wcd_adol_mspy", "wcd_adol_snp", "wcd_preg_mspy", "wcd_preg_snp", "wcd_lact_mspy", "wcd_lact_snp", "wcd_mamata"
)

names(merge_ben)[20:85] <- renames

# 6. Save Cleaned Data ------------------------------------------------------------------
# Save the cleaned beneficiary data to a CSV file
write.csv(merge_ben, file.path(data_cleaned_path, "ben_cleaned.csv"), row.names = FALSE)

# 7. Data Profile ------------------------------------------------------------------
# Generate a detailed summary report of the merged beneficiary data
#' @description This section generates and saves a comprehensive summary profile of the beneficiary data post cleaning.
#' It utilizes the 'summarytools' package to create a visual HTML report.
summarytools::stview(dfSummary(merge_ben))

# Generate a summary of the merged data
merge_prof <- dfSummary(merge_ben)

# View the summary in the default viewer
summarytools::stview(merge_prof)

# Save the summary profile as an HTML file in the specified output directory
#' @param file The file path where the HTML summary will be saved.
view(merge_prof, file = file.path(output_path, "ben_profile.html"))
