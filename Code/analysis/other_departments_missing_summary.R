# =============================================================================
# Title     : other_department_missing_summary.r
# Author    : Aastha Mohapatra
# Date      : 2025-07-14
# Purpose   : Calculates total missing percentage for WCD, SME and ENRG. 
# Aim is to filter the dataset, isolating it to the departments currently in 
# the wide format. It converts them to the long format. 
# It then assigns logics to each department separately and calculates
# the total missing percentages per department per district. 
# =============================================================================

library(dplyr)
library(tidyr)
library(readr)

# -------------------------------
# LOAD DATA
# -------------------------------

# Update path if needed
data_path <- file.path(data_cleaned_path, "community_cleaned.csv")
df <- read_csv(data_path, show_col_types = FALSE)

# -------------------------------
# EDUCATION (EDU)
# -------------------------------

edu_name_cols <- names(df)[grepl("^q16a_EDU_school_name_", names(df))]
edu_prefixes <- c("q16b_EDU_school_type_", 
                  "q16c_EDU_school_toilets_", 
                  "q17_EDU_school_kitchen_")

# Convert relevant columns to character
edu_vars <- c(
  edu_name_cols,
  unlist(lapply(edu_prefixes, function(pre) names(df)[grepl(paste0("^", pre), names(df))]))
)
df[edu_vars] <- lapply(df[edu_vars], as.character)

# Pivot school names first
edu_long <- df %>%
  select(
    district_id, district_name, block_id, block_name,
    panchayat_id, panchayat_name, village_id, village_name,
    all_of(edu_name_cols)
  ) %>%
  pivot_longer(
    cols = all_of(edu_name_cols),
    names_to = "school_no",
    names_pattern = "q16a_EDU_school_name_(\\d+)$",
    values_to = "school_name"
  ) %>%
  filter(!is.na(school_name), school_name != "", school_name != "NA")

# Loop over other variables and merge
for (prefix in edu_prefixes) {
  var_cols <- names(df)[grepl(paste0("^", prefix), names(df))]
  if (length(var_cols) > 0) {
    value_name <- case_when(
      grepl("school_type", prefix) ~ "school_type",
      grepl("school_toilets", prefix) ~ "school_toilets",
      grepl("school_kitchen", prefix) ~ "school_kitchen",
      TRUE ~ "value"
    )
    temp_long <- df %>%
      select(
        district_id, district_name, block_id, block_name,
        panchayat_id, panchayat_name, village_id, village_name,
        all_of(var_cols)
      ) %>%
      pivot_longer(
        cols = all_of(var_cols),
        names_to = "school_no",
        names_pattern = paste0(prefix, "(\\d+)$"),
        values_to = value_name
      )
    edu_long <- edu_long %>%
      left_join(
        temp_long,
        by = c(
          "district_id", "district_name",
          "block_id", "block_name",
          "panchayat_id", "panchayat_name",
          "village_id", "village_name",
          "school_no"
        )
      )
  }
}

# Summarise at district level
edu_district <- edu_long %>%
  group_by(district_name) %>%
  summarise(
    total_schools = n(),
    missing_school_type = sum(is.na(school_type) | school_type %in% c("", "NA")),
    missing_school_toilets = sum(is.na(school_toilets) | school_toilets %in% c("", "NA")),
    missing_school_kitchen = sum(is.na(school_kitchen) | school_kitchen %in% c("", "NA")),
    pct_missing_school_type = ifelse(total_schools == 0, NA, 100 * missing_school_type / total_schools),
    pct_missing_school_toilets = ifelse(total_schools == 0, NA, 100 * missing_school_toilets / total_schools),
    pct_missing_school_kitchen = ifelse(total_schools == 0, NA, 100 * missing_school_kitchen / total_schools),
    EDU_TOTAL_MISSING_PCT = rowMeans(
      cbind(
        pct_missing_school_type,
        pct_missing_school_toilets,
        pct_missing_school_kitchen
      ), na.rm = TRUE
    )
  )

# -------------------------------
# WCD (AWCs)
# -------------------------------

wcd_name_cols <- names(df)[grepl("^q30a_WCD_awc_name_", names(df))]
wcd_prefixes <- c("q30b_WCD_awc_building_type_",
                  "q30c_WCD_awc_drinking_water_",
                  "q30d_WCD_awc_toilet_",
                  "q30e_WCD_awc_electrified_",
                  "q30f_WCD_awc_kitchen_",
                  "q30g_WCD_awc_lpg_connection_")

# Convert to character
wcd_vars <- c(
  wcd_name_cols,
  unlist(lapply(wcd_prefixes, function(pre) names(df)[grepl(paste0("^", pre), names(df))]))
)
df[wcd_vars] <- lapply(df[wcd_vars], as.character)

# Pivot AWC names
wcd_long <- df %>%
  select(
    district_id, district_name, block_id, block_name,
    panchayat_id, panchayat_name, village_id, village_name,
    all_of(wcd_name_cols)
  ) %>%
  pivot_longer(
    cols = all_of(wcd_name_cols),
    names_to = "awc_no",
    names_pattern = "q30a_WCD_awc_name_(\\d+)$",
    values_to = "awc_name"
  ) %>%
  filter(!is.na(awc_name), awc_name != "", awc_name != "NA")

# Loop over WCD fields
for (prefix in wcd_prefixes) {
  var_cols <- names(df)[grepl(paste0("^", prefix), names(df))]
  if (length(var_cols) > 0) {
    value_name <- sub(".*awc_", "", sub("_$", "", prefix))
    temp_long <- df %>%
      select(
        district_id, district_name, block_id, block_name,
        panchayat_id, panchayat_name, village_id, village_name,
        all_of(var_cols)
      ) %>%
      pivot_longer(
        cols = all_of(var_cols),
        names_to = "awc_no",
        names_pattern = paste0(prefix, "(\\d+)$"),
        values_to = value_name
      )
    wcd_long <- wcd_long %>%
      left_join(
        temp_long,
        by = c(
          "district_id", "district_name",
          "block_id", "block_name",
          "panchayat_id", "panchayat_name",
          "village_id", "village_name",
          "awc_no"
        )
      )
  }
}

wcd_district <- wcd_long %>%
  group_by(district_name) %>%
  summarise(
    total_awcs = n(),
    missing_building_type = sum(is.na(building_type) | building_type %in% c("", "NA")),
    missing_drinking_water = sum(is.na(drinking_water) | drinking_water %in% c("", "NA")),
    missing_toilet = sum(is.na(toilet) | toilet %in% c("", "NA")),
    missing_electrified = sum(is.na(electrified) | electrified %in% c("", "NA")),
    missing_kitchen = sum(is.na(kitchen) | kitchen %in% c("", "NA")),
    missing_lpg_connection = sum(is.na(lpg_connection) | lpg_connection %in% c("", "NA")),
    pct_missing_building_type = ifelse(total_awcs == 0, NA, 100 * missing_building_type / total_awcs),
    pct_missing_drinking_water = ifelse(total_awcs == 0, NA, 100 * missing_drinking_water / total_awcs),
    pct_missing_toilet = ifelse(total_awcs == 0, NA, 100 * missing_toilet / total_awcs),
    pct_missing_electrified = ifelse(total_awcs == 0, NA, 100 * missing_electrified / total_awcs),
    pct_missing_kitchen = ifelse(total_awcs == 0, NA, 100 * missing_kitchen / total_awcs),
    pct_missing_lpg_connection = ifelse(total_awcs == 0, NA, 100 * missing_lpg_connection / total_awcs),
    WCD_TOTAL_MISSING_PCT = rowMeans(
      cbind(
        pct_missing_building_type,
        pct_missing_drinking_water,
        pct_missing_toilet,
        pct_missing_electrified,
        pct_missing_kitchen,
        pct_missing_lpg_connection
      ), na.rm = TRUE
    )
  )

# -------------------------------
# ENRG (Hamlets)
# -------------------------------

hamlet_name_cols <- names(df)[grepl("^q27a_energy_hamlet_name_", names(df))]
hamlet_elec_cols <- names(df)[grepl("^q27b_energy_hamlet_electrified_", names(df))]

df[hamlet_name_cols] <- lapply(df[hamlet_name_cols], as.character)
df[hamlet_elec_cols] <- lapply(df[hamlet_elec_cols], as.character)

# Pivot hamlet names
enrg_long <- df %>%
  select(
    district_id, district_name, block_id, block_name,
    panchayat_id, panchayat_name, village_id, village_name,
    all_of(hamlet_name_cols)
  ) %>%
  pivot_longer(
    cols = all_of(hamlet_name_cols),
    names_to = "hamlet_no",
    names_pattern = "q27a_energy_hamlet_name_(\\d+)$",
    values_to = "hamlet_name"
  )

# Count hamlets per village
hamlet_count <- enrg_long %>%
  filter(!is.na(hamlet_name), hamlet_name != "", hamlet_name != "NA") %>%
  group_by(district_name, block_name, panchayat_name, village_name) %>%
  summarise(num_hamlets = n(), .groups = "drop")

# Pivot electrification
if (length(hamlet_elec_cols) > 0) {
  elec_long <- df %>%
    select(
      district_id, district_name, block_id, block_name,
      panchayat_id, panchayat_name, village_id, village_name,
      all_of(hamlet_elec_cols)
    ) %>%
    pivot_longer(
      cols = all_of(hamlet_elec_cols),
      names_to = "hamlet_no",
      names_pattern = "q27b_energy_hamlet_electrified_(\\d+)$",
      values_to = "hamlet_electrified"
    )
  
  enrg_long <- enrg_long %>%
    left_join(
      elec_long,
      by = c(
        "district_id", "district_name",
        "block_id", "block_name",
        "panchayat_id", "panchayat_name",
        "village_id", "village_name",
        "hamlet_no"
      )
    )
}
 
enrg_village <- hamlet_count %>%
  right_join(
    df %>%
      distinct(district_name, block_name, panchayat_name, village_name),
    by = c("district_name", "block_name", "panchayat_name", "village_name")
  ) %>%
  mutate(num_hamlets = replace_na(num_hamlets, 0)) %>%
  mutate(
    pct_missing_electrified = ifelse(
      num_hamlets == 0, 
      100, 
      enrg_long %>%
        filter(!is.na(hamlet_name), hamlet_name != "", hamlet_name != "NA") %>%
        group_by(district_name, block_name, panchayat_name, village_name) %>%
        summarise(
          missing_elec = sum(is.na(hamlet_electrified) | hamlet_electrified %in% c("", "NA")),
          total_hamlets = n(),
          .groups = "drop"
        ) %>%
        mutate(pct_missing = 100 * missing_elec / total_hamlets) %>%
        right_join(
          df %>%
            distinct(district_name, block_name, panchayat_name, village_name),
          by = c("district_name", "block_name", "panchayat_name", "village_name")
        ) %>%
        pull(pct_missing) %>% replace_na(100)
    )
  )

enrg_district <- enrg_village %>%
  group_by(district_name) %>%
  summarise(
    ENRG_TOTAL_MISSING_PCT = mean(pct_missing_electrified, na.rm = TRUE)
  )

# -------------------------------
# FINAL JOIN
# -------------------------------

final_summary <- edu_district %>%
  full_join(wcd_district, by = "district_name") %>%
  full_join(enrg_district, by = "district_name")

print(final_summary, n = Inf)

write_csv(final_summary, file.path(output_path, "other_departments_missing_summary.csv"), row.names = FALSE)
