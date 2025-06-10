# =============================================================================
# Author    : Aastha Mohapatra
# Date      : 2025-06-06
# Purpose   : Summary of missing data (counts only) by question and district
# =============================================================================

library(dplyr)
library(tidyr)
library(readr)

# ---- Step 1: Load and Clean Dataset -----------------------------------------
# Replace special missing values
final[final == "\\N"] <- NA
final[final == "--select--"] <- NA
final[final == ""] <- NA

# ---- Step 2: Drop Repeated School and AWC Variables -------------------------
repeated_vars <- grep("_(School\\.Name|School\\.Type|Toilet|Kitchen\\.facility|Anganwadi\\.Centre\\.Name|Own\\.\\.\\.Rental\\.Building|Drinking\\.water\\.facility|Toilet\\.facility|Electrified|Kitchen\\.facility|LPG\\.Connection)_\\d+$", 
                      names(final), value = TRUE)
final <- final %>% select(-all_of(repeated_vars))

# ---- Step 3: Convert to Character for Standardization -----------------------
final_char <- final %>% mutate(across(everything(), as.character))

# ---- Step 4: Identify District Column ---------------------------------------
district_col <- "district_name"

# ---- Step 5: Long Format Missing Summary ------------------------------------
na_summary_long <- final_char %>%
  pivot_longer(-all_of(district_col), names_to = "variable", values_to = "value") %>%
  mutate(is_na = is.na(value) | value == "") %>%
  group_by(!!sym(district_col), variable) %>%
  summarise(
    missing_count = sum(is_na),
    .groups = "drop"
  )

# ---- Step 6: Wide Format Table of Missing Counts ----------------------------
na_counts_wide <- na_summary_long %>%
  pivot_wider(names_from = variable, values_from = missing_count, values_fill = 0)

# ---- Step 7: Add Row and Column Totals --------------------------------------
# Row totals (per district)
na_counts_wide <- na_counts_wide %>%
  mutate(Total_Missing_Per_District = rowSums(across(-all_of(district_col))))

# Column totals (per question)
counts_totals <- na_counts_wide %>%
  summarise(across(-all_of(district_col), sum)) %>%
  mutate(!!district_col := "Total_Missing_Per_Question")

# Combine into final table
na_counts_wide_final <- bind_rows(na_counts_wide, counts_totals)

# ---- Step 8: Export ----------------------------------------------------------
output_folder <- "C:/Users/Admin/Box/2. Projects/11. PR&DW/10. Output"
dir.create(output_folder, recursive = TRUE, showWarnings = FALSE)

write_csv_safe <- function(df, filename) {
  filepath <- file.path(output_folder, filename)
  tryCatch({
    write_csv(df, filepath, na = "")
    message(sprintf("Successfully wrote: %s", filepath))
  }, error = function(e) {
    warning(sprintf("Failed to write %s: %s", filepath, e$message))
  })
}

write_csv_safe(na_counts_wide_final, "community_missing_counts.csv")
