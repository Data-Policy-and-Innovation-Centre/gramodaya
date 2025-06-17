# =============================================================================
# Author    : Aastha Mohapatra
# Date      : 2025-06-06
# Purpose   : Summary of missing data (counts only) by question and district
# =============================================================================

library(dplyr)
library(tidyr)
library(readr)

# ---- Step 1: Identify District Column ---------------------------------------
district_col <- "district_name"

# ---- Step 2: Convert to Character for Missing Check -------------------------
final_char <- final %>% mutate(across(everything(), as.character))

# ---- Step 3: Long Format Missing Summary ------------------------------------
na_summary_long <- final_char %>%
  pivot_longer(-all_of(district_col), names_to = "variable", values_to = "value") %>%
  mutate(is_na = is.na(value) | value == "") %>%
  group_by(!!sym(district_col), variable) %>%
  summarise(
    missing_count = sum(is_na),
    .groups = "drop"
  )

# ---- Step 4: Wide Format Table of Missing Counts ----------------------------
na_counts_wide <- na_summary_long %>%
  pivot_wider(names_from = variable, values_from = missing_count, values_fill = 0)

# ---- Step 5: Add Totals and Save --------------------------------------------
na_counts_wide_final <- na_counts_wide %>%
  mutate(Total_Missing_Per_District = rowSums(across(-all_of(district_col)))) %>%
  bind_rows(
    summarise(na_counts_wide, across(-all_of(district_col), sum)) %>%
      mutate(!!district_col := "Total_Missing_Per_Question")
  )

# ---- Step 6: Export Only Missing Summary ------------------------------------
output_folder <- "C:/Users/Admin/Box/2. Projects/11. PR&DW/10. Output"
dir.create(output_folder, recursive = TRUE, showWarnings = FALSE)

write_csv(na_counts_wide_final,
          file.path(output_folder, "missing_summary_by_district_and_question.csv"),
          na = "")
