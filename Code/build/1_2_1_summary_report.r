source(file.path(dirname(rstudioapi::getActiveDocumentContext()$path), "0_setup_r.r"))
df <- read_dta(file.path(box_path,"9. Cleaned Data", "gramodaya_community_data.dta"))
df <- df %>%
  rename_with(tolower) %>%                       # Make column names lowercase
  janitor::clean_names(case = "snake") %>%       # Convert to snake_case
  mutate(across(everything(), ~ {
    x <- as.character(.)
    x[x == "\\N"] <- NA                          # Replace literal \N with NA
    x
  }))

# 3. Generate summary report
sum_table <- dfSummary(df)

# 4. Save to HTML
view(sum_table, file = file.path(output_path,"summary_report.html"))
