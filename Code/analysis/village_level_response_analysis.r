# Village-level response analysis
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
# ========== 1. Load and Prepare Data ==========

# Load village-level data
village <- read.csv(file.path(data_cleaned_path, "community_cleaned.csv"), colClasses = "character") %>%
    select(
        1:8,
        {
            remaining <- names(.)[9:ncol(.)]
            remaining[order(as.numeric(str_extract(remaining, "\\d+")))]
        }
    )

# Define base columns
common_columns <- names(village)[1:8]

# Subset by department
wcd <- village[, c(common_columns, grep("_WCD_", names(village), value = TRUE))]
edu <- village[, c(common_columns, grep("_EDU_", names(village), value = TRUE))]
other_dept <- village[, c(common_columns, setdiff(names(village), c(common_columns, grep("_WCD|_EDU", names(village), value = TRUE))))]

# Load and clean energy data
energy <- read.csv(file.path(gramodaya_village_path, "Energy Department (Hamlet Wise).csv"), colClasses = "character")
village_energy <- energy %>%
    mutate(hamlet_energy_electrified = na_if(hamlet_energy_electrified, "")) %>%
    group_by(village_id, village_name) %>%
    summarise(
        yes_count = sum(hamlet_energy_electrified == "YES", na.rm = TRUE),
        no_count = sum(hamlet_energy_electrified == "NO", na.rm = TRUE),
        missing_count = sum(is.na(hamlet_energy_electrified)),
        total = n(),
        .groups = "drop"
    ) %>%
    mutate(
        q27_ENRG_electrification_status = case_when(
            yes_count == total ~ "Full",
            yes_count > 0 & yes_count < total ~ "Partial",
            yes_count == 0 & no_count > 0 ~ "None",
            yes_count == 0 & no_count == 0 ~ NA
        )
    ) %>%
    select(village_id, village_name, q27_ENRG_electrification_status) %>%
    mutate(across(everything(), as.character))

# Merge energy with other_dept
other_dept <- other_dept %>%
    left_join(village_energy, by = c("village_id", "village_name"))

# ========== 2. Compute Percent Table (Long and Wide) ==========

collapse_questions <- c(
    "q15_AGRI_solar_pumps", "q21_HLTH_health_workers_sanctioned", "q22_HLTH_health_workers_present", "q39_IT_network_provider", "q3_PRDW_tubewell_count", "q5_PRDW_pipe_water_supply",
    "q8b_PRDW_road_belongs_to", "q9_PRDW_concrete_road_length", "q10_PRDW_concrete_road_requirement", "q44_TRANS_bus_type", "q38_IT_mobile_network_covered", "q35b_FIN_bank_name"
)

percent_table <- other_dept %>%
    pivot_longer(cols = -(1:8), names_to = "question", values_to = "response") %>%
    mutate(
        department = str_extract(question, "(?<=_)[^_]+(?=_)"),
        response = if_else(
            question %in% collapse_questions,
            if_else(is.na(response), "Missing", "Non-missing"),
            response
        ),
        response = if_else(response == "IN GP HQRS", "YES", response)
    ) %>%
    group_by(district_name, department, question, response, .drop = FALSE) %>%
    summarise(count = n(), .groups = "drop") %>%
    group_by(district_name, department, question) %>%
    mutate(
        proportion = count / sum(count),
        floor_pct = floor(proportion * 100),
        remainder = proportion * 100 - floor_pct,
        missing = 100 - sum(floor_pct),
        add_one = row_number() <= missing,
        percent = floor_pct + ifelse(add_one, 1, 0)
    ) %>%
    ungroup() %>%
    mutate(
        response = ifelse(is.na(response), "Missing", response),
        q_num = as.numeric(str_extract(question, "\\d+")),
        q_sub = str_extract(question, "(?<=\\d)[a-zA-Z]?")
    ) %>%
    arrange(district_name, department, q_num, q_sub) %>%
    filter(!department %in% c("name", "energy")) %>%
    select(district_name, department, question, response, count, percent)

# Pivot to wide format
percent_table_wide <- percent_table %>%
    pivot_wider(
        id_cols = c(district_name, department, question),
        names_from = response,
        values_from = percent,
        values_fill = NA
    ) %>%
    arrange(district_name, as.numeric(str_extract(question, "\\d+")))

# Export to Excel by district
district_groups <- percent_table_wide %>%
    group_by(district_name) %>%
    group_split()

district_names <- percent_table_wide %>%
    group_by(district_name) %>%
    group_keys() %>%
    pull(district_name)

names(district_groups) <- district_names

wb <- createWorkbook()
for (i in seq_along(district_groups)) {
    addWorksheet(wb, sheetName = make.names(district_names[i]))
    writeData(wb, sheet = i, district_groups[[i]])
    setColWidths(wb, sheet = i, cols = 1:ncol(district_groups[[i]]), widths = "auto")
}
saveWorkbook(wb, file = file.path(output_path, "district_percent.xlsx"), overwrite = TRUE)

# ========== 2B. Save other_percent.xlsx (by department) ==========

# Reuse long percent_table without district grouping
percent_table_by_dept <- other_dept %>%
    pivot_longer(cols = -(1:8), names_to = "question", values_to = "response") %>%
    mutate(
        department = str_extract(question, "(?<=_)[^_]+(?=_)"),
        response = if_else(
            question %in% collapse_questions,
            if_else(is.na(response), "Missing", "Non-missing"),
            response
        ),
        response = if_else(response == "IN GP HQRS", "YES", response)
    ) %>%
    group_by(department, question, response, .drop = FALSE) %>%
    summarise(count = n(), .groups = "drop") %>%
    group_by(department, question) %>%
    mutate(
        proportion = count / sum(count),
        floor_pct = floor(proportion * 100),
        remainder = proportion * 100 - floor_pct,
        missing = 100 - sum(floor_pct),
        add_one = row_number() <= missing,
        percent = floor_pct + ifelse(add_one, 1, 0)
    ) %>%
    ungroup() %>%
    mutate(
        response = ifelse(is.na(response), "Missing", response),
        q_num = as.numeric(str_extract(question, "\\d+")),
        q_sub = str_extract(question, "(?<=\\d)[a-zA-Z]?")
    ) %>%
    arrange(department, q_num, q_sub) %>%
    filter(!department %in% c("name", "energy")) %>%
    select(department, question, response, count, percent)

# Pivot wide
percent_table_dept_wide <- percent_table_by_dept %>%
    pivot_wider(id_cols = c(department, question), names_from = response, values_from = percent, values_fill = NA) %>%
    arrange(as.numeric(str_extract(question, "\\d+")))

# Split and write
dept_groups <- percent_table_by_dept %>%
    group_by(department) %>%
    group_split()

dept_names <- percent_table_by_dept %>%
    group_by(department) %>%
    group_keys() %>%
    pull(department)

names(dept_groups) <- dept_names

writexl::write_xlsx(dept_groups, path = file.path(output_path, "other_percent.xlsx"))

# ========== 2C. Save percent.xlsx (wide format by department) ==========

writexl::write_xlsx(
    percent_table_dept_wide,
    path = file.path(output_path, "percent.xlsx")
)

# ========== 3. Yes/Full % Summary ==========

yes_questions <- c(
    "q27_ENRG_electrification_status",
    "q11_FS_fair_price_shop",
    "q20_HLTH_sub_centre",
    "q35_FIN_AVAIL_bank",
    "q43_TRANS_bus_to_block",
    "q1_PRDW_community_centre",
    "q41_IT_mo_seva_kendra"
)

district_yes_percent_long <- other_dept %>%
    select(district_name, all_of(yes_questions)) %>%
    pivot_longer(cols = all_of(yes_questions), names_to = "question", values_to = "response") %>%
    mutate(
        response = na_if(response, ""),
        response = if_else(response == "IN GP HQRS", "YES", response),
        is_yes = case_when(
            question == "q27_ENRG_electrification_status" ~ response == "Full",
            TRUE ~ response == "YES"
        )
    ) %>%
    group_by(district_name, question) %>%
    summarise(
        total = n(),
        n_yes = sum(is_yes, na.rm = TRUE),
        yes_percent = round(n_yes / total, 2),
        .groups = "drop"
    )

# Pivot wide and order
district_wide <- district_yes_percent_long %>%
    pivot_wider(id_cols = district_name, names_from = question, values_from = yes_percent)

question_order <- tibble(question = yes_questions) %>%
    mutate(q_num = as.numeric(str_extract(question, "\\d+"))) %>%
    arrange(q_num) %>%
    pull(question)

district_wide_ordered <- district_wide %>%
    mutate(avg_yes_percent = round(rowMeans(across(all_of(question_order)), na.rm = TRUE), 2)) %>%
    select(district_name, all_of(question_order), avg_yes_percent) %>%
    arrange(desc(avg_yes_percent))

# Save Yes% summary
writexl::write_xlsx(
    list("district_yes_percent" = district_wide_ordered),
    path = file.path(output_path, "district_yes_percent.xlsx")
)
