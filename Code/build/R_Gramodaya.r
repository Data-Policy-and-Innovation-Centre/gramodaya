# R setup for gramodaya data

## 0. Setup & Boilerplate -----------------------------------------------------------------
rm(list = ls(all.names = TRUE)) # Clears all objects from the environment
gc()

### 0.1 Directories -----------------------------------------------------------------
#### Define user-specific paths -----------------------------------------------------------------
username <- Sys.info()["user"]

# *Store all user-specific settings in a list
user_settings <- list(
    Nikil = list(
        box = "C:/Users/Admin/Box/2. Projects/11. PR&DW",
        github = "C:/Users/Admin/OneDrive - University of Chicago IIC/Desktop/gramodaya"
    ),
    Admin = list( # Admin refers to aastha
        box = "C:/Users/Admin/Box/11. PR&DW",
        github = "C:/Users/Admin/OneDrive - University of Chicago IIC/Desktop/gramodaya"
    )
    # Add other users here if needed
)

if (!username %in% names(user_settings)) {
    stop(paste("Username '", username, "' not recognized in user_settings. Please add your paths.", sep = ""))
}

# *Assign paths based on current user
box_path <- user_settings[[username]]$box
github_path <- user_settings[[username]]$github

##### MASTER FILE PATHS ------------------------------------------------------------------
code_path <- file.path(github_path, "Code")
raw_data_path <- file.path(box_path, "7. Raw Data - Working/gramodaya")
process_data_path <- file.path(box_path, "8. Analysis/gramodaya")
data_temp_path <- file.path(process_data_path, "Temp")
data_cleaned_path <- file.path(box_path, "9. Cleaned Data/gramodaya")

### 0.2 Boilerplate ------------------------------------------------------------------
# *ERASE TEMP
# *Ensure the temp directory exists, create if not
if (!dir.exists(data_temp_path)) {
    dir.create(data_temp_path, recursive = TRUE, showWarnings = FALSE)
    message(paste("Created temp directory:", data_temp_path))
} else {
    message(paste("Temp directory exists:", data_temp_path))
}

temp_files_to_delete <- list.files(data_temp_path, full.names = TRUE, all.files = TRUE, no.. = TRUE)

if (length(temp_files_to_delete) > 0) {
    removed_status <- file.remove(temp_files_to_delete)
    message(paste("Attempted to remove", length(temp_files_to_delete), "files from temp. Success for files where status is TRUE:", paste(removed_status, collapse = ", ")))
} else {
    message("Temp directory is empty. No files to remove.")
}

# *Load necessary packages
packages <- c("data.table", "haven", "languageserver", "dplyr", "readr", "stringr", "purrr", "janitor")
lapply(packages, function(pkg) {
    if (!require(pkg, character.only = TRUE)) {
        install.packages(pkg, dependencies = TRUE)
        library(pkg, character.only = TRUE)
    } else {
        library(pkg, character.only = TRUE) # Ensure it's loaded even if already installed
    }
})

# Load HH_wise data------------------------------------------------------------------

hh_wise <- file.path(raw_data_path, "HH Wise Data 29-4-25")

library(data.table)

read_csvs_to_hh_objects <- function(directory_path) {
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
            paste0("hh_", .)

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
load_hh <- read_csvs_to_hh_objects(directory_path = hh_wise)

# Get common columns
common_columns <- mget(ls(pattern = "^hh_"), .GlobalEnv) %>%
    Filter(is.data.frame, .) %>%
    {
        if (length(.) > 1) reduce(map(., names), intersect) else character(0)
    }

# Keep only valid data.tables
hh_ <- mget(ls(pattern = "^hh_"), .GlobalEnv) %>%
    purrr::keep(~ is.data.frame(.x))

# Ensure one-to-one join by checking uniqueness
merge_one_to_one <- function(dt1, dt2, by_cols) {
    # Check uniqueness of keys
    key1 <- dt1[, .N, by = by_cols][N > 1]
    key2 <- dt2[, .N, by = by_cols][N > 1]

    if (nrow(key1) > 0 | nrow(key2) > 0) {
        keys_to_keep <- fsetdiff(
            dt1[, ..by_cols],
            rbindlist(list(key1[, ..by_cols], key2[, ..by_cols]), use.names = TRUE)
        )
        dt1 <- dt1[keys_to_keep, on = by_cols, nomatch = 0]
        dt2 <- dt2[keys_to_keep, on = by_cols, nomatch = 0]
    }

    merge(dt1, dt2, by = by_cols, all = FALSE)
}

# Merge all data.tables strictly one-to-one
if (length(hh_) == 0) {
    merged_hh_clean <- data.table()
} else {
    merged_hh_clean <- hh_[[1]]
    for (i in 2:length(hh_)) {
        merged_hh_clean <- merge_one_to_one(merged_hh_clean, hh_[[i]], common_columns)
    }
}

rm(list = ls(pattern = "^hh_"))

merged_hh_clean <- merged_hh_clean %>%
    rename_with(tolower) %>%
    janitor::clean_names(case = "snake") %>%
    mutate(across(everything(), ~ gsub("\\\\N", "", as.character(.))))
