# R setup for gramodaya data

## 0. Setup & Boilerplate -----------------------------------------------------------------
rm(list = ls(all.names = TRUE)) # Clears all objects from the environment
gc()

### 0.1 Directories -----------------------------------------------------------------
#### Define user-specific paths -----------------------------------------------------------------
username <- Sys.info()["user"]

# *Store all user-specific settings in a list
user_settings <- list(
    nikileshanusha = list(
        box = "/Users/nikileshanusha/Library/CloudStorage/Box-Box/2. Projects/11. PR&DW",
        github = "/Users/nikileshanusha/repositories/gramodaya"
    ),
    Admin = list( # Admin refers to aastha
        box = "C:/Users/Admin/Box/2. Projects/11. PR&DW",
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
raw_data_path <- file.path(box_path, "7. Raw Data - Working/gramodaya/gramoday data on 06.06.2025")
process_data_path <- file.path(box_path, "8. Analysis/gramodaya")
data_temp_path <- file.path(process_data_path, "Temp")
data_cleaned_path <- file.path(box_path, "9. Cleaned Data/gramodaya")
output_path <- file.path(box_path, "10. Output")


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

# Note: XQuartz is required for macOS. install XQuartz from https://www.xquartz.org/
# *Load necessary packages
packages <- c(
    "data.table", "haven", "languageserver", "dplyr", "readr", "stringr", "purrr", "janitor", "digest",
    "summarytools" # add more packages as needed
)

lapply(packages, function(pkg) {
    if (!require(pkg, character.only = TRUE)) {
        install.packages(pkg, dependencies = TRUE)
        library(pkg, character.only = TRUE)
    } else {
        library(pkg, character.only = TRUE) # Ensure it's loaded even if already installed
    }
})
