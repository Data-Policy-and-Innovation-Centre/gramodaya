# =============================================================================
# Title     : 1_1_community_cleaning.r
# Author    : Nikilesh Anusha
# Date      : 2025-06-06
# Purpose   : Reads CSVs from a folder, assigns objects, detects long-format
#             data frames, reshapes them to wide format, merges with other
#             data frames on shared keys, and produces a consolidated data frame.
# =============================================================================

# ---- Setup ------------------------------------------------------------------

get_current_script_dir <- function() {
    initial_wd <- getwd()

    if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
        tryCatch(
            {
                path <- rstudioapi::getActiveDocumentContext()$path
                if (!is.null(path) && path != "") {
                    return(dirname(path))
                }
            },
            error = function(e) {}
        )
    }

    for (i in sys.nframe():1) {
        frame <- sys.frame(i)
        if (!is.null(frame$fileName) && frame$fileName != "") {
            script_path <- frame$fileName
            if (!grepl("^(/|[A-Za-z]:/)", script_path) && !file.exists(script_path)) {
                script_path <- file.path(initial_wd, script_path)
            }
            if (file.exists(script_path)) {
                return(dirname(normalizePath(script_path)))
            }
        }
    }

    args <- commandArgs(trailingOnly = FALSE)
    file_arg_match <- grep("--file=", args)
    if (length(file_arg_match) > 0) {
        script_path <- substring(args[file_arg_match[1]], 8)
        if (file.exists(script_path)) {
            return(dirname(normalizePath(script_path)))
        }
    }

    if (length(args) >= 2) {
        script_path <- args[2]
        if (!grepl("^(/|[A-Za-z]:/)", script_path) && !file.exists(script_path)) {
            script_path <- file.path(initial_wd, script_path)
        }
        if (file.exists(script_path)) {
            return(dirname(normalizePath(script_path)))
        }
    }

    warning("Script path not detected; using working directory.")
    return(initial_wd)
}

current_script_dir <- get_current_script_dir()
global_setups_dir <- file.path(current_script_dir, "..", "global_setups")
setup_file_path <- file.path(global_setups_dir, "0_setup_r.r")

if (file.exists(setup_file_path)) {
    source(setup_file_path, chdir = FALSE)
} else {
    warning(paste("Setup file not found at:", setup_file_path))
}

community_path <- file.path(raw_data_path, "gramoday data on 06.06.2025", "Community Data Village wise -29-4-25")

# ---- CSV Reader -------------------------------------------------------------

read_csv_files_as_objects <- function(folder_path) {
    folder_path <- normalizePath(folder_path, mustWork = TRUE)
    csv_files <- list.files(path = folder_path, pattern = "\\.csv$", full.names = TRUE)

    for (file in csv_files) {
        base_name <- tools::file_path_sans_ext(basename(file))
        clean_name <- gsub("[^a-zA-Z0-9_]", "_", tolower(base_name))
        clean_name <- gsub("_+", "_", clean_name)
        clean_name <- gsub("^_|_$", "", clean_name)
        obj_name <- make.names(clean_name)

        assign(obj_name, read.csv(file, stringsAsFactors = FALSE), envir = .GlobalEnv)
        message(sprintf("Loaded: %s -> %s", file, obj_name))
    }
}

read_csv_files_as_objects(community_path)
rm(user_settings)

# ---- Helper Functions -------------------------------------------------------

find_common_columns <- function() {
    all_objects <- mget(ls(envir = .GlobalEnv), envir = .GlobalEnv)
    df_list <- Filter(is.data.frame, all_objects)
    if (length(df_list) < 2) {
        return(NULL)
    }
    column_lists <- lapply(df_list, names)
    Reduce(intersect, column_lists)
}

is_long_format <- function(df, key_cols) {
    any(duplicated(df[key_cols]))
}

pivot_long_df_to_wide <- function(df, key_cols) {
    pivot_cols <- setdiff(names(df), key_cols)

    df <- df %>%
        group_by(across(all_of(key_cols))) %>%
        mutate(entry_id = row_number()) %>%
        ungroup()

    df_wide <- df %>%
        pivot_wider(
            id_cols = all_of(key_cols),
            names_from = entry_id,
            values_from = all_of(pivot_cols),
            names_glue = "{.value}_{entry_id}"
        )

    return(df_wide)
}

# ---- Main Merge Function ----------------------------------------------------

merge_long_and_normal <- function() {
    library(dplyr)
    library(tidyr)

    all_objects <- mget(ls(envir = .GlobalEnv), envir = .GlobalEnv)
    df_list <- Filter(is.data.frame, all_objects)

    key_cols <- find_common_columns()
    if (is.null(key_cols)) stop("No common columns found.")

    long_dfs <- Filter(function(df) is_long_format(df, key_cols), df_list)
    normal_dfs <- Filter(function(df) !is_long_format(df, key_cols), df_list)

    # Merge all normal (already wide) data frames
    merged_normal <- Reduce(function(x, y) {
        merge(x, y, by = key_cols, all = TRUE)
    }, normal_dfs)

    # Reshape and merge long-format data frames
    if (length(long_dfs) > 0) {
        long_wide_list <- lapply(long_dfs, function(df) {
            pivot_long_df_to_wide(df, key_cols)
        })

        merged_long <- Reduce(function(x, y) {
            merge(x, y, by = key_cols, all = TRUE)
        }, long_wide_list)

        final_df <- merge(merged_normal, merged_long, by = key_cols, all = TRUE)
    } else {
        final_df <- merged_normal
    }

    return(final_df)
}

# ---- Run Merge --------------------------------------------------------------

common_cols <- find_common_columns()
print(common_cols)

final <- merge_long_and_normal()
final[final == "\\N"] <- NA
final[final == "--select--"] <- NA

write.csv(final, "C:/Users/Admin/Box/11. PR&DW/9. Cleaned Data/gramodaya/community_cleaned.csv", row.names = FALSE)

source("$code/build/community_summary.r") 