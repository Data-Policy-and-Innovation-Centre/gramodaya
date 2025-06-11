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

community_path <- "C:/Users/Admin/Box/2. Projects/11. PR&DW/7. Raw Data - Working/gramodaya/gramoday data on 06.06.2025/Community Data Village wise -29-4-25"


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
final [final == ""] <- NA

# ---- Clean and Rename Columns -----------------------------------------------
final <- final %>%
    rename(
        q15_AGRI_solar_pumps = `No..of.Functional.Solar.Pumps.available.for.farmers.in.the.village`,
        q44_TRANS_bus_type = `If.yes.x`,
        q39_IT_network_provider = `If.Yes`,
        q8b_PRDW_road_belongs_to = `the.Road.Belongs.to`,
        q21_HLTH_health_workers_sanctioned = `Health.workers.position.in.the.Health.sub.centres_Sanctioned`,
        q22_HLTH_health_workers_present = `Health.workers.position.in.the.Health.sub.centres_Present.position`,
        q18_EDU_teachers_sanctioned = `Teachers.Position.Sanction`,
        q19_EDU_teachers_present = `Teachers.Present.position`,
        q5_PRDW_pipe_water_supply = `Number.of.Household.provided.pipe.water.supply`,
        q8_PRDW_allweather_connectivity = `If.yes.y`,
        q1_PRDW_community_centre = `Community.Centre.available`,
        q2_PRDW_shg_workshed = `Common.work.shed.for.Self.Help.Group..SHG..available`,
        q3_PRDW_tubewell_count = `Number.of.functional.Tube.well`,
        q6_PRDW_playground = `Availability.of.Play.Ground.in.the.village`,
        q7_PRDW_solar_streetlights = `Coverage.of.functional.Solar.Street.Lights.in.the.village`,
        q9_PRDW_concrete_road_length = `Availability.of.Concrete.Roads.Length..in.meter.`,
        q10_PRDW_concrete_road_requirement = `Requirement.of.Concrete.Roads.Length..in.meter.`,
        q11_FS_fair_price_shop = `Availability.of.Fair.Price.Shop.in.the.village`,
        q12_AGRI_micro_river_lift = `Availability.of.Micro.River.Lift.Irrigation.Points.in.the.village`,
        q13_AGRI_seed_centre = `Available.of.Seed.Distribution.Centre.in.the.village`,
        q14_AGRI_agri_impl_centre = `Availability.of.agriculture.implement.distribution.centre`,
        q16_EDU_school_toilets = `Number.of.functional.Toilet_1`,
        q17_EDU_school_kitchen = `Whether.there.is.Kitchen.facility_1`,
        q20_HLTH_sub_centre = `Whether.Health.Sub.Centres.available.in.the.village`,
        q23_HLTH_health_camps = `Whether.Regular.Health.Check.up.Camps.organised`,
        q24_HLTH_ambulance = `Availability.of.Ambulance.Service.facility.in.the.village`,
        q25_HLTH_mobile_unit = `Availability.of.Mobile.Health.Units.in.the.village`,
        q26_HLTH_asha_worker = `Availability.of.ASHA.worker.within.the.habitation`,
        q27_ENRG_village_electrified = `Electrified_1`,
        q28_STSC_residential_school = `Availability.of.Residential.School.in.the.GP`,
        q29_STSC_students_enrolled = `Number.of.students.enrolled.in.residential.school`,
        q31_WCD_aw_workers_sanctioned = `Man.in.Position.Anganwadi.Workers`,
        q33_WCD_aw_helpers_sanctioned = `Man.in.Position.Anganwadi.Helpers`,
        q36_FIN_banking_correspondent = `Availability.of.Banking.correspondent`,
        q37_FIN_micro_atm = `Availability.of.Micro.ATM.in.the.GP.Village`,
        q40_IT_common_service_centre = `Availability.of.Common.Service.centre.in.the.village`,
        q41_IT_mo_seva_kendra = `Availability.of.Mo.Seva.Kendra.in.the.village`,
        q42_SPRT_sports_equipment = `Supply.of.Sports.equipment.by.Government.to.the.village`,
        q43_TRANS_bus_to_block = `Availability.of.Bus.facility.to.the.Block.Headquarters`,
        q45_WR_deep_borewell = `Availability.of.Deep.Bore.well.in.the.farmer.s.field.in.the.village`,
        q46_ENV_allweather_forest_road = `Availability.of.all.weather.connectivity.within.forest.connecting.habitation.and.village`,
        q47_HE_agniveer_coaching = `Is.there.any.Coaching.Camp.for.Agniveer`
    ) %>%
    select(-`type.of.bus.facility`, -`then.specify.the.Bank.Name`, -`Paramilitary.and.Police.Service`, -`then.provide.company.name`, -`then.specify.Bank.Name`, -`the.Road.Belongs.to`) %>%
    mutate(across(c(
        q15_AGRI_solar_pumps,
        q21_HLTH_health_workers_sanctioned,
        q22_HLTH_health_workers_present,
        q18_EDU_teachers_sanctioned,
        q19_EDU_teachers_present,
        q31_WCD_aw_workers_sanctioned,
        q33_WCD_aw_helpers_sanctioned,
        q5_PRDW_pipe_water_supply
    ), ~ suppressWarnings(as.numeric(as.character(.)))))


write.csv(final, "C:/Users/Admin/Box/2. Projects/11. PR&DW/9. Cleaned Data/gramodaya/community_cleaned_excel.csv")

