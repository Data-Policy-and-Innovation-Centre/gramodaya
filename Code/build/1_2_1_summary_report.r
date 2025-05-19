# Falls back to the current working directory if the script path can't be determined.
get_current_script_dir <- function() {
  initial_wd <- getwd()

  # 1. Try RStudio API
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    tryCatch(
      {
        path <- rstudioapi::getActiveDocumentContext()$path
        if (!is.null(path) && path != "") {
          message("Detected script path using rstudioapi.")
          return(dirname(path))
        }
      },
      error = function(e) {}
    )
  }

  # 2. Try parent frames (works well when the script is 'sourced')
  for (i in sys.nframe():1) {
    frame <- sys.frame(i)
    if (!is.null(frame$fileName) && frame$fileName != "") {
      script_path <- frame$fileName
      if (!grepl("^(/|[A-Za-z]:/)", script_path) && !file.exists(script_path)) {
        script_path <- file.path(initial_wd, script_path)
      }
      if (file.exists(script_path)) {
        message("Detected script path using parent frames.")
        return(dirname(normalizePath(script_path)))
      }
    }
  }

  # 3. Try commandArgs (works for Rscript and R --file)
  args <- commandArgs(trailingOnly = FALSE)
  file_arg_match <- grep("--file=", args)
  if (length(file_arg_match) > 0) {
    script_path <- substring(args[file_arg_match[1]], 8)
    if (file.exists(script_path)) {
      message("Detected script path using commandArgs('--file=').")
      return(dirname(normalizePath(script_path)))
    }
  }
  if (length(args) >= 2) {
    script_path <- args[2]
    if (!grepl("^(/|[A-Za-z]:/)", script_path) && !file.exists(script_path)) {
      script_path_relative <- file.path(initial_wd, script_path)
      if (file.exists(script_path_relative)) {
        message("Detected script path using commandArgs (relative to initial WD).")
        return(dirname(normalizePath(script_path_relative)))
      }
    } else if (file.exists(script_path)) {
      message("Detected script path using commandArgs (absolute or direct).")
      return(dirname(normalizePath(script_path)))
    }
  }

  # 4. Fallback: Current working directory
  warning("Could not determine the script file path precisely. Falling back to the initial working directory.")
  return(initial_wd)
}

# 1. Get the directory of the *current* script (your_script.R)
current_script_dir <- get_current_script_dir()
message(paste("Current script directory detected as:", current_script_dir))

# 2. Navigate from the current script's directory up one level (to the parent folder)
#    and then down into the 'global_setups' folder.
#    file.path handles the '/' or '\' separators correctly across operating systems.
#    '..' is the standard way to represent the parent directory.
global_setups_dir <- file.path(current_script_dir, "..", "global_setups")
message(paste("Attempting to locate global_setups directory at:", global_setups_dir))

# 3. Construct the full path to the setup file
setup_file_path <- file.path(global_setups_dir, "0_setup_r.r")
message(paste("Attempting to source file:", setup_file_path))

# 4. Check if the file exists and source it
if (file.exists(setup_file_path)) {
  message("File found. Sourcing...")
  # Use source() to execute the file
  source(setup_file_path, chdir = FALSE) # Set chdir=TRUE if needed by 0_setup_r.r
  message("Sourcing complete.")
} else {
  # Handle the case where the setup file is not found
  warning(paste("Setup file not found at:", setup_file_path))
}

# --- Load data ---
df <- read_dta(file.path(box_path, "9. Cleaned Data", "gramodaya_community_data.dta"))

# --- Extract variable names and labels ---
var_labels <- attributes(df)$variable.labels

# Create a data frame with variable names and their labels
label_df <- data.frame(
  variable_name = names(var_labels),
  label = var_labels,
  stringsAsFactors = FALSE
)

# Print the variable names and labels to the console
print(label_df)

# --- Data cleaning ---
df <- df %>%
  rename_with(tolower) %>%
  janitor::clean_names(case = "snake") %>%
  mutate(across(everything(), ~ {
    x <- as.character(.)
    x[x == "\\n"] <- NA
    x
  }))

# --- Generate and save summary report ---
sum_table <- dfSummary(df)
view(sum_table, file = file.path(output_path, "summary_report.html"))
