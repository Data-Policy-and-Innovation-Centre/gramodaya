# Function to get current script directory
get_current_script_dir <- function() {
  initial_wd <- getwd()
  
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    tryCatch({
      path <- rstudioapi::getActiveDocumentContext()$path
      if (!is.null(path) && path != "") {
        message("Detected script path using rstudioapi.")
        return(dirname(path))
      }
    }, error = function(e) {})
  }
  
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
  
  warning("Could not determine the script file path. Using working directory.")
  return(initial_wd)
}

# --- Source setup file ---
current_script_dir <- get_current_script_dir()
file_to_source <- file.path(current_script_dir, "0_setup_r.r")

if (file.exists(file_to_source)) {
  message(paste("Sourcing setup file:", file_to_source))
  source(file_to_source)
} else {
  stop(paste("Required setup file not found:", file_to_source))
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
