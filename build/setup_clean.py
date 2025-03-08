"""
This script is responsible for setting up and cleaning the data for the Gramodaya project.

It performs the following tasks:

1. Defines the base paths for raw, clean, and processed data.
2. Ensures that the necessary directories exist and creates them if they don't.
3. Sets up a log file to track the data processing progress.
4. Loads raw data from a CSV file and performs initial processing.
5. Splits the data by department and saves each department's data to a separate CSV file.
6. Checks for duplicate ben_id values in each department's dataset and saves a report if any duplicates are found.

The script uses the Polars library for data manipulation and the hashlib library for hashing personally identifiable information (PII).

Author: Nikilesh Anusha

"""

import polars as pl
import os
import hashlib
import random
from config import paths

# Define base paths for raw, clean, and processed data
raw_data_path = os.path.join(paths["data_path"], "Gramodaya/raw_data")
clean_data_path = os.path.join(paths["data_path"], "Gramodaya/clean_data")
process_data_path = os.path.join(paths["data_path"], "Gramodaya/process_data")


def ensure_directory_exists(directory):
    """Ensure that a directory exists, creating it if necessary."""
    os.makedirs(directory, exist_ok=True)


# Ensure necessary directories exist
ensure_directory_exists(clean_data_path)
ensure_directory_exists(process_data_path)


def log_message(message, log_file_path):
    """Log a message to a specified file and print it."""
    formatted_message = f"> {message}"
    with open(log_file_path, "a") as log_file:
        log_file.write(formatted_message + "\n")
    print(formatted_message)


# Set up log file path and initialize log
log_file_path = os.path.join(process_data_path, "processing_log.txt")

if os.path.exists(log_file_path):
    os.remove(log_file_path)  # Remove existing log file if it exists
log_message(f"Starting data processing... Log file: {log_file_path}", log_file_path)

# Load raw data from CSV file
raw_data_file = os.path.join(raw_data_path, "Indivisual Benefits.csv")
ind_benefits_raw = pl.read_csv(
    raw_data_file,
    null_values="\\N",
    infer_schema_length=1000,
)
log_message(
    f"Loaded raw data with {ind_benefits_raw.height} rows from {raw_data_file}.",
    log_file_path,
)

# Hash PII columns in the raw data
pii_columns = [
    "ben_name",
    "father_husband_name",
    "mobile_no",
    "aadhar_no",
    "ration_card_no",
]


def hash_pii(df, columns):
    """Hash personally identifiable information (PII) in specified columns."""

    def hash_value(x):
        return hashlib.sha256(x.encode()).hexdigest() if x is not None else None

    return df.with_columns(
        [
            pl.col(col).cast(pl.Utf8).fill_null("").map_elements(hash_value).alias(col)
            for col in columns
            if col in df.columns
        ]
    )


ind_benefits_raw = hash_pii(ind_benefits_raw, pii_columns)
log_message("Hashed PII columns.", log_file_path)

# Identify and report duplicate rows with counts
duplicate_groups = (
    ind_benefits_raw.group_by(pl.all())
    .agg(pl.count().alias("occurrences"))
    .filter(pl.col("occurrences") > 1)
)

if duplicate_groups.height > 0:
    duplicate_ben_id_report_path = os.path.join(
        process_data_path, "duplicate_rows_report.csv"
    )
    duplicate_groups.write_csv(duplicate_ben_id_report_path)
    total_duplicate_entries = (
        duplicate_groups["occurrences"].sum() - duplicate_groups.height
    )
    log_message(
        f"Found {duplicate_groups.height} unique duplicated rows ({total_duplicate_entries} total duplicates). "
        f"Report saved at {duplicate_ben_id_report_path}.",
        log_file_path,
    )

# Record original count before deduplication
original_count = ind_benefits_raw.height

# Remove duplicate rows, keeping only the first occurrence
ind_benefits_raw = ind_benefits_raw.unique(subset=None, keep="first")
rows_removed = original_count - ind_benefits_raw.height

log_message(
    f"Removed {rows_removed} duplicate rows. "
    f"Dataset reduced from {original_count} to {ind_benefits_raw.height} rows.",
    log_file_path,
)

# Ensure split_data directory exists before saving department-specific data
split_data_path = os.path.join(clean_data_path, "split_data")
ensure_directory_exists(split_data_path)

# Prepare for department-specific data processing
# Department abbreviation mapping
dept_abbr = {
    "Agriculture & FE Department": ["agri"],
    "Cooperation": ["cooperation"],
    "E & IT /Telecom": ["eit"],
    "Energy Department": ["energy"],
    "Finance Department": ["finance"],
    "Food Supplies & Consumer welfare Department": ["food"],
    "Forest & Environment": ["forest"],
    "Handloom, Textile & Handicraft": ["handloom"],
    "Health and Family Welfare": ["health"],
    "Higher Education": ["higheredu"],
    "Labour & ESI": ["labour"],
    "Mission Shakti": ["missionshakti"],
    "MSME": ["msmy"],
    "Odia language, Literature & Culture": ["odia"],
    "Panchayati Raj & Drinking Water Department": ["prdw"],
    "Revenue & DM Department": ["revenue"],
    "School & Mass Education": ["sme"],
    "Skill Development & Technical Education": ["skill"],
    "SSEPD": ["ssepd"],
    "ST & SC Dev Department": ["scst", "scste"],
    "W & CD Department": ["wcd"],
}


def split_and_save_by_department(
    df, dept_name_col, dept_abbr_map, output_path, common_cols, log_file_path
):
    """Split data by department and save to CSV files, returning saved paths."""
    department_files = {}
    """Split data by department and save to CSV files."""
    # Filter and save missing department entries first
    missing_departments_df = df.filter(pl.col(dept_name_col).is_null())

    if missing_departments_df.height > 0:
        missing_dept_path = os.path.join(clean_data_path, "missing_departments.csv")
        missing_departments_df.write_csv(missing_dept_path)
        log_message(
            f"Saved {missing_departments_df.height} rows with missing department info to {missing_dept_path}",
            log_file_path,
        )
    else:
        log_message("No rows with missing department information found", log_file_path)

    for dept, abbr_list in dept_abbr_map.items():
        dept_df = df.filter(pl.col(dept_name_col) == dept).drop(dept_name_col)
        if dept_df.height > 0:
            chosen_abbr = (
                "scst" if dept == "ST & SC Dev Department" else random.choice(abbr_list)
            )
            output_file = os.path.join(output_path, f"{chosen_abbr}_benefits.csv")

            # Select appropriate columns
            prefix = chosen_abbr + "_"
            dept_specific_cols = [
                col for col in dept_df.columns if col.startswith(prefix)
            ]
            relevant_cols = [
                col
                for col in common_cols + dept_specific_cols
                if col in dept_df.columns
            ]

            dept_df.select(relevant_cols).unique().write_csv(output_file)
            department_files[chosen_abbr] = output_file
            log_message(f"Saved {dept} data to {output_file}", log_file_path)

    return department_files  # Return mapping of abbreviations to file paths


dept_prefixes = {abbr for abbr_list in dept_abbr.values() for abbr in abbr_list}
common_columns = [
    col
    for col in ind_benefits_raw.columns
    if not any(col.startswith(f"{abbr}_") for abbr in dept_prefixes)
]

log_message(
    f"Identified common columns across all departments: {common_columns}", log_file_path
)

# Split data and get file paths
department_files = split_and_save_by_department(
    ind_benefits_raw,
    "dept_name",
    dept_abbr,
    split_data_path,
    common_columns,
    log_file_path,
)


def check_ben_id_uniqueness(department_files, log_file_path):
    """Check uniqueness of ben_id in each department's saved CSV file."""
    duplicate_reports = []

    for abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)

        if "ben_id" not in df.columns:
            log_message(f"No ben_id column in {abbr} data", log_file_path)
            continue

        duplicates = (
            df.group_by("ben_id")
            .agg(pl.count().alias("occurrences"))
            .filter(pl.col("occurrences") > 1)
        )

        if duplicates.height > 0:
            log_message(
                f"Found {duplicates.height} duplicate ben_ids in {abbr} data",
                log_file_path,
            )
            duplicate_reports.extend(
                [
                    {
                        "department": abbr,
                        "ben_id": row["ben_id"],
                        "occurrences": row["occurrences"],
                    }
                    for row in duplicates.iter_rows(named=True)
                ]
            )

    if duplicate_reports:
        report_df = pl.DataFrame(duplicate_reports)
        report_path = os.path.join(process_data_path, "duplicate_ben_id_report.csv")
        report_df.write_csv(report_path)
        log_message(
            f"Saved duplicate ben_id report with {len(duplicate_reports)} entries at {report_path}",
            log_file_path,
        )
    else:
        log_message("All ben_ids are unique across department datasets", log_file_path)


# Check uniqueness using the actual saved files
check_ben_id_uniqueness(department_files, log_file_path)
