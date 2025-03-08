import polars as pl
import os
import hashlib
import random
from config import paths  # Assuming paths are defined in config.py

# Define base paths for raw, clean, and processed data
raw_data_path = os.path.join(paths["data_path"], "Gramodaya/raw_data")
clean_data_path = os.path.join(paths["data_path"], "Gramodaya/clean_data")
process_data_path = os.path.join(paths["data_path"], "Gramodaya/process_data")


def ensure_directory_exists(directory):
    """Ensure that a directory exists, creating it if necessary."""
    os.makedirs(directory, exist_ok=True)


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

# Ensure necessary directories exist
ensure_directory_exists(clean_data_path)
ensure_directory_exists(process_data_path)

# Hash PII columns in the raw data
pii_columns = [
    "ben_name",
    "father_husband_name",
    "mobile_no",
    "aadhar_no",
    "ration_card_no",
]
ind_benefits_raw = hash_pii(ind_benefits_raw, pii_columns)
log_message("Hashed PII columns.", log_file_path)

# Identify and report duplicate rows
duplicate_rows = ind_benefits_raw.filter(ind_benefits_raw.is_duplicated())
if duplicate_rows.height > 0:
    duplicate_ben_id_report_path = os.path.join(
        process_data_path, "duplicate_ben_id_report.csv"
    )
    duplicate_rows.select("ben_id").unique().write_csv(duplicate_ben_id_report_path)
    log_message(
        f"Identified {duplicate_rows.height} duplicate rows and saved report at {duplicate_ben_id_report_path}.",
        log_file_path,
    )

# Remove completely duplicate rows
ind_benefits_raw = ind_benefits_raw.unique()
log_message("Removed completely duplicate rows from raw data.", log_file_path)

# Ensure split_data directory exists before saving department-specific data
split_data_path = os.path.join(clean_data_path, "split_data")
ensure_directory_exists(split_data_path)

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
    """Split data by department and save to CSV files."""
    missing_departments_df = df.filter(pl.col(dept_name_col).is_null())
    df = df.drop_nulls(dept_name_col)
    department_subsets = {}

    for dept, abbr_list in dept_abbr_map.items():
        dept_df = df.filter(pl.col(dept_name_col) == dept).drop(dept_name_col)
        if dept_df.height > 0:
            if dept == "ST & SC Dev Department":
                # Include both scst_ and scste_ columns
                dept_specific_cols = [
                    col
                    for col in dept_df.columns
                    if col.startswith("scst_") or col.startswith("scste_")
                ]
                chosen_abbr = "scst"  # Save as scst_benefits.csv
            else:
                chosen_abbr = random.choice(abbr_list)
                dept_specific_cols = [
                    col for col in dept_df.columns if col.startswith(chosen_abbr + "_")
                ]

            relevant_cols = [
                col
                for col in common_cols + dept_specific_cols
                if col in dept_df.columns
            ]
            dept_df = dept_df.select(relevant_cols).unique()
            output_file = os.path.join(output_path, f"{chosen_abbr}_benefits.csv")
            dept_df.write_csv(output_file)
            log_message(
                f"Saved {dept_df.height} rows for {dept} at {output_file}.",
                log_file_path,
            )

    if missing_departments_df.height > 0:
        missing_file = os.path.join(clean_data_path, "missing_departments.csv")
        missing_departments_df.write_csv(missing_file)
        log_message(
            f"Saved {missing_departments_df.height} rows with missing department info at {missing_file}.",
            log_file_path,
        )

    return department_subsets


def check_ben_id_uniqueness(datasets, log_file_path):
    """Check uniqueness of ben_id in each department dataset."""
    for dept, df in datasets.items():
        if "ben_id" in df.columns:
            duplicates = df.filter(pl.col("ben_id").is_duplicated())
            if duplicates.height > 0:
                duplicate_report_path = os.path.join(
                    process_data_path, f"{dept}_duplicate_report.csv"
                )
                duplicates.write_csv(duplicate_report_path)
                log_message(
                    f"Found {duplicates.height} duplicate ben_id in {dept} dataset. Report saved at {duplicate_report_path}.",
                    log_file_path,
                )
            else:
                log_message(f"All ben_id are unique in {dept} dataset.", log_file_path)


# Prepare for department-specific data processing
dept_prefixes = {abbr for abbr_list in dept_abbr.values() for abbr in abbr_list}
common_columns = [
    col
    for col in ind_benefits_raw.columns
    if not any(col.startswith(abbr + "_") for abbr in dept_prefixes)
]

# Split data by department and check ben_id uniqueness
datasets = split_and_save_by_department(
    ind_benefits_raw,
    "dept_name",
    dept_abbr,
    split_data_path,
    common_columns,
    log_file_path,
)
check_ben_id_uniqueness(datasets, log_file_path)
log_message("Data processing complete.", log_file_path)
