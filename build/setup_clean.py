"""
Gramodaya Data Processing Pipeline

Core Processes:
1. Infrastructure: Directory setup, audit logging, path configuration
2. Security: SHA-256 PII hashing (names, contacts, IDs) with null preservation
3. Quality: Multi-stage duplicate removal, ben_id validation, missing data handling
4. Processing: Department splitting (namespace management), dataset merging (column coalescing)
5. Outputs: Standardized CSVs (split/merged), diagnostic reports, processing metadata

Key Features:
- Polars-based high-performance transformations
- Deterministic data handling
- Modular pipeline components
- Conflict-resistant column namespaces
- Multi-layer data validation

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


def hash_pii(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Hash PII columns while preserving nulls and empty strings."""

    def hash_value(x: str) -> str | None:
        if not x:  # Covers both None and empty strings
            return x
        return hashlib.sha256(x.encode()).hexdigest()

    return df.with_columns(
        [
            pl.col(col)
            .cast(pl.Utf8)
            .map_elements(hash_value, return_dtype=pl.Utf8)
            .alias(col)
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


def remove_duplicate_ben_ids(department_files, process_data_path, log_file_path):
    """Return cleaned DataFrames with duplicate ben_ids removed."""
    report_path = os.path.join(process_data_path, "duplicate_ben_id_report.csv")
    cleaned_dfs = {}

    if not os.path.exists(report_path):
        log_message("No duplicate report found", log_file_path)
        return cleaned_dfs

    try:
        duplicate_report = pl.read_csv(report_path)
    except Exception as e:
        log_message(f"Failed reading report: {str(e)}", log_file_path)
        return cleaned_dfs

    # Create reverse department mapping
    reverse_dept_mapping = {}
    for full_name, abbr_list in dept_abbr.items():
        for abbr in abbr_list:
            reverse_dept_mapping[full_name.lower()] = abbr
            reverse_dept_mapping[abbr.lower()] = abbr

    # Group report by department and collect ben_ids
    dept_ben_ids = {}
    for row in duplicate_report.iter_rows(named=True):
        raw_dept_name = row["department"]
        ben_id = row["ben_id"]

        normalized_dept = raw_dept_name.strip().lower()
        matched_abbr = reverse_dept_mapping.get(normalized_dept)
        if not matched_abbr:
            log_message(f"No match for department '{raw_dept_name}'", log_file_path)
            continue
        if matched_abbr not in dept_ben_ids:
            dept_ben_ids[matched_abbr] = set()
        dept_ben_ids[matched_abbr].add(ben_id)

    # Process each department with duplicates
    for abbr, ben_ids in dept_ben_ids.items():
        if abbr not in department_files:
            log_message(f"No file for abbreviation '{abbr}'", log_file_path)
            continue

        file_path = department_files[abbr]
        try:
            df = pl.read_csv(file_path)
            original_count = df.height
            cleaned_df = df.filter(~pl.col("ben_id").is_in(ben_ids))
            removed_count = original_count - cleaned_df.height

            if removed_count > 0:
                cleaned_dfs[abbr] = cleaned_df
                log_message(
                    f"Removed {removed_count} entries of ben_ids from {abbr}",
                    log_file_path,
                )
        except Exception as e:
            log_message(f"Failed processing {file_path}: {str(e)}", log_file_path)

    return cleaned_dfs


# Process duplicates and save cleaned data
cleaned_dfs = remove_duplicate_ben_ids(
    department_files, process_data_path, log_file_path
)
for abbr, df in cleaned_dfs.items():
    file_path = department_files[abbr]
    df.write_csv(file_path)
    log_message(f"Saved cleaned data for {abbr} to {file_path}", log_file_path)


def merge_department_data(department_files, log_file_path):
    """Merge department datasets while preserving column order."""
    # 1. Identify common columns across ALL departments
    all_columns = []
    for dept_abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)
        all_columns.append(set(df.columns))

    common_columns = set.intersection(*[set(cols) for cols in all_columns]) - {"ben_id"}
    common_columns = sorted(common_columns)
    log_message(f"Common columns identified: {common_columns}", log_file_path)

    # 2. Initialize merged dataframe and column order tracker
    merged_df = None
    column_order = ["ben_id"] + list(common_columns)

    # 3. Process departments in given order
    for dept_abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)

        # Create department-specific column list with prefixes
        dept_specific = [
            f"{dept_abbr}_{col}"
            for col in df.columns
            if col not in common_columns and col != "ben_id"
        ]

        # Rename columns and maintain processing order
        current_cols = ["ben_id"] + list(common_columns) + dept_specific
        df = df.rename(
            {
                col: f"{dept_abbr}_{col}"
                for col in df.columns
                if col not in common_columns and col != "ben_id"
            }
        ).select(current_cols)

        # Add new department-specific columns to global order
        column_order.extend([col for col in dept_specific if col not in column_order])

        if merged_df is None:
            merged_df = df
        else:
            # Outer join and coalesce common columns
            merged_df = merged_df.join(
                df, on="ben_id", how="outer", coalesce=True, suffix=f"_{dept_abbr}_temp"
            )

            # Clean up temporary columns from coalesce
            for col in common_columns:
                merged_df = merged_df.with_columns(
                    pl.coalesce(pl.col(col), pl.col(f"{col}_{dept_abbr}_temp")).alias(
                        col
                    )
                ).drop(f"{col}_{dept_abbr}_temp")

        log_message(
            f"Merged {dept_abbr} | Columns: {len(merged_df.columns)}", log_file_path
        )

    # 4. Enforce final column order
    final_columns = [col for col in column_order if col in merged_df.columns]
    merged_df = merged_df.select(final_columns)

    return merged_df


# Merge data and save final result
final_data = merge_department_data(
    department_files=department_files,
    log_file_path=log_file_path,
)
output_file = os.path.join(clean_data_path, "individual_benefits_clean.csv")
final_data.write_csv(output_file)
log_message(f"Saved merged data to {output_file}", log_file_path)

log_message("Data processing completed successfully", log_file_path)
