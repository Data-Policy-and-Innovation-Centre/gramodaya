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

# Set up log file path and initialize log
log_file_default = os.path.join(process_data_path, "processing_log.txt")

if os.path.exists(log_file_default):
    os.remove(log_file_default)  # Remove existing log file if it exists


def log_message(message, log_file_default=log_file_default):
    """Log a message to a specified file and print it."""
    formatted_message = f"> {message}"
    with open(log_file_default, "a") as log_file:
        log_file.write(formatted_message + "\n")
    print(formatted_message)


log_message(f"Starting data processing... Log file: {log_file_default}")

# -------------- Individual Benefits ----------------------------------------------------
# Load raw data from CSV file
individual_raw_data_path = os.path.join(raw_data_path, "Indivisual Benefits.csv")
ind_benefits_raw = pl.read_csv(
    individual_raw_data_path,
    null_values="\\N",
    infer_schema_length=1000,
)
log_message(
    f"Loaded raw data with {ind_benefits_raw.height} rows from {individual_raw_data_path}."
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
log_message("Hashed PII columns.")

# Identify and report duplicate rows with counts
duplicate_groups = (
    ind_benefits_raw.group_by(pl.all())
    .agg(pl.count().alias("occurrences"))
    .filter(pl.col("occurrences") > 1)
)

if duplicate_groups.height > 0:
    duplicate_ben_id_report_path = os.path.join(
        process_data_path, "duplicate_rows_ind_report.csv"
    )
    duplicate_groups.write_csv(duplicate_ben_id_report_path)
    total_duplicate_entries = (
        duplicate_groups["occurrences"].sum() - duplicate_groups.height
    )
    log_message(
        f"Found {duplicate_groups.height} unique duplicated rows ({total_duplicate_entries} total duplicates). "
        f"Report saved at {duplicate_ben_id_report_path}."
    )

# Record original count before deduplication
original_count = ind_benefits_raw.height

# Remove duplicate rows, keeping only the first occurrence
ind_benefits_raw = ind_benefits_raw.unique(subset=None, keep="first")
rows_removed = original_count - ind_benefits_raw.height

log_message(
    f"Removed {rows_removed} duplicate rows. "
    f"Dataset reduced from {original_count} to {ind_benefits_raw.height} rows."
)

# Ensure split_data directory exists before saving department-specific data
split_data_path = os.path.join(clean_data_path, "split_data")
split_ind_path = os.path.join(split_data_path, "individual_benefits")
ensure_directory_exists(split_ind_path)

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
    df, dept_name_col, dept_abbr_map, output_path, common_cols
):
    """Split data by department and save to CSV files, returning saved paths."""
    department_files = {}
    # Filter and save missing department entries first
    missing_departments_df = df.filter(pl.col(dept_name_col).is_null())

    if missing_departments_df.height > 0:
        missing_dept_path = os.path.join(clean_data_path, "missing_departments_ind.csv")
        missing_departments_df.write_csv(missing_dept_path)
        log_message(
            f"Saved {missing_departments_df.height} rows with missing department info to {missing_dept_path}"
        )
    else:
        log_message("No rows with missing department information found")

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
            log_message(f"Saved {dept} data to {output_file}")

    return department_files  # Return mapping of abbreviations to file paths


dept_prefixes = {abbr for abbr_list in dept_abbr.values() for abbr in abbr_list}
common_columns = [
    col
    for col in ind_benefits_raw.columns
    if not any(col.startswith(f"{abbr}_") for abbr in dept_prefixes)
]

log_message(f"Identified common columns across all departments: {common_columns}")

# Split data and get file paths
ind_department_files = split_and_save_by_department(
    ind_benefits_raw, "dept_name", dept_abbr, split_ind_path, common_columns
)


def check_ben_id_uniqueness(department_files):
    """Check uniqueness of ben_id in each department's saved CSV file."""
    duplicate_reports = []

    for abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)

        if "ben_id" not in df.columns:
            log_message(f"No ben_id column in {abbr} data")
            continue

        duplicates = (
            df.group_by("ben_id")
            .agg(pl.count().alias("occurrences"))
            .filter(pl.col("occurrences") > 1)
        )

        if duplicates.height > 0:
            log_message(f"Found {duplicates.height} duplicate ben_ids in {abbr} data")
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
            f"Saved duplicate ben_id report with {len(duplicate_reports)} entries at {report_path}"
        )
    else:
        log_message("All ben_ids are unique across department datasets")


# Check uniqueness using the actual saved files
check_ben_id_uniqueness(ind_department_files)


def remove_duplicate_ben_ids(department_files, process_data_path):
    """Return cleaned DataFrames with duplicate ben_ids removed."""
    report_path = os.path.join(process_data_path, "duplicate_ben_id_report.csv")
    cleaned_dfs = {}

    if not os.path.exists(report_path):
        log_message("No duplicate report found")
        return cleaned_dfs

    try:
        duplicate_report = pl.read_csv(report_path)
    except Exception as e:
        log_message(f"Failed reading report: {str(e)}")
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
            log_message(f"No match for department '{raw_dept_name}'")
            continue
        if matched_abbr not in dept_ben_ids:
            dept_ben_ids[matched_abbr] = set()
        dept_ben_ids[matched_abbr].add(ben_id)

    # Process each department with duplicates
    for abbr, ben_ids in dept_ben_ids.items():
        if abbr not in department_files:
            log_message(f"No file for abbreviation '{abbr}'")
            continue

        file_path = department_files[abbr]
        try:
            df = pl.read_csv(file_path)
            original_count = df.height
            cleaned_df = df.filter(~pl.col("ben_id").is_in(ben_ids))
            removed_count = original_count - cleaned_df.height

            if removed_count > 0:
                cleaned_dfs[abbr] = cleaned_df
                log_message(f"Removed {removed_count} entries of ben_ids from {abbr}")
        except Exception as e:
            log_message(f"Failed processing {file_path}: {str(e)}")

    return cleaned_dfs


# Process duplicates and save cleaned data
cleaned_dfs = remove_duplicate_ben_ids(ind_department_files, process_data_path)
for abbr, df in cleaned_dfs.items():
    file_path = ind_department_files[abbr]
    df.write_csv(file_path)
    log_message(f"Saved cleaned data for {abbr} to {file_path}")


def merge_department_data(department_files, unique_id):
    """Merge department datasets while preserving column order.

    Args:
        department_files (dict): Dictionary mapping department abbreviations to file paths
        unique_id (str): Name of the column containing unique identifiers (e.g., "village_id", "individual_id")

    Returns:
        pl.DataFrame: Merged DataFrame with consistent column order
    """
    # Validate unique_id parameter
    if not unique_id or not isinstance(unique_id, str):
        raise ValueError(
            "unique_id must be a non-empty string specifying the ID column name"
        )

    # 1. Identify common columns across ALL departments
    all_columns = []
    for dept_abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)
        if unique_id not in df.columns:
            raise ValueError(
                f"Unique ID column '{unique_id}' missing in {dept_abbr} data"
            )
        all_columns.append(set(df.columns))

    common_columns = set.intersection(*[set(cols) for cols in all_columns]) - {
        unique_id
    }
    common_columns = sorted(common_columns)
    log_message(f"Common columns identified: {common_columns}")

    # 2. Initialize merged dataframe and column order tracker
    merged_df = None
    column_order = [unique_id] + list(common_columns)

    # 3. Process departments in given order
    for dept_abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)

        # Create department-specific column list with prefixes
        dept_specific = [
            f"{dept_abbr}_{col}"
            for col in df.columns
            if col not in common_columns and col != unique_id
        ]

        # Rename columns and maintain processing order
        current_cols = [unique_id] + list(common_columns) + dept_specific
        df = df.rename(
            {
                col: f"{dept_abbr}_{col}"
                for col in df.columns
                if col not in common_columns and col != unique_id
            }
        ).select(current_cols)

        # Add new department-specific columns to global order
        column_order.extend([col for col in dept_specific if col not in column_order])

        if merged_df is None:
            merged_df = df
        else:
            # Outer join and coalesce common columns
            merged_df = merged_df.join(
                df,
                on=unique_id,
                how="outer",
                coalesce=True,
                suffix=f"_{dept_abbr}_temp",
            )

            # Clean up temporary columns from coalesce
            for col in common_columns:
                merged_df = merged_df.with_columns(
                    pl.coalesce(pl.col(col), pl.col(f"{col}_{dept_abbr}_temp")).alias(
                        col
                    )
                ).drop(f"{col}_{dept_abbr}_temp")

        log_message(f"Merged {dept_abbr} | Columns: {len(merged_df.columns)}")

    # 4. Enforce final column order
    final_columns = [col for col in column_order if col in merged_df.columns]
    merged_df = merged_df.select(final_columns)

    return merged_df


# Merge data and save final result
final_data = merge_department_data(
    department_files=ind_department_files, unique_id="ben_id"
)
final_ind_path = os.path.join(clean_data_path, "individual_benefits_clean.csv")
final_data.write_csv(final_ind_path)
log_message(f"Saved merged data to {final_ind_path}")

# ----------------------------------------------------------------------------------------

# ------------------------------- Village Benefits ---------------------------------------

village_raw_data_path = os.path.join(raw_data_path, "Village Wise Data.csv")
village_benefits_raw = pl.read_csv(
    village_raw_data_path,
    encoding="utf-8",
    null_values="\\N",
    infer_schema_length=0,
)

log_message(
    f"Loaded raw data with {village_benefits_raw.height} rows from {village_raw_data_path}."
)

# Identify and report duplicate rows with counts
duplicate_groups = (
    village_benefits_raw.group_by(pl.all())
    .agg(pl.count().alias("occurrences"))
    .filter(pl.col("occurrences") > 1)
)

if duplicate_groups.height > 0:
    duplicate_vill_id_report_path = os.path.join(
        process_data_path, "duplicate_rows_vill_report.csv"
    )
    duplicate_groups.write_csv(duplicate_vill_id_report_path)
    total_duplicate_entries = (
        duplicate_groups["occurrences"].sum() - duplicate_groups.height
    )
    log_message(
        f"Found {duplicate_groups.height} unique duplicated rows ({total_duplicate_entries} total duplicates). "
        f"Report saved at {duplicate_vill_id_report_path}."
    )

# Record original count before deduplication
original_count = village_benefits_raw.height

# Remove duplicate rows, keeping only the first occurrence
village_benefits_raw = village_benefits_raw.unique(subset=None, keep="first")
rows_removed = original_count - village_benefits_raw.height

log_message(
    f"Removed {rows_removed} duplicate rows. "
    f"Dataset reduced from {original_count} to {village_benefits_raw.height} rows."
)

# Split data by departm
split_vill_path = os.path.join(split_data_path, "village_benefits")
ensure_directory_exists(split_vill_path)


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
    "Water Resource": ["water"],
    "Higher Education": ["higher_edu"],
    "Commerce & Transport": ["transport"],
    "Sport & Youth Affairs": ["sport"],
}

dept_prefixes = {abbr for abbr_list in dept_abbr.values() for abbr in abbr_list}
# Identify common columns for village data (columns without department prefixes)
common_columns_village = [
    col
    for col in village_benefits_raw.columns
    if not any(col.startswith(f"{abbr}_") for abbr in dept_prefixes)
]

log_message(f"Village common columns: {common_columns_village}")


def split_and_save_by_department(
    df, dept_name_col, dept_abbr_map, output_path, common_cols
):
    """Split data by department and save to CSV files, returning saved paths."""
    department_files = {}
    # Filter and save missing department entries first
    missing_departments_df = df.filter(pl.col(dept_name_col).is_null())

    if missing_departments_df.height > 0:
        missing_dept_path = os.path.join(
            clean_data_path, "missing_departments_vill.csv"
        )
        missing_departments_df.write_csv(missing_dept_path)
        log_message(
            f"Saved {missing_departments_df.height} rows with missing department info to {missing_dept_path}"
        )
    else:
        log_message("No rows with missing department information found")

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
            log_message(f"Saved {dept} data to {output_file}")

    return department_files  # Return mapping of abbreviations to file paths


# Split and save village department data
village_department_files = split_and_save_by_department(
    village_benefits_raw,
    dept_name_col="dept_name",
    dept_abbr_map=dept_abbr,
    output_path=split_vill_path,
    common_cols=common_columns_village,
)


def check_vill_id_uniqueness(department_files):
    """Check uniqueness of ben_id in each department's saved CSV file."""
    duplicate_reports = []

    for abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)

        if "village_id" not in df.columns:
            log_message(f"No village_id column in {abbr} data")
            continue

        duplicates = (
            df.group_by("village_id")
            .agg(pl.count().alias("occurrences"))
            .filter(pl.col("occurrences") > 1)
        )

        if duplicates.height > 0:
            log_message(
                f"Found {duplicates.height} duplicate village_ids in {abbr} data"
            )
            duplicate_reports.extend(
                [
                    {
                        "department": abbr,
                        "village_id": row["village_id"],
                        "occurrences": row["occurrences"],
                    }
                    for row in duplicates.iter_rows(named=True)
                ]
            )

    if duplicate_reports:
        report_df = pl.DataFrame(duplicate_reports)
        report_path = os.path.join(process_data_path, "duplicate_village_id_report.csv")
        report_df.write_csv(report_path)
        log_message(
            f"Saved duplicate village_ids report with {len(duplicate_reports)} entries at {report_path}"
        )
    else:
        log_message("All village_ids are unique across department datasets")


check_vill_id_uniqueness(village_department_files)


def remove_duplicate_village_ids(department_files, process_data_path):
    """Return cleaned DataFrames with duplicate ben_ids removed."""
    report_path = os.path.join(process_data_path, "duplicate_vill_id_report.csv")
    cleaned_dfs = {}

    if not os.path.exists(report_path):
        log_message("No duplicate report found")
        return cleaned_dfs

    try:
        duplicate_report = pl.read_csv(report_path)
    except Exception as e:
        log_message(f"Failed reading report: {str(e)}")
        return cleaned_dfs

    # Create reverse department mapping
    reverse_dept_mapping = {}
    for full_name, abbr_list in dept_abbr.items():
        for abbr in abbr_list:
            reverse_dept_mapping[full_name.lower()] = abbr
            reverse_dept_mapping[abbr.lower()] = abbr

    # Group report by department and collect village_ids
    dept_village_ids = {}
    for row in duplicate_report.iter_rows(named=True):
        raw_dept_name = row["department"]
        village_id = row["village_id"]

        normalized_dept = raw_dept_name.strip().lower()
        matched_abbr = reverse_dept_mapping.get(normalized_dept)
        if not matched_abbr:
            log_message(f"No match for department '{raw_dept_name}'")
            continue
        if matched_abbr not in dept_village_ids:
            dept_village_ids[matched_abbr] = set()
        dept_village_ids[matched_abbr].add(village_id)

    # Process each department with duplicates
    for abbr, ben_ids in dept_village_ids.items():
        if abbr not in department_files:
            log_message(f"No file for abbreviation '{abbr}'")
            continue

        file_path = department_files[abbr]
        try:
            df = pl.read_csv(file_path)
            original_count = df.height
            cleaned_df = df.filter(~pl.col("village_id").is_in(village_id))
            removed_count = original_count - cleaned_df.height

            if removed_count > 0:
                cleaned_dfs[abbr] = cleaned_df
                log_message(
                    f"Removed {removed_count} entries of village_ids from {abbr}"
                )
        except Exception as e:
            log_message(f"Failed processing {file_path}: {str(e)}")

    return cleaned_dfs


# Process duplicates and save cleaned data
cleaned_dfs = remove_duplicate_village_ids(
    department_files=village_department_files, process_data_path=process_data_path
)
for abbr, df in cleaned_dfs.items():
    file_path = village_department_files[abbr]
    df.write_csv(file_path)
    log_message(f"Saved cleaned data for {abbr} to {file_path}")


def merge_department_data(department_files, unique_id):
    """Merge department datasets while preserving column order.

    Args:
        department_files (dict): Dictionary mapping department abbreviations to file paths
        unique_id (str): Name of the column containing unique identifiers (e.g., "village_id", "individual_id")

    Returns:
        pl.DataFrame: Merged DataFrame with consistent column order
    """
    # Validate unique_id parameter
    if not unique_id or not isinstance(unique_id, str):
        raise ValueError(
            "unique_id must be a non-empty string specifying the ID column name"
        )

    # 1. Identify common columns across ALL departments
    all_columns = []
    for dept_abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)
        if unique_id not in df.columns:
            raise ValueError(
                f"Unique ID column '{unique_id}' missing in {dept_abbr} data"
            )
        all_columns.append(set(df.columns))

    common_columns = set.intersection(*[set(cols) for cols in all_columns]) - {
        unique_id
    }
    common_columns = sorted(common_columns)
    log_message(f"Common columns identified: {common_columns}")

    # 2. Initialize merged dataframe and column order tracker
    merged_df = None
    column_order = [unique_id] + list(common_columns)

    # 3. Process departments in given order
    for dept_abbr, file_path in department_files.items():
        df = pl.read_csv(file_path)

        # Create department-specific column list with prefixes
        dept_specific = [
            f"{dept_abbr}_{col}"
            for col in df.columns
            if col not in common_columns and col != unique_id
        ]

        # Rename columns and maintain processing order
        current_cols = [unique_id] + list(common_columns) + dept_specific
        df = df.rename(
            {
                col: f"{dept_abbr}_{col}"
                for col in df.columns
                if col not in common_columns and col != unique_id
            }
        ).select(current_cols)

        # Add new department-specific columns to global order
        column_order.extend([col for col in dept_specific if col not in column_order])

        if merged_df is None:
            merged_df = df
        else:
            # Outer join and coalesce common columns
            merged_df = merged_df.join(
                df,
                on=unique_id,
                how="outer",
                coalesce=True,
                suffix=f"_{dept_abbr}_temp",
            )

            # Clean up temporary columns from coalesce
            for col in common_columns:
                merged_df = merged_df.with_columns(
                    pl.coalesce(pl.col(col), pl.col(f"{col}_{dept_abbr}_temp")).alias(
                        col
                    )
                ).drop(f"{col}_{dept_abbr}_temp")

        log_message(f"Merged {dept_abbr} | Columns: {len(merged_df.columns)}")

    # 4. Enforce final column order
    final_columns = [col for col in column_order if col in merged_df.columns]
    merged_df = merged_df.select(final_columns)

    return merged_df


# Merge data and save final result
final_data_village = merge_department_data(
    department_files=village_department_files, unique_id="village_id"
)
final_village_path = os.path.join(clean_data_path, "village_benefits_clean.csv")
final_data_village.write_csv(final_village_path)
log_message(f"Saved final data for villages to {final_village_path}")


log_message("Data processing completed successfully")
