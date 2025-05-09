clear all
set more off
set varabbrev off

// DIRECTORIES -----------------------------------------------------------------
if c(username) == "Nikil" {
    global box "C:/Users/Admin/Box/2. Projects/11. PR&DW"
	global RSOURCE_PATH "C:/Program Files/R/R-4.3.2/bin/Rscript"
	global Rterm_options `"--vanilla"'
	global github "C:/Users/Admin/OneDrive - University of Chicago IIC/Desktop/gramodaya"
}
else if c(username) == "Admin" { // Admin refers to aastha 
    global box "C:/Users/Admin/Box/2. Projects/11. PR&DW"
	global RSOURCE_PATH "C:/Program Files/R/R-4.3.2/bin/Rscript"
    global Rterm_options `"--vanilla"'
	global github "C:/Users/Admin/OneDrive - University of Chicago IIC/Desktop/gramodaya"

// Setting up data paths
global raw_data "C:\Users\Admin\Box\11. PR&DW\4. Raw Data\gramodaya"

cd "$raw_data"

// Create summary file to store results
tempname summary
postfile `summary' str60 file_name obs vars ndistricts nblocks nvillages n_duplicates n_missing_ben_id n_missing_district n_missing_block n_missing_village using "gramodaya_summary.dta", replace

// Loop over all CSV files in the directory
local files : dir "$raw_data" files "*.csv"

foreach file of local files {
    display "================================================="
    display "Processing: `file'"

    // Import the current CSV file
    import delimited "`file'", clear

    // Check observations and variables
    local obs = _N
    ds
    local vars : word count `r(varlist)'

    // Count unique levels for district_id, block_id, and village_id
    local ndistricts = 0
    cap confirm variable district_id
    if !_rc {
        quietly levelsof district_id, local(dids)
        local ndistricts : word count `dids'
    }

    local nblocks = 0
    cap confirm variable block_id
    if !_rc {
        quietly levelsof block_id, local(bids)
        local nblocks : word count `bids'
    }

    local nvillages = 0
    cap confirm variable village_id
    if !_rc {
        quietly levelsof village_id, local(vids)
        local nvillages : word count `vids'
    }

    // Initialize counts for missing and duplicates
    local n_duplicates = 0
    local n_missing_ben_id = 0
    local n_missing_district = 0
    local n_missing_block = 0
    local n_missing_village = 0

    // Check for missing values in ben_id, district_id, block_id, and village_id
    cap confirm variable ben_id
    if !_rc {
        count if missing(ben_id)
        local n_missing_ben_id = r(N)
        duplicates tag ben_id, gen(dup_tag)
        quietly count if dup_tag > 0
        local n_duplicates = r(N)
        drop dup_tag
    }

    cap confirm variable district_id
    if !_rc {
        count if missing(district_id)
        local n_missing_district = r(N)
    }

    cap confirm variable block_id
    if !_rc {
        count if missing(block_id)
        local n_missing_block = r(N)
    }

    cap confirm variable village_id
    if !_rc {
        count if missing(village_id)
        local n_missing_village = r(N)
    }

    // Posting data to summary file
    post `summary' ("`file'") (`obs') (`vars') (`ndistricts') (`nblocks') (`nvillages') (`n_duplicates') (`n_missing_ben_id') (`n_missing_district') (`n_missing_block') (`n_missing_village')

    // Log the counts and checks
    display "Observations: `obs'"
    display "Variables: `vars'"
    display "Districts: `ndistricts'"
    display "Blocks: `nblocks'"
    display "Villages: `nvillages'"
    display "Duplicates (ben_id): `n_duplicates'"
    display "Missing ben_id: `n_missing_ben_id'"
    display "Missing district_id: `n_missing_district'"
    display "Missing block_id: `n_missing_block'"
    display "Missing village_id: `n_missing_village'"

    // Quick value checks for gender, occupation, and age
    cap tab gender
    cap summarize age
    cap tab occupation

    // Ensure the consistency of ID variables
    cap tab district_id
    cap tab block_id
    cap tab village_id
}

// Close the postfile and log
postclose `summary'

// Export master summary to Excel
use "gramodaya_summary.dta", clear
sort file_name
export excel using "gramodaya_summary.xlsx", firstrow(variables) replace

