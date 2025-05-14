// Boilerplate
collect clear

do "0_setup_master.do"

// Setup for Household-Level Data
tempfile hh_combined
local first = 1
local hh_files : dir "$raw_data/HH Wise Data 29-4-25" files "*.csv"

// Create summary file for Household-level data
tempname hh_summary
postfile `hh_summary' str60 file_name obs vars ndistricts nblocks nvillages n_duplicates n_missing_ben_id n_missing_district n_missing_block n_missing_village using "gramodaya_hh_summary.dta", replace

// Loop over all CSV files in the Household Data directory
foreach file of local hh_files {
    display "================================================="
    display "Processing Household Data: `file'"

    // Import the current CSV file for Household data
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
    post `hh_summary' ("`file'") (`obs') (`vars') (`ndistricts') (`nblocks') (`nvillages') (`n_duplicates') (`n_missing_ben_id') (`n_missing_district') (`n_missing_block') (`n_missing_village')

    // Log the counts and checks for Household Data
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

// Close the postfile for Household-level data
postclose `hh_summary'

// Export Household-level summary to Excel
use "gramodaya_hh_summary.dta", clear
sort file_name
export excel using "$processdata/gramodaya_hh_summary.xlsx", firstrow(variables) replace


// Setup for Community-Level Data
tempfile community_combined
local first = 1
local community_files : dir "$raw_data/Community Data Village wise -29-4-25" files "*.csv"

// Create summary file for Community-level data
tempname community_summary
postfile `community_summary' str60 file_name obs vars ndistricts nblocks nvillages n_duplicates n_missing_ben_id n_missing_district n_missing_block n_missing_village using "gramodaya_community_summary.dta", replace

// Loop over all CSV files in the Community Data directory
foreach file of local community_files {
    display "================================================="
    display "Processing Community Data: `file'"

    // Import the current CSV file for Community data
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
    post `community_summary' ("`file'") (`obs') (`vars') (`ndistricts') (`nblocks') (`nvillages') (`n_duplicates') (`n_missing_ben_id') (`n_missing_district') (`n_missing_block') (`n_missing_village')

    // Log the counts and checks for Community Data
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

// Close the postfile for Community-level data
postclose `community_summary'

// Export Community-level summary to Excel
use "gramodaya_community_summary.dta", clear
sort file_name
export excel using "$processdata/gramodaya_community_summary.xlsx", firstrow(variables) replace
