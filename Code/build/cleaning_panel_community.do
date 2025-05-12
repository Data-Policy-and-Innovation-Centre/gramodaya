// Boilerplate 
collect clear

do "0_setup_master.do"

// 2. Setup
tempfile combined
local first = 1
local files : dir "$raw_data/Community Data Village wise -29-4-25" files "*.csv"

// 3. Loop through files
foreach file of local files {
    di "Processing: `file'"
    
    // Import file
    import delimited using `"${raw_data}\\`file'"', clear stringcols(_all)
    
    // Standardize variable names and values
    rename *, lower
    ds, has(type string)
    foreach var of varlist `r(varlist)' {
        replace `var' = lower(`var')
    }

    // First file: save and continue
    if `first' == 1 {
        save `combined', replace
        local first = 0
    }
    else {
        tempfile addvars
        save `addvars', replace

        // Use the first 8 variables (common variables for merging)
        local common_vars district_id district_name block_id block_name panchayat_id panchayat_name village_id village_name

        use `combined', clear
        // Ensure the common variables are present in both datasets before merging
        foreach var of local common_vars {
            capture confirm variable `var'
            if _rc {
                di as error "`var' not found in `combined'. Exiting merge."
                exit
            }
        }

        use `addvars', clear
        foreach var of local common_vars {
            capture confirm variable `var'
            if _rc {
                di as error "`var' not found in `addvars'. Exiting merge."
                exit
            }
        }

        // De-duplicate using dataset based on common variables
        quietly duplicates drop `common_vars', force

        save `addvars', replace
        use `combined', clear

        // Also de-duplicate master to be safe
        quietly duplicates drop `common_vars', force

        // Merge using the common variables
        di "Merging on: `common_vars'"
        merge 1:1 `common_vars' using `addvars', nogenerate
        save `combined', replace
    }
}

// 4. Standardize Missing Values to ""
ds
local found 0
foreach var of varlist `rvarlist' {
    if "`var'" == "village_name" { 
        local found 1 
        continue 
    }

    if `found' == 1 {
        // Clean variable name (lowercase, replace spaces and special characters)
        local newvar = lower("`var'")
        local newvar : subinstr local newvar " " "_", all
        local newvar : subinstr local newvar "-" "_", all
        local newvar : subinstr local newvar "(" "", all
        local newvar : subinstr local newvar ")" "", all
        local newvar : subinstr local newvar "/" "_", all
        local newvar : subinstr local newvar "," "", all
        local newvar : subinstr local newvar "__" "_", all

        // Trim trailing or leading underscores
        while substr("`newvar'", 1, 1) == "_" {
            local newvar = substr("`newvar'", 2, .)
        }
        while substr("`newvar'", -1, 1) == "_" {
            local newvar = substr("`newvar'", 1, length("`newvar'") - 1)
        }

        // Add the prefix to the cleaned variable name
        local newvar = "`prefix'_" + "`newvar'"

        // Only rename if it’s different from the original
        if ("`var'" != "`newvar'") {
            capture rename `var' `newvar'
        }
    }
}
// 4. Final Save
use `combined', clear
save "$data_cleaned/merged_community_data.dta", replace