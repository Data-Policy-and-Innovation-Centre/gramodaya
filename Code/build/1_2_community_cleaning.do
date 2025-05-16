// Boilerplate
collect clear
do "0_setup_master.do"

// 1. Setup
tempfile combined
local first = 1

// 2. Get list of CSV files from subfolder
local folder "$raw_data/Community Data Village wise -29-4-25"
local files : dir "`folder'" files "*.csv"

// 3. Loop through each file
foreach file of local files {
    di "Processing: `file'"

    // Build full file path safely
    local filepath `"`folder'/`file'"'

    // Import CSV with compound quotes to handle spaces and &
    capture import delimited using "`filepath'", clear stringcols(_all)
    if _rc {
        di as error "⚠️ Failed to import: `filepath'"
        continue
    }

    // Standardize variable names
    rename *, lower
    ds, has(type string)
    foreach var of varlist `r(varlist)' {
        replace `var' = lower(trim(`var'))
    }

    // Save or merge
    if `first' == 1 {
        save `combined', replace
        local first = 0
    }
    else {
        tempfile addvars
        save `addvars', replace

        local common_vars district_id district_name block_id block_name panchayat_id panchayat_name village_id village_name

        // Drop duplicates before merging
        use `combined', clear
        quietly duplicates drop `common_vars', force
        save `combined', replace

        use `addvars', clear
        quietly duplicates drop `common_vars', force
        save `addvars', replace

        // Merge
        use `combined', clear
        merge 1:1 `common_vars' using `addvars', nogenerate
        save `combined', replace
    }
}

// 4. Load merged dataset
use `combined', clear

// 5. Convert string to numeric for known numeric variables
local numeric_vars ///
    nooffunctionalsolarpumpsavailabl ///
    numberofhouseholdprovidedpipewat ///
    numberoffunctionaltoilet ///
    numberofstudentsenrolledinreside ///
    maninpositionanganwadiworkers ///
    maninpositionanganwadihelpers ///
    healthworkerspositioninthehealth ///
    v14 ///
    numberoffunctionaltubewell ///
    teacherspositionsanction ///
    teacherspresentposition ///
    theroadbelongsto ///
    availabilityofconcreteroadslengt

foreach var of local numeric_vars {
    capture confirm variable `var'
    if !_rc {
        capture confirm string variable `var'
        if !_rc {
            replace `var' = "\n" if !regexm(trim(`var'), "^-?[0-9]+(\.[0-9]*)?$")
            destring `var', replace ignore("\n") force
        }
    }
}

// 6. Additional Cleaning
capture drop v11
capture rename v14 subcentres_pp
capture drop paramilitaryandpoliceservice requirementofconcreteroadslength

foreach var in ownrentalbuilding drinkingwaterfacility toiletfacility electrified kitchenfacility lpgconnection {
    capture replace `var' = trim(`var')
    capture drop if `var' == "--select--"
}

// Replace empty strings with "\n"
ds, has(type string)
foreach var of varlist `r(varlist)' {
    replace `var' = "\n" if trim(`var') == ""
}

// 7. Remove duplicates and sort
duplicates drop
sort district_id district_name block_id block_name panchayat_id panchayat_name village_id village_name

// 8. Save final cleaned dataset
save `"C:/Users/Admin/Box/11. PR&DW/9. Cleaned Data/gramodaya/community_cleaned.dta"', replace


// 9. Run R script for summary report

rscript using "$code/build/1_2_1_summary_report.r"
