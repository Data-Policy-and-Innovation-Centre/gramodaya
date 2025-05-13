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
    import delimited using `"${raw_data}/`file'"', clear stringcols(_all)
    
    // Standardize variable names and values
    rename *, lower
    ds, has(type string)
    foreach var of varlist `r(varlist)' {
        replace `var' = lower(trim(`var'))
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
        quietly duplicates drop `common_vars', force
        save `combined', replace

        use `addvars', clear
        quietly duplicates drop `common_vars', force
        save `addvars', replace

        use `combined', clear
        merge 1:1 `common_vars' using `addvars', nogenerate
        save `combined', replace
    }
}

// Load merged data
use `combined', clear

// Replace empty strings with "\n"
ds, has(type string)
foreach var of varlist `r(varlist)' {
    replace `var' = "\n" if trim(`var') == ""
}

// List of variables expected to be purely numeric
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
    numberoffunctionaltoilet ///
    teacherspositionsanction ///
    teacherspresentposition ///
	theroadbelongsto ///
	availabilityofconcreteroadslengt ///


// Clean and convert to numeric if needed
foreach var of local numeric_vars {
    capture confirm variable `var'
    if !_rc {
        capture confirm string variable `var'
        if !_rc {
            // If string: remove non-numerics
            replace `var' = "\n" if !regexm(trim(`var'), "^-?[0-9]+(\.[0-9]*)?$")
            destring `var', replace ignore("\n") force
        }
        else {
            // Already numeric, do nothing
        }
    }
}

// Additional Cleaning Steps
// Drop the 'v11' column
drop v11

// Rename 'v14' as 'subcentres_pp'
rename v14 subcentres_pp

// Drop the 'paramilitaryandpoliceservice' and 'requirementofconcreteroadslength' columns
drop paramilitaryandpoliceservice requirementofconcreteroadslength

// Replace all instances of "--select--" with missing values (.)

replace ownrentalbuilding = trim(ownrentalbuilding)
drop if ownrentalbuilding == "--select--"

replace drinkingwaterfacility = trim(drinkingwaterfacility)
drop if drinkingwaterfacility == "--select--"

replace toiletfacility = trim(toiletfacility)
drop if toiletfacility == "--select--"

replace electrified = trim(electrified)
drop if electrified == "--select--"

replace kitchenfacility = trim(kitchenfacility)
drop if kitchenfacility == "--select--"

replace lpgconnection = trim(lpgconnection)
drop if lpgconnection == "--select--"

// Remove duplicates
duplicates drop
sort `common_vars'


// 4. Final Save
use `combined', clear
save "$cleaneddata/gramodaya_community_data.dta", replace
