// 0. Boilerplate and Import configuration
collect clear

do "0_setup_master.do"

//Cleaning Setup File Paths
global hh_wise "$raw_data/HH Wise Data 29-5-25"

***************************************************************
* 1. Import CSVs and append to master file 
***************************************************************
tempfile combined
local first = 1 
local files : dir "$raw_data" files "*.csv"
foreach file of local files {
	import delimited using "`file'", clear
    ds, has(type string)
    foreach var of varlist `r(varlist)' {
        replace `var' = lower(`var')
    }

    if `first' == 1 {
        save `combined', replace
        local first = 0
    }
    else {
        append using `combined'
        save `combined', replace
    }
}

***************************************************************
* 2. Hashing Aadhaar and Ration Card Numbers 
***************************************************************

python:

import pandas as pd
import hashlib
import pyreadstat

def hash_data(series):
    return series.astype(str).apply(lambda x: hashlib.md5(x.encode()).hexdigest() if x and x != 'nan' else None)

df, meta = pyreadstat.read_dta(r"`combined'")

if 'aadhar_no' in df.columns:
    df['hashed_aadhar'] = hash_data(df['aadhar_no'])

if 'ration_card_no' in df.columns:
    df['hashed_ration_card'] = hash_data(df['ration_card_no'])

# Save updated file
pyreadstat.write_dta(df, r"`combined'")
end

use `combined', clear

***************************************************************
* 3. Remove Columns with 'Specify' in their Name
***************************************************************

ds *specify*
foreach var of varlist `r(varlist)' {
    drop `var'
}

***************************************************************
* 4. Standardize Missing Values to "\N"
***************************************************************

ds, has(type string)
foreach var of varlist `r(varlist)' {
    replace `var' = "\N" if trim(`var') == "" | `var' == "."
}

//Renaming variables to remove spaces and special characters
local filename : subinstr local file ".csv" ""  // Remove .csv extension
local prefix : word 1 of `filename'  // First word as prefix
local prefix = lower("`prefix'")  // Convert to lowercase for consistency
local prefix : subinstr local prefix " " "_", all  // Replace spaces with underscores

**Rename each variable with the prefix and shorten the name
ds
foreach var of varlist `r(varlist)' {
* Clean variable name (lowercase, replace spaces and special characters)
        local newvar = lower("`var'")
        local newvar : subinstr local newvar " " "_", all
        local newvar : subinstr local newvar "-" "_", all
        local newvar : subinstr local newvar "(" "", all
        local newvar : subinstr local newvar ")" "", all
        local newvar : subinstr local newvar "/" "_", all
        local newvar : subinstr local newvar "," "", all
        local newvar : subinstr local newvar "__" "_", all

        * Trim trailing or leading underscores
        while substr("`newvar'", 1, 1) == "_" {
            local newvar = substr("`newvar'", 2, .)
        }
        while substr("`newvar'", -1, 1) == "_" {
            local newvar = substr("`newvar'", 1, length("`newvar'") - 1)
        }

        * Add the prefix to the cleaned variable name
        local newvar = "`prefix'_" + "`newvar'"

        * Truncate if variable name is too long for Stata
        if length("`newvar'") > 32 {
            local newvar = substr("`newvar'", 1, 32)
        }

        * Only rename if it’s different from the original
        if ("`var'" != "`newvar'") {
            capture rename `var' `newvar'
        }
    }
***************************************************************
* 6. Standardize Missing Values (All Variables)
***************************************************************
foreach var of varlist _all {
    * Only replace missing values if the variable is not a string variable
    if !missing(`var') & !strvar(`var') {
        replace `var' = "\N" if missing(`var')
    }
}

***************************************************************
* 7. Sort the Data by District, Block, Village, and Panchayat and Drop Duplicates
***************************************************************
sort district_name block_name panchayat_name village_name
duplicates drop 
***************************************************************
* 8. Save the Cleaned Dataset
***************************************************************
save "$data_cleaned/clean_gramodaya_beneficiary.dta", replace

