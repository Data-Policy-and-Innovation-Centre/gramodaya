// 0. Boilerplate and Import configuration
collect clear

do "0_setup_master.do"

//Cleaning Setup File Paths
global hh_wise "$raw_data/HH Wise Data 29-5-25"

***************************************************************
* 1. Import CSVs 
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
    return series.astype(str).apply(lambda x: hashlib.sha256(x.encode()).hexdigest() if pd.notna(x) and str(x).strip().lower() != 'nan' else None)

df, meta = pyreadstat.read_dta(r"`combined'")

columns_to_hash = {
    'aadhar_no' : 'hashed_aadhar',
    'ration_card_no' : 'hashed_ration_card'
    'father_husband_name' : 'hashed_father_husband_name',
    'beneficiary_name' :'enocded_beneficiary_name',
    'village_name'  : 'encoded_village_name',
    'block_name' : 'encoded_block_name',
    'district_name' : 'encoded_district_name',
}

for col, new_col in columns_to_hash.items(): 
    if col in df.columns:
        df[new_col] = sha256_encode(df[col])

    pyreadstat.write_dta(df, r"`combined'")

end

***************************************************************
* 3. Standardize Missing Values to ""
***************************************************************
ds 
local found 0
foreach var of varlist `rvarlist'{
    if "`var'" == "occupation" { 
        local found 1 
        continue 
}

if `found' == 1 {

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

        * Only rename if it’s different from the original
        if ("`var'" != "`newvar'") {
            capture rename `var' `newvar'
        }
    }

***************************************************************
* 4. Sort the Data by District, Block, Village, and Panchayat and Drop Duplicates
***************************************************************
sort district_name block_name panchayat_name village_name
duplicates drop 

5. Drop Inaccurate Age 

    // Make all string variables lowercase
    ds, has(type string)
    foreach var of varlist `r(varlist)' {
        replace `var' = lower(`var')
    }

    // Remove observations with implausible age
    capture confirm variable age
    if !_rc {
        destring age, replace ignore("., ")
        drop if age > 120
    }
***************************************************************
* 5. Save the Cleaned Dataset
***************************************************************
save "$data_cleaned/clean_gramodaya_beneficiary.dta", replace

