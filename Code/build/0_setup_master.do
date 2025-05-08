** Gramodaya Survey Cleaning **

* 0. Setup *

// BOILERPLATE -----------------------------------------------------------------
set more off
set varabbrev off
clear all
set maxvar 16000
macro drop _all

* edit as needed*
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
}

// FILE PATHS ------------------------------------------------------------------
global code "$github/Code"
global raw_data "$box/7. Raw Data - Working/gramodaya"
global process_data "$box/8. Analysis/gramodaya"
global data_temp "$process_data/Temp"
global data_cleaned "$box/9. Cleaned Data/gramodaya"

// ERASE TEMP ------------------------------------------------------------------
cd "$data_temp"
local files : dir . files *
foreach file of local files {
	rm `file'
}
clear

// Install Necessary Packages---------------------------------------------------
local statapackages rscript

* Check to see if statapackages are installed; if not, install them.
foreach statapackage in `statapackages' {
	cap which `statapackage'
	if _rc {
		display "`statapackage' not installed"
		ssc install `statapackage'
	}
	else {
		display "`statapackage' already installed"
	}
}