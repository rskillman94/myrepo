### Detect Violating WS V2.0 ###
# Author: Rachel Skillman
# Date Created: 8/18/25
# Date Updated: 8/18/25

#TO DO:
# - Need to get reporting unit to match, and scale this up! Ask Dan
#   - Get UoM from TMNALRA even though MCL is not kept up to date

# - How to get sources and facilities (change the facility id pulled)

#### Set Working Directory ####

setwd("C:/Users/rskillman/Downloads") #CHANGE

#### Load Libraries ####

#install.packages("")
library(readxl)
library(tidyverse)
library(odbc) #object database connection
library(lubridate) #for date conversions
library(slider) #for RAA calculation
library(purrr) #for loop


#### DLR Reference List ####
# Inorganic Chemicals (start with these - should I include hex chrom?)

# Aluminum (1002) - 1. mg/L (DLR = 0.05 mg/L) [1 sf]
# - (as a secondary analyte) 0.2 mg/L [1 sf]

# Antimony (1074) - 0.006 mg/L (DLR = 0.006) [1 sf] MCL = DLR?

# Arsenic (1005) - 0.010 mg/L (DLR = 0.002 mg/L) [2 sf]

# Asbestos (1094) - 7 MFL (DLR = 0.2 MFL>10um) [1 sf]

# Barium (1010) - 1. mg/L (DLR = 0.1 mg/L) [1 sf] [BUT THE ANALYTE IS MEASURED IN UG/L]

# Beryllium (1075) - 0.004 mg/L (DLR = 0.001 mg/L) [1 sf]

# Cadmium (1015) - 0.005 mg/L (DLR = 0.001 mg/L) [1 sf]

# Chromium (hexavalent) (1080) - 0.010 mg/L (DLR = 0.0001 mg/L) [2 sf]

# Chromium (total) (1020) - 0.05 mg/L (DLR = 0.01 mg/L) [1 sf]

# Cyanide (1024) - 0.15 mg/L (DLR = 0.1 mg/L) [2 sf]

# Fluoride (1025) - 2.0 mg/L (DLR = 0.1 mg/L) [2 sf]

# Mercury (1035) - 0.002 mg/L (DLR = 0.001 mg/L) [1 sf]

# Nickel (1036) - 0.1 mg/L (DLR = 0.01 mg/L) [1 sf]

# Selenium (1045) - 0.05 mg/L (DLR = 0.005 mg/L) [1 sf]

# Thallium (1085) - 0.002 mg/L (DLR = 0.001 mg/L) [1 sf]

#### Set desired information ####

water_system_of_interest <- "CA0110003"
facility_of_interest <- "014"
analyte_of_interest <- "1010"
all_results <- list()

#### Open the Server Connection for SSMS Pulls ####
con <- dbConnect(odbc(),                 
                 Driver = "SQL Server",                 
                 Server = "reportmanager,1542",                 
                 Database = "ReportDB",                 
                 UID = Sys.getenv("CDM5_SQL_SERVER_USER"),
                 PWD = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
                 TrustServerCertificate = "yes",
                 Port = 1542) #must be logged into SSMS and/or VPN

#### Pull relevant SDWIS tables	####

# Query System List from SDWIS TINWSYS 
tinwsys <- dbGetQuery(con, "SELECT 
                        	TINWSYS_IS_NUMBER,
                        	ACTIVITY_STATUS_CD,
                        	NUMBER0,
                        	NAME,
                        	D_POPULATION_COUNT,
                        	D_PWS_FED_TYPE_CD
                    FROM [ReportDB].[SDWIS].[TINWSYS]") #does it need to have any other variables
cat("The SDWIS TINWSYS dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tinwsys), "water systems.\n")
#The SDWIS TINWSYS dataset queried 08/18/2025 contains 15956 water systems.


# Query Facility List from SDWIS TINWSF 
tinwsf <- dbGetQuery(con, "SELECT 
                        TINWSF_IS_NUMBER,
                        ACTIVITY_STATUS_CD,
                        ST_ASGN_IDENT_CD,
                        NAME,
                        TYPE_CODE,
                        WATER_TYPE_CODE,
                        TINWSYS_IS_NUMBER
                    FROM [ReportDB].[SDWIS].[TINWSF]") 
cat("The SDWIS TINWSF dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tinwsf), "water systems facilities.\n")
#The SDWIS TINWSF dataset queried 08/18/2025 contains 70342 water system facilities.

# Query Sample Point List from SDWIS TSASMPPT 
tsasmppt <- dbGetQuery(con, "SELECT	
                          TSASMPPT_IS_NUMBER,
                          ACTIVITY_STATUS_CD,
                          IDENTIFICATION_CD,
                          DESCRIPTION_TEXT,
                          TINWSF0IS_NUMBER
                    FROM [ReportDB].[SDWIS].[TSASMPPT]") 
cat("The SDWIS TSASMPPT dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tsasmppt), "sample points.\n")
#The SDWIS TSASMPPT dataset queried 08/18/2025 contains 67659 sample points.

# Query Sample Results from SDWIS TSASAMPL [SAMPLES]
tsasampl <- dbGetQuery(con, "SELECT 
                          TSASAMPL_IS_NUMBER, 
                          COLLLECTION_END_DT, 
                          COLLCTN_END_TIME, 
                          TYPE_CODE, 
                          COMPL_PURP_IND_CD, 
                          TSASMPPT_IS_NUMBER
                    FROM [ReportDB].[SDWIS].[TSASAMPL]") 
cat("The SDWIS TSASAMPL dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tsasampl), "sample results.\n")
#The SDWIS TSASAMPL dataset queried 08/18/2025 contains 4576439 sample results.


# Query Sample Analytical Results from SDWIS TSASAR [RESULTS]
start_time <- Sys.time() # Record the start time
tsasar <- dbGetQuery(con, "SELECT 
                        r.TSASAMPL_IS_NUMBER, 
                        r.REPORTED_MSR, 
                        r.CONCENTRATION_MSR, 
                        r.UOM_CODE, 
                        r.TSAANLYT_IS_NUMBER, 
                        r.TSAANLYT_ST_CODE,
                        r.DATA_QTY_RSN_CD,
                        r.D_INITIAL_USERID,
                        r.LESS_THAN_IND,
                        s.COLLLECTION_END_DT
                    FROM [ReportDB].[SDWIS].[TSASAR] r
                    INNER JOIN [ReportDB].[SDWIS].[TSASAMPL] s
                    ON r.TSASAMPL_IS_NUMBER = s.TSASAMPL_IS_NUMBER
                    WHERE CAST(s.COLLLECTION_END_DT AS DATE) >= CAST(CONCAT(YEAR(GETDATE()) - 5, '-01-01') AS DATE)")
end_time <- Sys.time() # Record the end time
execution_time <- end_time - start_time
print(paste("Query execution time:", execution_time))
cat("The SDWIS TSASAR dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tsasar), "sample analytical results.\n")


# start_time2 <- Sys.time() # Record the start time
# tsasar2 <- dbGetQuery(con, "SELECT 
#                         TSASAMPL_IS_NUMBER, 
#                         REPORTED_MSR, 
#                         CONCENTRATION_MSR, 
#                         UOM_CODE, 
#                         TSAANLYT_IS_NUMBER, 
#                         TSAANLYT_ST_CODE,
#                         DATA_QTY_RSN_CD,
#                         D_INITIAL_USERID,
#                         LESS_THAN_IND
#                     FROM [ReportDB].[SDWIS].[TSASAR]") 
# end_time2 <- Sys.time() # Record the end time
# 
# # Calculate and print the duration
# execution_time2 <- end_time2 - start_time2
# print(paste("Query execution time:", execution_time2))

# Query Analyte List from SDWIS TSAANLYT
tsaanlyt <- dbGetQuery(con, "SELECT 
                          NAME, 
                          TSAANLYT_IS_NUMBER,
                          TSAANLYT_ST_CODE,
                          CODE
                       FROM [ReportDB].[SDWIS].[TSAANLYT]") 
cat("The SDWIS TSAANLYT dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tsaanlyt), "analytes.\n")
#The SDWIS TSAANLYT dataset queried 08/18/2025 contains 1059 analytes.



# Query MCL from SDWIS TMNALRA (Analyte Level Rule Assessment) #MCLs are not kept up to date in SDWIS
tmnalra <- dbGetQuery(con, "SELECT
                          TSAANLYT_IS_NUMBER,
                          TSAANLYT_ST_CODE,
                          MEASURE,
                          MEASURE_TEXT,
                          UOM_CODE,
                          THRESHOLD_TYPE_CD,
                          MSR_LEVEL_TYPE_CD,
                          BEGIN_DATE,
                          END_DATE
                       FROM [ReportDB].[SDWIS].[TMNALRA]") #although not up to date, use as a scaffold for the UoM
cat("The SDWIS TMNALRA dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tmnalra), "analyte level rule assessment.\n")
#The SDWIS TMNALRA dataset queried 08/18/2025 contains 414 analyte level rule assessment.


#### TABLE 1: Generate the water system of interest	####

water_system_inventory <- tinwsys %>% 
  
  # Remove any trailing or leading white space from NUMBER0 field
  mutate(NUMBER0 = str_trim(NUMBER0)) %>% 
  
  # Select water system of interest
  filter(NUMBER0 == water_system_of_interest) %>%
  
  # Rename columns (align's with DL's SQL code)
  rename(
    PWSID = NUMBER0, 
    WATER_SYSTEM_NAME = NAME, 
    WATER_SYSTEM_STATUS = ACTIVITY_STATUS_CD,
    POPULATION = D_POPULATION_COUNT,
    FEDERAL_WATER_SYSTEM_TYPE = D_PWS_FED_TYPE_CD
  ) %>%
  
  # Change A --> ACTIVE and otherwise make it N/A in WATER_SYSTEM_STATUS field
  mutate(WATER_SYSTEM_STATUS = ifelse(WATER_SYSTEM_STATUS == "A", "ACTIVE", NA))


#### TABLE 2: Generates the facilities of interest ####

water_system_sources <- tinwsf %>%
  filter(
    TYPE_CODE %in% c("CC", "IG", "IN", "RC", "SP", "WL"),  # Filter by TYPE_CODE (should we start with sources generally?)
    ACTIVITY_STATUS_CD == "A"  # Filter by ACTIVITY_STATUS_CD
  ) %>%
  rename(
    FACILITY_ID = ST_ASGN_IDENT_CD,
    FACILITY_NAME = NAME,
    FACILITY_TYPE = TYPE_CODE,
    FACILITY_STATUS = ACTIVITY_STATUS_CD,
    FACILITY_WATER_TYPE_CODE = WATER_TYPE_CODE
  ) %>%
  mutate(FACILITY_STATUS = ifelse(FACILITY_STATUS == "A", "ACTIVE", NA))


#### TABLE 3: Generates the sample points ####

sample_points <- tsasmppt %>%
  filter(
    ACTIVITY_STATUS_CD == "A"  # Filter by ACTIVITY_STATUS_CD
  ) %>%
  rename(
    SAMPLE_POINT_ID = IDENTIFICATION_CD,  # Rename variable
    SAMPLE_POINT_NAME = DESCRIPTION_TEXT,  # Rename variable
    SAMPLE_POINT_STATUS = ACTIVITY_STATUS_CD,
    TINWSF_IS_NUMBER = TINWSF0IS_NUMBER
  ) %>%
  mutate(SAMPLE_POINT_STATUS = ifelse(SAMPLE_POINT_STATUS == "A", "ACTIVE", NA))


#### TABLE 4: Generates the samples ####

samples <- tsasampl %>% filter(COMPL_PURP_IND_CD == "Y", #keep only samples for compliance purposes
                               !TYPE_CODE == "FB", #drop any remaining field blank samples #doesn't actually do anything, assuming compliance purpose filter works correctly
                               COLLLECTION_END_DT >= as.Date( #filter samples to those collected after a specific date
                                 paste0(as.numeric(format(Sys.Date(), "%Y")) - 5, "-01-01"))) %>% #sets date to filter until (5 years from date/year code is ran, start 1/1 of that year)
  rename(SAMPLE_DATE = COLLLECTION_END_DT) 

#### TABLE 5: Generates the sampling results data (run this after running samples) ####
results <- tsasar %>% rename(RESULT = CONCENTRATION_MSR, RESULT_REPORTING_UNIT = UOM_CODE) %>%
  filter(str_detect(D_INITIAL_USERID, "ADMIN|FINDING|CLIP1\\.0"),
         DATA_QTY_RSN_CD == "  ") %>% #3 spaces is how it reads in R but also could maybe just str_detect 2 spaces?
  left_join(samples %>% select(TSASAMPL_IS_NUMBER, TSASMPPT_IS_NUMBER)) %>% 
  left_join(sample_points %>% select(TSASMPPT_IS_NUMBER, 	TINWSF_IS_NUMBER)) %>% 
  left_join(water_system_sources %>% select(TINWSF_IS_NUMBER, TINWSYS_IS_NUMBER, FACILITY_ID)) %>%
  left_join(tinwsys %>% 
              mutate(NUMBER0 = str_trim(NUMBER0)) %>% # Remove any trailing or leading white space
              filter(ACTIVITY_STATUS_CD == "A") %>%
              rename(PWSID = NUMBER0) %>% 
              select(PWSID, TINWSYS_IS_NUMBER)) %>%
  group_by(PWSID, FACILITY_ID, TSAANLYT_IS_NUMBER) %>%
  mutate(RESULT_SUM = sum(RESULT, na.rm = TRUE)) %>% #here's where we need to push, sum together the result for each facility + analyte
  ungroup() %>% 
  dplyr::select(!c("PWSID", "FACILITY_ID", "TINWSYS_IS_NUMBER", "TINWSF_IS_NUMBER", "TSASMPPT_IS_NUMBER"))


#### TABLE 6: Generate the analyte list and date list ####

analyte <- tsaanlyt %>% rename(ANALYTE_NAME = NAME, ANALYTE_CODE = CODE)

# Generate DATE_RANGE equivalent in R (covering the last 5 years)
date_range <- tibble(
  dtDate = seq.Date(
    from = as.Date(paste0(year(Sys.Date()) - 5, "-01-01")),
    to = Sys.Date(),
    by = "day"
  )
) %>%
  mutate(
    QUARTER = paste0(year(dtDate), "-", quarter(dtDate)) # Create Year-Quarter string
  ) %>%
  distinct(QUARTER) # Get distinct QUARTER values

#Barium
barium <- c("1010", "1.", 1) #1 significant figure - join this with analyte dataframe
arsenic <- c("1005", "0.010", 2) #2 significant figures - join this with analyte dataframe
hexchrom <- c("1080", "0.010", 2) #2 significant figures - join this with analyte dataframe
mcl_list <- rbind(barium, arsenic, hexchrom) %>% as.data.frame() %>% rename(ANALYTE_CODE = 1, MCL = 2, SF = 3) %>% mutate(SF = as.numeric(SF))

analyte_mcl <- analyte %>% left_join(mcl_list)

#### TABLE 7: Generate the water quality well ####

water_quality_well <- water_system_inventory %>% 
  left_join(water_system_sources) %>% 
  left_join(sample_points) %>% 
  left_join(samples) %>%
  left_join(results) %>%
  left_join(analyte_mcl) 

#### TABLE 8: Generate the quarter table ####

# Define a custom rounding function for significant figures
round_to_sf <- function(value, sig_figs) {
  if (is.na(value) || is.na(sig_figs) || sig_figs <= 0) {
    return(NA)
  }
  round(value, digits = sig_figs - ceiling(log10(abs(value))))
}

# Create quarter table from water quality well
quarter_table <- water_quality_well %>%
  # Filter rows based on conditions
  filter(
    RESULT_SUM > 0,
    FACILITY_ID == facility_of_interest,
    ANALYTE_CODE == analyte_of_interest
  ) %>%
  # Add new calculated columns
  mutate(
    FACILITY_ID = paste0(trimws(PWSID), "-", FACILITY_ID), # Combine PWSID and FACILITY_ID
    QUARTER = paste0(year(SAMPLE_DATE), "-", quarter(SAMPLE_DATE)) # Calculate quarter and year
  ) %>%
  # Group by the necessary columns
  group_by(
    PWSID,
    POPULATION,
    FACILITY_ID,
    ANALYTE_NAME,
    ANALYTE_CODE,
    QUARTER
  ) %>%
  # Summarize data within each group
  summarise(
    QUARTER_MEAN_sf = round_to_sf(mean(RESULT, na.rm = TRUE), first(SF)), # Use first(SF) to get SF value per group
    QUARTER_MEAN = mean(RESULT, na.rm = TRUE), # Average of RESULT.... here's where we get into tricky waters... basically I need to make the rounding function part of the table so it can know what to round to....
    QUARTER_MEAN_COUNT = n() # Count of rows in each group
  ) %>%
  ungroup() %>% # Ungroup the data to avoid issues with further processing
  mutate(QUARTER_MEAN_FINAL = ifelse(QUARTER_MEAN_COUNT > 1, QUARTER_MEAN_sf, QUARTER_MEAN)) #check this decision w/ KN

#### TABLE 9: Generate the quarter table pre ####

# Left join the expanded table with the original data to "fill in" missing rows
quarter_table_pre <- quarter_table %>%
  distinct(PWSID, POPULATION, FACILITY_ID, ANALYTE_NAME, ANALYTE_CODE) %>% # Get unique combinations
  crossing(date_range) %>% # Add all QUARTER values from `date_range`
  left_join(quarter_table, by = c("PWSID", "POPULATION", "FACILITY_ID", "ANALYTE_NAME", "ANALYTE_CODE", "QUARTER")) %>%
  mutate(
    QUARTER_MEAN_FINAL = coalesce(QUARTER_MEAN_FINAL, NA_real_), # Fill missing QUARTER_MEAN with NA
    QUARTER_MEAN_COUNT = coalesce(QUARTER_MEAN_COUNT, NA_real_) # Fill missing QUARTER_MEAN_COUNT with NA
  ) %>%
  select(PWSID, POPULATION, FACILITY_ID, ANALYTE_NAME, ANALYTE_CODE, QUARTER, QUARTER_MEAN_FINAL, QUARTER_MEAN_COUNT) %>%
  group_by(FACILITY_ID, ANALYTE_NAME, ANALYTE_CODE) %>%
  arrange(QUARTER, .by_group = TRUE) %>% # Sort within each group by QUARTER
  mutate(RN = row_number()) %>%          # Add a row number for each group
  ungroup()                              # Ungroup after the operation

max_row_number <- quarter_table_pre %>% group_by(FACILITY_ID, ANALYTE_NAME) %>%
  summarize(MAX_RN = max(RN), .groups = "drop")

#### TABLE 10: Generate the cacl raa table ####

calc_raa <- quarter_table_pre %>%
  left_join(analyte_mcl %>% select(ANALYTE_CODE, SF)) %>%
  inner_join(max_row_number, by = c("FACILITY_ID", "ANALYTE_NAME")) %>%
  group_by(FACILITY_ID, ANALYTE_NAME) %>%
  arrange(FACILITY_ID, ANALYTE_NAME, QUARTER) %>% # Ensure proper ordering
  mutate(
    # Rolling Average (RAA): Use `.complete = FALSE` to allow calculations before a full window
    RAA = slide_dbl(QUARTER_MEAN_FINAL, mean, .before = 3, .complete = T, na.rm = TRUE),
    # Rolling Count (RAA_COUNT): Same logic for counting non-NA values
    RAA_COUNT = slide_int(!is.na(QUARTER_MEAN_FINAL), sum, .before = 3, .complete = T),
  ) %>%
  ungroup() %>%
  rowwise() %>%
  mutate(
    # Apply significant figures rounding to RAA
    RAA = if_else(!is.na(SF), round_to_sf(RAA, SF), RAA)
  )

#### TABLE 11: Find the most recent quartet with RAA ####

qtr_result <-  calc_raa %>%
  mutate(QUARTER_SORT = as.numeric(gsub("-", ".", QUARTER))) %>%  # Convert QUARTER to numeric for sorting
  arrange(desc(QUARTER_SORT)) %>%
  filter(!is.na(QUARTER_MEAN_FINAL)) %>%
  ungroup() %>%
  slice(1) %>%
  select(-QUARTER_SORT) %>% # Remove the temporary column 
  left_join(analyte_mcl %>% 
              dplyr::select(TSAANLYT_IS_NUMBER, ANALYTE_CODE, MCL)) %>% 
  mutate(TSAANLYT_IS_NUMBER =  str_trim(TSAANLYT_IS_NUMBER),
         TSAANLYT_IS_NUMBER = as.numeric((TSAANLYT_IS_NUMBER))) %>%
  left_join(tmnalra %>% 
              mutate(THRESHOLD_TYPE_CD = str_trim(THRESHOLD_TYPE_CD)) %>%
              filter(THRESHOLD_TYPE_CD == "MCL" )%>% 
              dplyr::select(TSAANLYT_IS_NUMBER, UOM_MCL = UOM_CODE)) %>%
  left_join(water_quality_well %>% dplyr::select(TSAANLYT_IS_NUMBER, RESULT_REPORTING_UNIT))
  mutate(MCL_num = as.numeric(MCL), exceed = ifelse(RAA > MCL_num, 1, 0))

