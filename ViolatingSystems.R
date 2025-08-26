### Detect Violating WS V2.0 ###
# Author: Rachel Skillman
# Date Created: 8/18/25
# Date Updated: 8/26/25

#TO DO:
#   - Get UoM from TMNALRA even though MCL is not kept up to date
# - Need to find a system exceeding for more than one analyte to test - use
# CA0400021 Robinson's Corner MHP - Only Well Facility (001) - and manganese and nitrate
# - Could try and use that table to see if you can get more stuff out of it? like the MCL so you don't have to do it manually 

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

# water_system_of_interest <- "CA0110003"
# facility_of_interest <- "014"

water_system_of_interest <- "CA0400021"
facility_of_interest <- "001"
primary_analyte_of_interest  <- c("1002", #Aluminum 1. mg/L [1 sf]
                                  "1074", #Antimony 0.006 mg/L [1 sf]
                                  "1005", #Arsenic 0.010 mg/L [2 sf]
                                  "1094", #Asbestos 7 MFL [1 sf]
                                  "1010", #Barium 1. mg/L [1 sf]
                                  "1075", #Beryllium 0.004 mg/L [1 sf]
                                  "1015", #Cadmium 0.005 mg/L [1 sf]
                                  "1080", #Chromium (hexavalent) 0.010 mg/L [2 sf]
                                  "1020", #Chromium (total) 0.05 mg/L [1 sf]
                                  "1024", #Cyanide 0.15 mg/L [2 sf]
                                  "1025", #Fluoride 2.0 mg/L [2 sf] ,
                                  "1035", #Mercury 0.002 mg/L [1 sf]
                                  "1036", #Nickel 0.1 mg/L [1 sf]
                                  "1040", #Nitrate 10. mg/L [1 sf]
                                  "1038", #Nitrate + Nitrite 10. mg/L [1 sf]
                                  "1041", #Nitrite 1. mg/L [1 sf]
                                  "1039", #Perchlorate 0.006 mg/L [1 sf]
                                  "1045", #Selenium 0.05 mg/L [1 sf]
                                  "1085") #Thallium 0.002 mg/L [1 sf]

secondary_analyte_of_interest  <- c("1032") #Manganese

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
#The SDWIS TINWSYS dataset queried 08/19/2025 contains 15956 water systems.
#The SDWIS TINWSYS dataset queried 08/25/2025 contains 15957 water systems.

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
#The SDWIS TINWSF dataset queried 08/19/2025 contains 70342 water system facilities.
#The SDWIS TINWSF dataset queried 08/25/2025 contains 70347 water systems facilities.

# Query Facility List from SDWIS TINWSF 
tinwsff <- dbGetQuery(con, "SELECT *
                    FROM [ReportDB].[SDWIS].[TINWSFF]") 
cat("The SDWIS TINWSFF dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tinwsff), "water systems facilities flow paths.\n")
#The SDWIS TINWSFF dataset queried 08/26/2025 contains 30442 water systems facilities flow paths.

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
#The SDWIS TSASMPPT dataset queried 08/19/2025 contains 67663 sample points.
#The SDWIS TSASMPPT dataset queried 08/25/2025 contains 67670 sample points.


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
#The SDWIS TSASAMPL dataset queried 08/19/2025 contains 4577219 sample results.
#The SDWIS TSASAMPL dataset queried 08/25/2025 contains 4579967 sample results.

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
print(paste("Query execution time:", execution_time)) #12.38 minutes
cat("The SDWIS TSASAR dataset queried", format(Sys.Date(), "%m/%d/%Y"), "contains", nrow(tsasar), "sample analytical results.\n")
#The SDWIS TSASAR dataset queried 08/19/2025 contains 11531655 sample analytical results.
#The SDWIS TSASAR dataset queried 08/25/2025 contains 11557152 sample analytical results.

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
#The SDWIS TSAANLYT dataset queried 08/25/2025 contains 1059 analytes.



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
#The SDWIS TMNALRA dataset queried 08/25/2025 contains 414 analyte level rule assessment.

#### Function to round for significant figures ####

# Define a custom rounding function for significant figures
round_to_sf <- function(value, sig_figs) {
  if (is.na(value) || is.na(sig_figs) || sig_figs <= 0) {
    return(NA)
  }
  round(value, digits = sig_figs - ceiling(log10(abs(value))))
}

#### Function to convert UOM ####

convert_units <- function(value, from, to) {
  # standardize text
  from <- toupper(trimws(from))
  to   <- toupper(trimws(to))
  
  # no conversion needed
  if (from == to) return(value)
  
  # mg/L ↔ ug/L
  if (from == "MG/L" && to == "UG/L") return(value * 1000)
  if (from == "UG/L" && to == "MG/L") return(value / 1000)
  
  # ug/L ↔ ng/L
  if (from == "UG/L" && to == "NG/L") return(value * 1000)
  if (from == "NG/L" && to == "UG/L") return(value / 1000)
  
  # mg/L ↔ ng/L
  if (from == "MG/L" && to == "NG/L") return(value * 1e6)
  if (from == "NG/L" && to == "MG/L") return(value / 1e6)
  
  stop(paste("Conversion not implemented for", from, "to", to))
}


#### TABLE 1: Generate the analyte list and date list ####

analyte <- tsaanlyt %>% rename(ANALYTE_NAME = NAME, ANALYTE_CODE = CODE)

aluminum <- c("1002", "1.", 1) #1 significant figure - join this with analyte dataframe
antimony <- c("1074", "0.006", 1) #1 significant figure - join this with analyte dataframe
arsenic <- c("1005", "0.010", 2) #2 significant figures - join this with analyte dataframe
asbestos <- c("1094", "7", 1) #2 significant figures - join this with analyte dataframe
barium <- c("1010", "1.", 1) #1 significant figure - join this with analyte dataframe
beryllium <- c("1075", "0.004", 1) #1 significant figure - join this with analyte dataframe
cadmium <- c("1015", "0.005", 1) #1 significant figure - join this with analyte dataframe
hexchrom <- c("1080", "0.010", 2) #2 significant figures - join this with analyte dataframe
chromium <- c("1020", "0.05", 1) #1 significant figure - join this with analyte dataframe
cyanide <- c("1024", "0.15", 2) #2 significant figures - join this with analyte dataframe
fluoride <- c("1025", "2.0", 2) #2 significant figures - join this with analyte dataframe
mercury <- c("1035", "0.002", 1) #1 significant figure - join this with analyte dataframe
nickel <- c("1036", "0.1", 1) #1 significant figure - join this with analyte dataframe
nitrate <- c("1040", "10", 1) #1 significant figure - join this with analyte dataframe
nitrite <- c("1041", "1.", 1) #1 significant figure - join this with analyte dataframe
nitratenitrite <- c("1038", "10.", 1) #1 significant figure - join this with analyte dataframe
perchlorate <- c("1039", "0.006", 1) #1 significant figure - join this with analyte dataframe
selenium <- c("1045", "0.05", 1) #1 significant figure - join this with analyte dataframe
thallium <- c("1085", "0.002", 1) #1 significant figure - join this with analyte dataframe

#manganese <- c("1032", "0.05", 1) #1 significant figure - join this with analyte dataframe

mcl_list <- rbind(aluminum, 
                  antimony, 
                  arsenic, 
                  asbestos,
                  barium, 
                  beryllium,
                  cadmium,
                  hexchrom, 
                  chromium,
                  cyanide,
                  fluoride,
                  mercury,
                  nickel, 
                  nitrate,
                  nitrite,
                  nitratenitrite,
                  perchlorate,
                  selenium,
                  thallium) %>% 
  as.data.frame() %>% 
  rename(ANALYTE_CODE = 1, MCL = 2, SF = 3) %>% 
  mutate(SF = as.numeric(SF))

analyte_mcl <- analyte %>% left_join(mcl_list)

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


#### TABLE 2: Count WS Facilities ####

active_facilities_count <- tinwsf %>% 
  mutate(WATER_TYPE_CODE = str_trim(WATER_TYPE_CODE)) %>% # Remove any trailing or leading from WATER_TYPE_CODE
  inner_join(tinwsys %>% 
               
               # Remove any trailing or leading white space from NUMBER0 field
               mutate(NUMBER0 = str_trim(NUMBER0),
                      D_PWS_FED_TYPE_CD = str_trim(D_PWS_FED_TYPE_CD)) %>% 
               
               # Select water system of interest
               #filter(NUMBER0 == water_system_of_interest) %>%
               
               # Rename columns (aligns with DL's SQL code)
               rename(
                 PWSID = NUMBER0, 
                 WATER_SYSTEM_NAME = NAME, 
                 WATER_SYSTEM_STATUS = ACTIVITY_STATUS_CD,
                 POPULATION = D_POPULATION_COUNT,
                 FEDERAL_WATER_SYSTEM_TYPE = D_PWS_FED_TYPE_CD
               ) %>%
               
               # Change A --> ACTIVE and otherwise make it N/A in WATER_SYSTEM_STATUS field
               mutate(WATER_SYSTEM_STATUS = ifelse(WATER_SYSTEM_STATUS == "A", "ACTIVE", NA)) %>%
               
               #just for fun, filter to active community WS only so it's easier to try and expand
               filter(FEDERAL_WATER_SYSTEM_TYPE == "C",
                      WATER_SYSTEM_STATUS == "ACTIVE") #2822 if filtered to active CWS 
             ) %>%
  filter(ACTIVITY_STATUS_CD == "A") %>% #active facilities
  group_by(PWSID, TINWSYS_IS_NUMBER) %>%
  summarise(
    n_active_facilities = n(),
    all_type_codes = paste(unique(TYPE_CODE), collapse = ", "), # concatenate unique TYPE_CODEs
    .groups = "drop"
  ) %>%
  mutate(
    no_TP = if_else(str_detect(all_type_codes, "TP"), 0L, 1L) # dummy: 1 if TP is missing
  )

# Find PWSIDs 
dswl <- active_facilities_count %>%
  filter(all_type_codes %in% c("DS, WL", "WL, DS")) %>% #even if you have multiple wells, it combines into one - at this point what if you just looked at any system with no TP?
  pull(PWSID)
notp <- active_facilities_count %>%
  filter(no_TP == 1) %>% 
  pull(PWSID)

#### TABLE 3: Generate water systems of interest with no TP	####

water_system_inventory_notp <- tinwsys %>% 
  
  # Remove any trailing or leading white space from NUMBER0 field
  mutate(NUMBER0 = str_trim(NUMBER0),
         D_PWS_FED_TYPE_CD = str_trim(D_PWS_FED_TYPE_CD)) %>% 
  
  # Select water system of interest
  #filter(NUMBER0 %in% dswl) %>%
  filter(NUMBER0 %in% notp) %>%
  
  
  # Rename columns (aligns with DL's SQL code)
  rename(
    PWSID = NUMBER0, 
    WATER_SYSTEM_NAME = NAME, 
    WATER_SYSTEM_STATUS = ACTIVITY_STATUS_CD,
    POPULATION = D_POPULATION_COUNT,
    FEDERAL_WATER_SYSTEM_TYPE = D_PWS_FED_TYPE_CD
  ) %>%
  
  # Change A --> ACTIVE and otherwise make it N/A in WATER_SYSTEM_STATUS field
  mutate(WATER_SYSTEM_STATUS = ifelse(WATER_SYSTEM_STATUS == "A", "ACTIVE", NA)) %>%
  
  #just for fun, filter to active community WS only so it's easier to try and expand
  filter(FEDERAL_WATER_SYSTEM_TYPE == "C",
         WATER_SYSTEM_STATUS == "ACTIVE")


#### TABLE 4: Generates the facilities of interest ####

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
#Also use D_SOURCE_CD  - do it two different ways to see what happens
#remember the wells that are not sources

#### TABLE 5: Generates "Starting Point" sources ####

startpts <- water_system_sources %>% 
  left_join(tinwsff %>% dplyr::select(TINWSF_IS_NUMBER, TINWSF0IS_NUMBER)) %>%
  filter(is.na(TINWSF0IS_NUMBER)) %>%
  dplyr::select(!c(TINWSF0IS_NUMBER))


#### TABLE 6: Generate flow path ####

# build graph from edges
g <- igraph::graph_from_data_frame(
  d = tinwsff %>% select(from = TINWSF0IS_NUMBER, to = TINWSF_IS_NUMBER),
  vertices = tinwsf %>% select(TINWSF_IS_NUMBER, TINWSYS_IS_NUMBER),
  directed = TRUE
)

# find sources = nodes with no incoming edges
sources <- igraph::V(g)[igraph::degree(g, mode = "in") == 0]

# function to get paths from one source
get_paths <- function(source) {
  # shortest_paths will actually give *all* simple paths to sinks
  sinks <- igraph::V(g)[igraph::degree(g, mode = "out") == 0]
  all_paths <- all_simple_paths(igraph::g, from = source, to = sinks)
  
  # turn each path into a data frame
  df_list <- lapply(seq_along(all_paths), function(i) {
    tibble(
      Path_ID = paste0(source$name, "_", i),
      Step = seq_along(all_paths[[i]]),
      Facility_ID = names(all_paths[[i]])
    )
  })
  bind_rows(df_list)
}

# apply to all sources
flow_paths <- purrr::map_df(sources, get_paths)

# join with additional water system information
flow_path_pwsid <- flow_paths %>% 
  mutate(TINWSF_IS_NUMBER = as.numeric(Facility_ID)) %>%
  left_join(tinwsf)

#detach("package:igraph", unload = TRUE)

#### TABLE 7: Generates the sample points ####

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


#### TABLE 8: Generates the samples ####

samples <- tsasampl %>% filter(COMPL_PURP_IND_CD == "Y", #keep only samples for compliance purposes
                               !TYPE_CODE == "FB", #drop any remaining field blank samples #doesn't actually do anything, assuming compliance purpose filter works correctly
                               COLLLECTION_END_DT >= as.Date( #filter samples to those collected after a specific date
                                 paste0(as.numeric(format(Sys.Date(), "%Y")) - 5, "-01-01"))) %>% #sets date to filter until (5 years from date/year code is ran, start 1/1 of that year)
  rename(SAMPLE_DATE = COLLLECTION_END_DT) 

#### TABLE 9: Generates the sampling results data (must run after samples) ####

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


#### TABLE 10: Generate the water quality well ####

water_quality_well <- water_system_inventory_notp %>% 
  #left_join(water_system_sources) %>% 
  left_join(startpts) %>%
  left_join(sample_points) %>% 
  left_join(samples) %>%
  left_join(results) %>%
  left_join(analyte_mcl) 

#### TABLE 11: Generate the quarter table ####

# Create quarter table from water quality well
quarter_table <- water_quality_well %>%
  # Filter rows based on conditions
  filter(
    RESULT_SUM > 0,
    FACILITY_ID == facility_of_interest,
    ANALYTE_CODE %in% primary_analyte_of_interest
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

#### TABLE 12: Generate the quarter table pre ####

quarter_table_pre <- quarter_table %>%
  distinct(PWSID, POPULATION, FACILITY_ID, ANALYTE_NAME, ANALYTE_CODE) %>% # Get unique combinations
  crossing(date_range) %>% # Add all QUARTER values from `date_range`
  left_join(quarter_table, by = c("PWSID", "POPULATION", "FACILITY_ID", "ANALYTE_NAME", "ANALYTE_CODE", "QUARTER")) %>%
  mutate(
    QUARTER_MEAN_FINAL = coalesce(QUARTER_MEAN_FINAL, NA_real_), # Fill missing QUARTER_MEAN with NA
    QUARTER_MEAN_COUNT = coalesce(QUARTER_MEAN_COUNT, NA_real_) # Fill missing QUARTER_MEAN_COUNT with NA
  ) %>%
  dplyr::select(PWSID, POPULATION, FACILITY_ID, ANALYTE_NAME, ANALYTE_CODE, QUARTER, QUARTER_MEAN_FINAL, QUARTER_MEAN_COUNT) %>%
  group_by(FACILITY_ID, ANALYTE_NAME, ANALYTE_CODE) %>%
  arrange(QUARTER, .by_group = TRUE) %>% # Sort within each group by QUARTER
  dplyr::mutate(RN = row_number()) %>%          # Add a row number for each group
  ungroup()                              # Ungroup after the operation

max_row_number <- quarter_table_pre %>% group_by(FACILITY_ID, ANALYTE_NAME) %>%
  summarize(MAX_RN = max(RN), .groups = "drop")

#### TABLE 13: Generate the RAA table for each quarter ####

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

#### TABLE 14: Generate most recent quarter with RAA for each facility and analyte ####

qtr_result <-  calc_raa %>%
  mutate(QUARTER_SORT = as.numeric(gsub("-", ".", QUARTER))) %>%  # Convert QUARTER to numeric for sorting
  arrange(desc(QUARTER_SORT)) %>%
  filter(!is.na(QUARTER_MEAN_FINAL)) %>%
  ungroup() %>%
  group_by(ANALYTE_CODE, FACILITY_ID) %>% #THIS WAS A BIG CHANGE - did you do it correctly?
  slice(1) %>% #somehow you need to know how many to slice (or like sort by analyte and always just grab the earliest result?)
  select(-QUARTER_SORT) %>% # Remove the temporary column 
  left_join(analyte_mcl %>% 
              dplyr::select(TSAANLYT_IS_NUMBER, ANALYTE_CODE, MCL)) %>% 
  mutate(TSAANLYT_IS_NUMBER =  str_trim(TSAANLYT_IS_NUMBER),
         TSAANLYT_IS_NUMBER = as.numeric((TSAANLYT_IS_NUMBER))) %>%
  left_join(tmnalra %>% 
              mutate(THRESHOLD_TYPE_CD = str_trim(THRESHOLD_TYPE_CD)) %>%
              filter(THRESHOLD_TYPE_CD == "MCL" ) %>%  
              group_by(TSAANLYT_IS_NUMBER) %>% # group by analyte
              slice_max(order_by = BEGIN_DATE, n = 1, with_ties = FALSE) %>% #if you use TMNALRA for MCL/SF --> make sure you take the latest date
              dplyr::select(TSAANLYT_IS_NUMBER, UOM_MCL = UOM_CODE)) %>%
  left_join(water_quality_well %>% dplyr::select(TSAANLYT_IS_NUMBER, RESULT_REPORTING_UNIT)  %>%
              filter(str_trim(RESULT_REPORTING_UNIT) != "") %>% 
              distinct()) #why are there 17? aren't there like 599 water systems? Maybe most of them are doing okay?


  
#### TABLE 15: Convert MCL to correct UOM ####

qtr_result_uom <- qtr_result %>% mutate(MCL_NUM = as.numeric(MCL)) #I'm definitely messing up SF here - when to convert? But once you get it right it should be fine
qtr_result_uom$MCL_converted <- mapply(convert_units, 
                           value = qtr_result_uom$MCL_NUM, 
                           from  = qtr_result_uom$UOM_MCL, 
                           to    = qtr_result_uom$RESULT_REPORTING_UNIT)

#### TABLE 16: Generate violation flags ####

qtr_result_violation <- qtr_result_uom %>% 
  mutate(exceed = ifelse(RAA > MCL_converted, 1, 0)) #seems to be working reasonably well 
