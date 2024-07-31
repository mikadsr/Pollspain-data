# Load required libraries
library(tidyverse)
library(glue)
library(lubridate)
library(stringr)
library(httr)
##############get_mun_census_data ----####################################################

## Correct examples ----

# Congress elections in April 2019
# Fetch municipal census data for the congress elections in April 2019
mun_census_data1 <- get_mun_census_data("congress", 2019, 4)

# Senate elections in April 2019
# Fetch municipal census data for the senate elections in April 2019
mun_census_data2 <- get_mun_census_data("senate", 2019, 4)

# Example usage to combine data from different elections into one table
# Fetch municipal census data for congress elections in Nov 2019, April 2019, and June 2016
combined_mun_census_data <- get_mun_census_data(
  c("congress", "congress", "congress"), 
  c(2019, 2019, 2016), 
  c(11, 4, 6)
)

## Incorrect examples ----

# Invalid election type
# This will fail because "national" is not a valid election type
mun_census_data5 <- get_mun_census_data("national", 2019, 4) 

# Length mismatch between year and month vectors
# This will fail because the length of the year and month vectors do not match
mun_census_data6 <- get_mun_census_data("congress", 2016, c(4, 11)) 

# Invalid date format
# This will fail because the date should be split into year and month
mun_census_data7 <- get_mun_census_data("congress", "2016-06-26") 

# Non-existent election data
# This will fail because 1990 congress elections are not available
mun_census_data8 <- get_mun_census_data("congress", 1990, 4) 


############## get_poll_station_data -----####################################################
## Correct examples ----

# Congress elections in April 2019
# Fetch poll station data for the congress elections in April 2019
poll_station_data1 <- get_poll_station_data("congress", 2019, 4)

# Senate elections in April 2019
# Fetch poll station data for the senate elections in April 2019
poll_station_data2 <- get_poll_station_data("senate", 2019, 4)

# Example usage to combine data from different elections into one table
# Fetch poll station data for congress elections in Nov 2019, April 2019, and June 2016
combined_poll_station_data <- get_poll_station_data(
  c("congress", "congress", "congress"), 
  c(2019, 2019, 2016), 
  c(11, 4, 6)
)

## Incorrect examples ----

# Invalid election type
# This will fail because "national" is not a valid election type
poll_station_data5 <- get_poll_station_data("national", 2019, 4)

# Length mismatch between year and month vectors
# This will fail because the length of the year and month vectors do not match
poll_station_data6 <- get_poll_station_data("congress", 2016, c(4, 11))

# Invalid date format
# This will fail because the date should be split into year and month
poll_station_data7 <- get_poll_station_data("congress", "2016-06-26")

# Non-existent election data
# This will fail because 1990 congress elections are not available
poll_station_data8 <- get_poll_station_data("congress", 1990, 4)

############## get_candidates_data -----####################################################
##Correct examples
# Get candidates data for congress elections in March 1996
candidates_data_1996_03 <- get_candidates_data("congress", 1996, 3)
print(candidates_data_1996_03)

# Get candidates data for congress elections in April 2019
candidates_data_2019_04 <- get_candidates_data("congress", 2019, 4)
print(candidates_data_2019_04)

##Wrong examples
# Trying to get candidates data for non-congress elections
try(get_candidates_data("senate", 1996, 3))

# Trying to get candidates data for non-existent elections
try(get_candidates_data("congress", 1800, 1))

############## get_candidacies_data -----####################################################
##Correct examples ----
# Single election
candidates_data_1986 <- get_candidacies_data("congress", 1986, 6)

# Multiple elections
combined_candidates_data <- get_candidacies_data(c("congress", "congress", "senate"),
                                                 c(2019, 2016, 2016),
                                                 c(11, 6, 6))

# With candidates details
candidates_data_with_details <- get_candidacies_data("congress", 2019, 4, 
                                                     include_candidates = TRUE)

##Incorrect examples ----

# Invalid election type
candidates_data_invalid <- get_candidacies_data("national", 2019, 4)

# Length mismatch between year and month
candidates_data_mismatch <- get_candidacies_data("congress", 2016, c(4, 11))

# Invalid date format
candidates_data_invalid_date <- get_candidacies_data("congress", "2016-06-26")

# Non-existent election data
candidates_data_non_existent <- get_candidacies_data("congress", 1990, 4)


############## get_CERA_data -----####################################################

## Correct examples ----
# Fetch CERA data aggregated at the municipal level
cera_data_mun <- get_CERA_data(poll_station_data1, 
                               id_col = "id_INE_poll_station", 
                               level = "mun")
print(cera_data_mun)

# Fetch CERA data aggregated at the provincial level
cera_data_prov <- get_CERA_data(poll_station_data1, 
                                id_col = "id_INE_poll_station", 
                                level = "prov")
print(cera_data_prov)

## Wrong examples ----
# Attempt to fetch CERA data with a non-existent id column, should raise an error
cera_data_invalid_col <- get_CERA_data(poll_station_data1, 
                                       id_col = "non_existent_column", 
                                       level = "mun")

# Attempt to fetch CERA data with an invalid aggregation level, should raise an error
cera_data_invalid_level <- get_CERA_data(poll_station_data1, 
                                         id_col = "id_INE_poll_station", 
                                         level = "barrio")

