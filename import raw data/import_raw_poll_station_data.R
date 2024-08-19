source("./import raw data/import_raw_data_functions.R")
#check, install if necessary, and load the required packages
if (!require("pacman")) install.packages("pacman")
pacman::p_load(tidyverse,glue,lubridate,usethis)
###########################################################

# IMPORT POLL STATION DATA ----
## ----- poll data congress -----
congress_elec <- dates_elections_spain %>%
  filter(cod_elec == "02" & year >= 1982) %>%
  mutate(cod_elec = as.character(cod_elec)) %>%
  drop_na(year, month) %>%
  select(type_elec, year, month, cod_elec)

historical_poll_station_data_congress <- congress_elec %>%
  rowwise() %>%
  reframe(import_poll_stations_MIR_files(type_elec, year, month)) %>%
  select(-any_of("value"))

## ----- poll data senate -----
senate_elec <- dates_elections_spain |>
  filter(cod_elec == "03" & year >= 1986) |>
  drop_na(year, month) |>
  select(type_elec, year, month)

historical_poll_station_data_senate <- senate_elec |>
  rowwise() |>
  reframe(import_poll_stations_MIR_files(type_elec, year, month)) |>
  select(-any_of("value"))

## ----- use data -----
historical_raw_poll_station_data <- bind_rows(historical_poll_station_data_congress,
                                              historical_poll_station_data_senate)

# too much size
# usethis::use_data(historical_raw_poll_station_data, overwrite = TRUE,
#                   compress = "xz")

## Write the results to CSV files
#historical_raw_poll_station_data %>%
#  split(historical_raw_poll_station_data$type_elec) %>%
#  map(function(x) {
#    split(x, x$date_elec)
#  }) %>%
#  map(function(y) {
#    map(y, function(x) {
#      election_info <- type_to_code_election(unique(x$type_elec))
#      date_elec <- unique(x$date_elec)
#      year <- year(date_elec)  # Extract year using lubridate
#      month <- month(date_elec)  # Extract month using lubridate
#      month <- sprintf("%02d", month)  # Ensure month is two digits
#      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
#      
#      # Create directory if it doesn't exist
#      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
#      
#      write_csv(x, file = glue("{output_dir}/raw_poll_stations_{unique(x$type_elec)}_{year}_{month}.csv"))
#    })
#  })

# ----- save_rda -----
historical_raw_poll_station_data %>%
  split(historical_raw_poll_station_data$type_elec) %>%
  map(function(x) {
    split(x, x$date_elec)
  }) %>%
  map(function(y) {
    map(y, function(x) {
      election_info <- type_to_code_election(unique(x$type_elec))
      date_elec <- unique(x$date_elec)
      year <- year(date_elec)  # Extract year using lubridate
      month <- month(date_elec)  # Extract month using lubridate
      month <- sprintf("%02d", month)  # Ensure month is two digits
      output_dir <- glue("C:/Users/mklde/OneDrive/Documents/R stuff/Pollspain-data/{election_info$dir}/{election_info$cod_elec}{year}{month}")
      
      # Create directory if it doesn't exist
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      
      # Save the data frame as .rda file
      save(x, file = glue("{output_dir}/raw_poll_stations_{unique(x$type_elec)}_{year}_{month}.rda"))
    })
  })


# ----- rm -----
rm(list = c("historical_poll_station_data_congress",
            "historical_poll_station_data_senate",
            "historical_raw_poll_station_data"))

