library(dplyr)
#########################
# Create the summary
mun_summary <- poll_station_data1 %>%
  filter(mun != "CERA") |> 
  group_by(ccaa, prov, mun) %>%
  summarise(
    n_poll_stations = n(),
    type_elec = unique(type_elec),
    date_elec = unique(date_elec),
    blank_ballots = sum(blank_ballots, na.rm = TRUE),
    invalid_ballots = sum(invalid_ballots, na.rm = TRUE),
    party_ballots = sum(party_ballots, na.rm = TRUE),
    valid_ballots = sum(valid_ballots, na.rm = TRUE),
    total_ballots = sum(total_ballots, na.rm = TRUE)
  ) 

# Select the relevant columns from mun_census_data1
census_data <- mun_census_data1 %>%
  select(type_elec, date_elec, ccaa, prov, mun, census_counting_mun)

# Join the census_counting_mun column to the summary dataframe
mun_summary <- mun_summary %>%
  left_join(census_data, by = c("type_elec", "date_elec","ccaa", "prov", "mun")) |> 
  mutate(turnout =round(100 * total_ballots / census_counting_mun, 2),
         porc_valid = round(100 * valid_ballots / total_ballots, 2),
         porc_invalid =round(100 * invalid_ballots / total_ballots, 2),
         porc_parties = round(100 * party_ballots / valid_ballots, 2),
         porc_blank = round(100 * blank_ballots / valid_ballots, 2))

# View the updated summary
print(mun_summary)
str(mun_summary)

#get_pollstation_data and join with get_mun_census_data

############
# Create the summary
prov_summary <- poll_station_data1 %>%
  filter(mun != "CERA") |> 
  group_by(ccaa, prov) %>%
  summarise(
    n_poll_stations = n(),
    type_elec = unique(type_elec),
    date_elec = unique(date_elec),
    blank_ballots = sum(blank_ballots, na.rm = TRUE),
    invalid_ballots = sum(invalid_ballots, na.rm = TRUE),
    party_ballots = sum(party_ballots, na.rm = TRUE),
    valid_ballots = sum(valid_ballots, na.rm = TRUE),
    total_ballots = sum(total_ballots, na.rm = TRUE)
  ) 


# Select the relevant columns from mun_census_data1
census_data <- mun_census_data1 %>% 
  select(type_elec, date_elec, ccaa, prov, census_counting_mun) |> 
  group_by(ccaa, prov) |> 
  summarise(type_elec = unique(type_elec),
            date_elec = unique(date_elec),
            census_counting_prov = sum(census_counting_mun))

# Join the census_counting_mun column to the summary dataframe
prov_summary <- prov_summary %>%
  left_join(census_data, by = c("type_elec", "date_elec", "ccaa", "prov")) |> 
  mutate(turnout =round(100 * total_ballots / census_counting_prov, 2),
         porc_valid = round(100 * valid_ballots / total_ballots, 2),
         porc_invalid =round(100 * invalid_ballots / total_ballots, 2),
         porc_parties = round(100 * party_ballots / valid_ballots, 2),
         porc_blank = round(100 * blank_ballots / valid_ballots, 2))

############
# Create the summary
ccaa_summary <- poll_station_data1 %>%
  filter(mun != "CERA") |> 
  group_by(ccaa) %>%
  summarise(
    n_poll_stations = n(),
    type_elec = unique(type_elec),
    date_elec = unique(date_elec),
    blank_ballots = sum(blank_ballots, na.rm = TRUE),
    invalid_ballots = sum(invalid_ballots, na.rm = TRUE),
    party_ballots = sum(party_ballots, na.rm = TRUE),
    valid_ballots = sum(valid_ballots, na.rm = TRUE),
    total_ballots = sum(total_ballots, na.rm = TRUE)
  ) 


# Select the relevant columns from mun_census_data1
census_data <- mun_census_data1 %>% 
  select(type_elec, date_elec, ccaa, census_counting_mun) |> 
  group_by(ccaa) |> 
  summarise(type_elec = unique(type_elec),
            date_elec = unique(date_elec),
            census_counting_ccaa = sum(census_counting_mun))

# Join the census_counting_mun column to the summary dataframe
ccaa_summary <- ccaa_summary %>%
  left_join(census_data, by = c("type_elec", "date_elec", "ccaa")) |> 
  mutate(turnout =round(100 * total_ballots / census_counting_ccaa, 2),
         porc_valid = round(100 * valid_ballots / total_ballots, 2),
         porc_invalid =round(100 * invalid_ballots / total_ballots, 2),
         porc_parties = round(100 * party_ballots / valid_ballots, 2),
         porc_blank = round(100 * blank_ballots / valid_ballots, 2))

############
# Create the summary
national_summary <- poll_station_data1 %>%
  filter(mun != "CERA") |> 
  group_by(date_elec) %>%
  summarise(
    n_poll_stations = n(),
    type_elec = unique(type_elec),
    date_elec = unique(date_elec),
    blank_ballots = sum(blank_ballots, na.rm = TRUE),
    invalid_ballots = sum(invalid_ballots, na.rm = TRUE),
    party_ballots = sum(party_ballots, na.rm = TRUE),
    valid_ballots = sum(valid_ballots, na.rm = TRUE),
    total_ballots = sum(total_ballots, na.rm = TRUE)
  ) 

# Select the relevant columns from mun_census_data1
census_data <- mun_census_data1 %>% 
  select(type_elec, date_elec, census_counting_mun) |> 
  summarise(type_elec = unique(type_elec),
            date_elec = unique(date_elec),
            census_counting_national = sum(census_counting_mun))


# Join the census_counting_mun column to the summary dataframe
national_summary <- national_summary %>%
  left_join(census_data, by = c("type_elec", "date_elec")) |> 
  mutate(turnout =round(100 * total_ballots / census_counting_national, 2),
         porc_valid = round(100 * valid_ballots / total_ballots, 2),
         porc_invalid =round(100 * invalid_ballots / total_ballots, 2),
         porc_parties = round(100 * party_ballots / valid_ballots, 2),
         porc_blank = round(100 * blank_ballots / valid_ballots, 2))

############

# Perform a full join
census_data_full <- mun_census_data1 %>%
  full_join(cera_data_mun, by = c("type_elec", "date_elec", "cod_INE_ccaa", "cod_INE_prov", "cod_INE_mun"))

# View the result
View(census_data_full)
str(census_data_full)

# Identify rows with NA in census_data_full
census_data_full %>%
  filter(is.na(census_cera) | is.na(census_counting_mun)) %>%
  select(type_elec, date_elec, cod_INE_ccaa, cod_INE_prov, cod_INE_mun) |> 
  filter(cod_INE_mun == 999)
