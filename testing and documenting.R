mun_census_data1 <- get_mun_census_data("congress", 2023, 07)
poll_station_data1 <- get_poll_station_data("congress", 2023, 07)
candidates_data1 <- get_candidates_data("congress", 2023, 07)
candidacies_data_1 <- get_candidacies_data("congress", 1986, 6, include_candidates = TRUE)

cera_data_mun <- get_CERA_data(poll_station_data1, 
                               id_col = "id_INE_poll_station", 
                               level = "mun")

ballots_data1 <- get_candidacy_ballot_data("congress", 2023, 07)
aggregated_data_ccaa1 <- aggregate_election_data(ballots_data1, scope = "ccaa", group_by_candidacy = TRUE)

survey_data <- get_survey_data(from = 1982, to = 1986, min_days_to = 3)
