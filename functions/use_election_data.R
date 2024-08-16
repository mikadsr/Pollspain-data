#' @title Allocate Seats Using the D'Hondt Method for the Spanish Congress
#'
#' @description This function allocates seats to political parties for the Spanish Congress of Deputies 
#' using the D'Hondt method. It allows for aggregation of seat distribution at the provincial, 
#' autonomous community, or national level.
#'
#' @param last_election_ballots A data frame containing the votes for each polling station.
#' @param level The aggregation level for the final seat distribution. Options are `"prov"` (default) 
#'   for provincial level, `"ccaa"` for autonomous community level, and `"national"` for national level.
#'
#' @return A data frame with the final seat distribution, filtered to remove candidacies with zero seats.
#'
#' @details 
#' This function performs the following steps:
#' 1. Aggregates votes by province, autonomous community, and party.
#' 2. Determines the number of seats per province based on the election year using the internal dataset `seat_distribution_congress`.
#' 3. Applies the D'Hondt method to allocate seats to parties.
#' 4. Joins the results with party names and autonomous community names from the internal dataset `cod_INE_mun`.
#' 5. Aggregates the results based on the specified level.
#' 6. Filters out candidacies with zero seats before returning the final result.
#'
#' @examples
#' # Example usage:
#' final_seat_distribution <- allocate_seats_dhondt(last_election_ballots, 
#'                                                  level = "ccaa")
#' print(final_seat_distribution)
#'
#' @author DeSmedt, Mikaela
#' 
#'
#' @export
allocate_seats_dhondt <- function(last_election_ballots, level = "prov") {
  
  # Step 1: Aggregate votes by province, party, and ccaa
  votes_by_province <- last_election_ballots %>%
    group_by(cod_INE_prov, cod_MIR_ccaa, cod_candidacies_prov) %>%
    summarize(total_votes = sum(ballots), .groups = 'drop')
  
  # Step 2: Determine seats per province for the election year
  election_year <- as.character(year(last_election_ballots$date_elec[1]))
  
  # Access static datasets within the package environment
  seats_per_province <- seat_distribution_congress %>%
    filter(year == election_year) %>%
    select(cod_INE_prov, prov, seats)
  
  # Step 3: Join the aggregated votes with seat distribution data
  votes_with_seats <- votes_by_province %>%
    left_join(seats_per_province, by = "cod_INE_prov")
  
  # Step 4: Filter out candidacies that do not meet the 3% threshold
  votes_with_seats <- votes_with_seats %>%
    group_by(cod_INE_prov) %>%
    mutate(total_votes_prov = sum(total_votes)) %>%
    filter(total_votes / total_votes_prov >= 0.03) %>%
    ungroup()
  
  # Define the D'Hondt allocation function
  distribute_seats_dhondt <- function(votes, num_seats) {
    num_parties <- length(votes)
    quotients <- matrix(0, nrow = num_parties, ncol = num_seats)
    
    for (i in 1:num_parties) {
      quotients[i, ] <- votes[i] / 1:num_seats
    }
    
    sorted_quotients <- sort(as.vector(quotients), decreasing = TRUE)
    cutoff <- sorted_quotients[num_seats]
    
    seat_allocation <- integer(num_parties)
    for (i in 1:num_parties) {
      seat_allocation[i] <- sum(quotients[i, ] >= cutoff)
    }
    
    return(seat_allocation)
  }
  
  # Step 5: Apply the D'Hondt function to each province while preserving columns
  seat_distribution_results <- votes_with_seats %>%
    filter(cod_INE_prov != "99") %>%
    group_by(cod_INE_prov, cod_MIR_ccaa, prov) %>%
    summarize(
      seats_allocated = list(distribute_seats_dhondt(total_votes, unique(seats))),
      party_codes = list(cod_candidacies_prov),
      .groups = 'drop'
    )
  
  # Step 6: Expand the list columns to show seats per party
  seat_distribution_expanded <- seat_distribution_results %>%
    unnest(cols = c(seats_allocated, party_codes))
  
  # Step 7: Join back with party names and get the ccaa column from cod_INE_mun
  final_seat_distribution <- seat_distribution_expanded %>%
    left_join(last_election_ballots %>%
                select(cod_candidacies_prov, abbrev_candidacies, name_candidacies) %>%
                distinct(),
              by = c("party_codes" = "cod_candidacies_prov")) %>%
    left_join(cod_INE_mun %>%
                select(cod_INE_prov, ccaa) %>%
                distinct(),
              by = "cod_INE_prov")
  
  # Step 8: Aggregate results based on the selected level
  if (level == "ccaa") {
    final_seat_distribution <- final_seat_distribution %>%
      group_by(cod_MIR_ccaa, abbrev_candidacies, name_candidacies, ccaa) %>%
      summarize(seats = sum(seats_allocated), .groups = 'drop')
  } else if (level == "national") {
    final_seat_distribution <- final_seat_distribution %>%
      group_by(abbrev_candidacies, name_candidacies) %>%
      summarize(seats = sum(seats_allocated), .groups = 'drop')
  } else {
    final_seat_distribution <- final_seat_distribution %>%
      group_by(cod_INE_prov, prov, cod_MIR_ccaa, abbrev_candidacies, name_candidacies, ccaa) %>%
      summarize(seats = sum(seats_allocated), .groups = 'drop')
  }
  
  # Step 9: Filter out candidacies with 0 seats
  final_seat_distribution <- final_seat_distribution %>%
    filter(seats > 0)
  
  # Return the final seat distribution
  return(final_seat_distribution)
}


#' @title Plot Election Results by Province on a Map of Spain
#'
#' @description 
#' This function visualizes election results by province on a map of Spain. 
#' The function merges election data with geographic data of Spanish provinces 
#' and then colors each province according to the party that won the most seats there. 
#' It allows for a default color scheme provided via an external resource or the use 
#' of a user-defined color scheme.
#'
#' @param election_data A data frame containing the election results with the following structure:
#'   \describe{
#'     \item{cod_INE_prov}{character: Province code (as a string).}
#'     \item{prov}{character: Name of the province.}
#'     \item{cod_MIR_ccaa}{character: Autonomous community code.}
#'     \item{abbrev_candidacies}{character: Abbreviation of the political party or candidacy.}
#'     \item{name_candidacies}{character: Full name of the political party or candidacy.}
#'     \item{ccaa}{character: Name of the autonomous community.}
#'     \item{seats}{integer: Number of seats won by the candidacy in the province.}
#'   }
#' @param colors_url A string providing the URL to the .rda file containing the color mapping 
#'   for each party. The default URL is 
#'   "https://github.com/mikadsr/Pollspain-data/raw/main/get%20auxiliary%20data/party_colors_hex.rda".
#'
#' @return This function does not return a value. Instead, it generates a plot visualizing the 
#'   election results on a map of Spain.
#'
#' @examples
#' # Assuming 'a' is the election data frame:
#' plot_seat_allocation(election_data = a)
#'
#' @authors 
#' Mikaela DeSmedt
#' 
plot_election_results  <- function(election_data, colors_url = "https://github.com/mikadsr/Pollspain-data/raw/main/get%20auxiliary%20data/party_colors_hex.rda") {
  
  # Load the party colors from the specified URL
  temp_file <- tempfile()
  download.file(colors_url, temp_file, quiet = TRUE)
  load(temp_file)
  
  # Ensure the province codes in the election data are characters
  election_data$cod_INE_prov <- as.character(election_data$cod_INE_prov)
  
  # Get the provinces map data
  provinces_map <- esp_get_prov()
  
  # Merge the map with the election results
  merged_data <- merge(provinces_map, election_data, by.x = "cpro", by.y = "cod_INE_prov", all.x = TRUE)
  
  # Remove duplicates in the color data, keeping the first occurrence
  party_colors_hex_unique <- party_colors_hex %>%
    distinct(abbrev_candidacies, .keep_all = TRUE)
  
  # Join the color data with the merged data
  merged_data <- merged_data %>%
    left_join(party_colors_hex_unique, by = "abbrev_candidacies")
  
  # Plotting the results using the colors from the joined data
  ggplot(merged_data) +
    geom_sf(aes(fill = abbrev_candidacies), color = "white") +
    scale_fill_manual(values = setNames(merged_data$party_color, merged_data$abbrev_candidacies)) +
    labs(title = "Spanish Congress Election Results by Province",
         fill = "Winning Party") +
    theme_void() +
    theme(legend.position = "bottom")
}
