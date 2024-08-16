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
  # (function body remains the same as shown above)
}

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
  
  # Join the aggregated votes with seat distribution data
  votes_with_seats <- votes_by_province %>%
    left_join(seats_per_province, by = "cod_INE_prov")
  
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
  
  # Apply the D'Hondt function to each province while preserving columns
  seat_distribution_results <- votes_with_seats %>%
    filter(cod_INE_prov != "99") %>%
    group_by(cod_INE_prov, cod_MIR_ccaa, prov) %>%
    summarize(
      seats_allocated = list(distribute_seats_dhondt(total_votes, unique(seats))),
      party_codes = list(cod_candidacies_prov),
      .groups = 'drop'
    )
  
  # Expand the list columns to show seats per party
  seat_distribution_expanded <- seat_distribution_results %>%
    unnest(cols = c(seats_allocated, party_codes))
  
  # Join back with party names and get the ccaa column from cod_INE_mun
  final_seat_distribution <- seat_distribution_expanded %>%
    left_join(last_election_ballots %>%
                select(cod_candidacies_prov, abbrev_candidacies, name_candidacies) %>%
                distinct(),
              by = c("party_codes" = "cod_candidacies_prov")) %>%
    left_join(cod_INE_mun %>%
                select(cod_INE_prov, ccaa) %>%
                distinct(),
              by = "cod_INE_prov")
  
  # Aggregate results based on the selected level
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
  
  # Filter out candidacies with 0 seats
  final_seat_distribution <- final_seat_distribution %>%
    filter(seats > 0)
  
  # Return the final seat distribution
  return(final_seat_distribution)
}
