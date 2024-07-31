
library(rvest)
library(dplyr)
library(tidyverse)
library(lubridate)

# Define the URL and read the webpage
url <- "https://es.wikipedia.org/wiki/Anexo:Elecciones_en_Espa%C3%B1a"
webpage <- read_html(url)

# Extract all tables from the webpage
tables <- webpage %>% html_table(fill = TRUE)

# Select tables
referendum_elections <- tables[[10]]
eu_elections <- tables[[12]]
general_elections <- tables[[13]]
ccaa_elections <- tables[[14]]
municipal_elections <- tables[[15]]


library(dplyr)
library(lubridate)

# Function to convert "1976 15 de diciembre" to "DD/MM/YYYY"
convert_spanish_date <- function(date_string) {
  # Convert the Spanish month to a number
  months <- c("enero" = 1, "febrero" = 2, "marzo" = 3, "abril" = 4,
              "mayo" = 5, "junio" = 6, "julio" = 7, "agosto" = 8,
              "septiembre" = 9, "octubre" = 10, "noviembre" = 11, "diciembre" = 12)
  
  # Extract year, day, and month from the string
  parts <- strsplit(date_string, " ")[[1]]
  year <- as.numeric(parts[1])
  day <- as.numeric(parts[2])
  month <- months[parts[4]]
  
  # Create a date object
  date_obj <- make_date(year = year, month = month, day = day)
  
  # Format the date as "DD/MM/YYYY"
  formatted_date <- format(date_obj, "%d/%m/%Y")
  return(formatted_date)
}

# Apply the function to the dataframe
referendum_elections <- referendum_elections %>% 
  mutate(date_elec = paste(Año, Día)) |> 
  mutate(date_elec = sapply(date_elec, convert_spanish_date)) |> 
  rename(year = Año, 
         topic = `Referéndum nacional`) |> 
  mutate(day = day(date_elec), 
         month = month(date_elec)) |> 
  select(-Día) |> 
  mutate(cod_elec = "01", type_elec = "referendum")

eu_elections <- eu_elections %>% 
  mutate(date_elec = paste(Año, Día)) |> 
  mutate(date_elec = sapply(date_elec, convert_spanish_date)) |> 
  rename(year = Año) |> 
  mutate(day = day(date_elec), 
         month = month(date_elec)) |> 
  select(-Día, -`Elecciones al Parlamento Europeo`) |> 
  mutate(cod_elec = "07", type_elec = "EU")

general_elections <- general_elections %>% 
  mutate(date_elec = paste(Año, Día)) |> 
  mutate(date_elec = sapply(date_elec, convert_spanish_date)) |> 
  rename(year = Año) |> 
  mutate(day = day(date_elec), 
         month = month(date_elec)) |> 
  select(-Día, -`Elecciones generales`)

congress_elections <- general_elections %>%
  mutate(cod_elec = "02", type_elec = "congress")

senate_elections <- general_elections %>%
  mutate(cod_elec = "03", type_elec = "senate")

ccaa_elections <- ccaa_elections %>% 
  mutate(date_elec = paste(Año, Día)) |> 
  mutate(date_elec = sapply(date_elec, convert_spanish_date)) |> 
  rename(year = Año) |> 
  mutate(day = day(date_elec), 
         month = month(date_elec)) |> 
  select(-Día) |> 
  rename(topic = `Elecciones autonómicas`) |> 
  mutate(cod_elec = "05", type_elec = "regional")


municipal_elections <- municipal_elections %>% 
  mutate(date_elec = paste(Año, Día)) |> 
  mutate(date_elec = sapply(date_elec, convert_spanish_date)) |> 
  rename(year = Año) |> 
  mutate(day = day(date_elec), 
         month = month(date_elec)) |> 
  select(-Día, -`Elecciones municipales`) |> 
  mutate(cod_elec = "04", type_elec = "local")

# Bind all data frames into one
dates_elections_spain <- bind_rows(
  referendum_elections,
  eu_elections,
  ccaa_elections,
  municipal_elections,
  congress_elections,
  senate_elections
)

rm(referendum_elections,
   eu_elections,
   ccaa_elections,
   municipal_elections,
   congress_elections,
   senate_elections)

dates_elections_spain <- dates_elections_spain %>%
  select(cod_elec, type_elec, date_elec, year, month, day, topic) %>%
  mutate(date_elec = ymd(dmy(date_elec))) 

# Write rda
save(dates_elections_spain, file = "./get auxiliary data/dates_elections_spain.rda")

# Clear all objects from the current environment
rm(list = ls())

   