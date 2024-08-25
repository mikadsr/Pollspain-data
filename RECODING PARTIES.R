
##########################
library(dplyr)
library(stringi)

recod_parties <- function(parties_data, col_name_abbrev = "abbrev_candidacies", col_name_candidacies = "name_candidacies") {
  
  # Rename abbreviation and name columns
  parties_data <- parties_data %>%
    rename(abbrev_candidacies = {{col_name_abbrev}}, 
           name_candidacies = {{col_name_candidacies}})
  
  # General Cleanup
  parties_data <- parties_data %>%
    mutate(
      abbrev_candidacies = str_remove_all(abbrev_candidacies, "'|\\.|\\,|´"),
      abbrev_candidacies = str_remove_all(abbrev_candidacies, '\\"'),
      abbrev_candidacies = str_trim(abbrev_candidacies),
      abbrev_candidacies = str_replace_all(abbrev_candidacies, "–| - |/", "-"),
      abbrev_candidacies = str_replace_all(abbrev_candidacies, "\\+", ""),
      abbrev_candidacies = stri_trans_general(abbrev_candidacies, "Latin-ASCII"),
      
      name_candidacies = str_remove_all(name_candidacies, "'|\\.|\\,|´"),
      name_candidacies = str_remove_all(name_candidacies, '\\"'),
      name_candidacies = str_trim(name_candidacies),
      name_candidacies = str_replace_all(name_candidacies, "–| - |/", "-"),
      name_candidacies = str_replace_all(name_candidacies, "\\+", ""),
      name_candidacies = stri_trans_general(name_candidacies, "Latin-ASCII")
    )
  
  # Apply recoding using case_when
  parties_data <- parties_data %>%
    mutate(
      name_candidacies = case_when(
        str_detect(name_candidacies, "IZQU UNIDA-ESQUERRA UNIDA DEL PAIS VALENCIA|IZQUIERDA UNIDA-CONVOCATORIA POR ANDALUCIA|^IZQUIERDA UNIDA") ~ "IZQUIERDA UNIDA",
        str_detect(name_candidacies, "^CENTRE DEMOCRATIC I SOCIAL|^CENTRO DEMOCRATICO Y SOCIAL") ~ "CENTRO DEMOCRATICO Y SOCIAL",
        str_detect(name_candidacies, "^PARTIDO COMUNISTA") ~ "PARTIDO COMUNISTA DE ESPANA",
        str_detect(name_candidacies, "SUMAR") ~ "SUMAR",
        str_detect(name_candidacies, "UNION DEL PUEBLO NAVARRO-PARTIDO POPULAR|UNION DEL PUEBLO NAVARRO EN COALICION CON EL PARTIDO|PARTIDO POPULAR CENTRISTAS DE GALICIA|UNION DEL PUEBLO NAVARRO CON PARTIDO POPULAR|UNIÓN DEL PUEBLO NAVARRO EN COALICIÓN CON EL PARTI|PARTIDO POPULAR|PARTIT POP|PP EN COALICION CON UPM|UNION DEL PUEBLO NAVARRO EN COALICION CON EL PARTI") ~ "PARTIDO POPULAR",
        str_detect(name_candidacies, "COALICION CANARIA-PARTIDO NACIONALISTA CANARIO|COALICION CANARIA-NUEVA CANARIAS|NUEVA CANARIAS-COALICION CANARIA") ~ "COALICION CANARIA-NUEVA CANARIAS",
        str_detect(name_candidacies, "CIUTADANS PARTIDO DE LA CIUDADANIA|CIUTADANS-PARTIDO DE LA CIUDADANIA|CIUDADANOS-PARTIDO DE LA CIUDADANIA|CIUDADANOS PARTIDO DE LA CIUDADANIA") ~ "CIUDADANOS",
        str_detect(name_candidacies, "MES COMPROMIS") ~ "MES COMPROMIS",
        str_detect(name_candidacies, "EN COMU PODEM-GUANYEM EL CANVI|EN COMUN-UNIDAS PODEMOS|UNIDAS PODEMOS|UNIDES PODEM|ELKARREKIN PODEMOS-UNIDAS PODEMOS|UNITS PODEM MES|COMPROMIS-PODEMOS-EUPV: A LA VALENCIANA|EN MAREA|PODEMOS") ~ "UNIDAS PODEMOS",
        str_detect(name_candidacies, "PART SOCIALISTA OBRERO ESPANOL DE ANDALUCIA|PART SOCIALISTA OBRERO ESPANOL DE ANDALUCIA|SOCIALISTES|PARTIT DELS SOCIALISTES|PARTIT SOCIALISTA OBRER ESPANYOL|^PARTIDO DOS SOCIALISTAS|^PARTIDO SOCIALISTA") ~ "PARTIDO SOCIALISTA OBRERO ESPANOL",
        str_detect(name_candidacies, "CANDIDATURA DUNITAT POPULAR-PER LA RUPTURA|CUP-PR") ~ "CANDIDATURA DUNITAT POPULAR-PER LA RUPTURA",
        str_detect(name_candidacies, "EHBILDU|EH BILDU") ~ "EH-BILDU",
        str_detect(name_candidacies, "!TERUEL EXI") ~ "TERUEL EXISTE",
        str_detect(name_candidacies, "ALIANZA POP-PDEMOCPOPULAR-UNION VALENCIANA|ALIANZA POP-PDEMOCPOPULAR-PDEMOCLIBERAL-UCD|ALIANZA POPULAR|UPN-AP") ~ "ALIANZA POPULAR",
        str_detect(name_candidacies, "ARA|PV") ~ "ARA REPUBLIQUES",
        str_detect(name_candidacies, "BLOQUE NACIONALISTA GALEGO|BNG-NOS|BNG-NOS") ~ "BLOQUE NACIONALISTA GALEGO",
        str_detect(name_candidacies, "NC-CCA-PNC|NC-CC-PNC|NC-CCA|NC-CC|CC-NC-PNC|CC-PNC|CCA-PNC|COALICION CANARIA-NUEVA CANARIAS|NUEVA CANARIAS-COALICION CANARIA") ~ "COALICION CANARIA-NUEVA CANARIAS",
        str_detect(name_candidacies, "COMPROMIS") ~ "COMPROMIS",
        str_detect(name_candidacies, "AVANT ADELANTE LOS VERDES|AVANT LOS VERDES|GREENS|LOS VERDES|LV-LV|AVANT-LOS V|VERDES") ~ "LOS VERDES",
        str_detect(name_candidacies, "DL") ~ "CONVERGENCIA I UNIO",
        str_detect(name_candidacies, "M PAIS|MAS PAIS") ~ "MAS PAIS",
        str_detect(name_candidacies, "IULV-CA|ICV-EUIA") ~ "IZQUIERDA UNIDA",
        str_detect(name_candidacies, "PSA-PA") ~ "PARTIDO ANDALUCISTA",
        str_detect(name_candidacies, "ESQUERRA REPUBLICANA|ESQUERRA REPUBLICANA DE CATALUNYA-CATALUNYA SI|ERC-CATSI|ERC") ~ "ESQUERRA REPUBLICANA",
        str_detect(name_candidacies, "RUIZ-MATEOS") ~ "RUIZ-MATEOS",
        str_detect(name_candidacies, "EA-EUE") ~ "EUSKO ALKARTASUNA",
        str_detect(name_candidacies, "NA\\+") ~ "NAVARRA SUMA",
        str_detect(name_candidacies, "LIT-CI|LITCI") ~ "LIGA INTERNACIONAL DE TRABAJADORES",
        TRUE ~ name_candidacies
      ),
      abbrev_candidacies = case_when(
        str_detect(name_candidacies, "IZQUIERDA UNIDA") ~ "IU",
        str_detect(name_candidacies, "CENTRO DEMOCRATICO Y SOCIAL") ~ "CDS",
        str_detect(name_candidacies, "^PARTIDO COMUNISTA") ~ "PCE",
        str_detect(name_candidacies, "SUMAR") ~ "SUMAR",
        str_detect(name_candidacies, "COALICION CANARIA-NUEVA CANARIAS|NUEVA CANARIAS-COALICION CANARIA") ~ "CC-NC",
        str_detect(name_candidacies, "CIUTADANS-PARTIDO DE LA CIUDADANIA|CIUDADANOS-PARTIDO DE LA CIUDADANIA|CIUDADANOS") ~ "CS",
        str_detect(name_candidacies, "MES COMPROMIS") ~ "MES",
        str_detect(name_candidacies, "EN COMUN-UNIDAS PODEMOS|UNIDAS PODEMOS|UNIDES PODEM|ELKARREKIN PODEMOS-UNIDAS PODEMOS|UNITS PODEM MES|COMPROMIS-PODEMOS-EUPV: A LA VALENCIANA|EN MAREA|^PODEMOS") ~ "UP",
        str_detect(name_candidacies, "PARTIDO DE LOS SOCIALISTAS DE GALICIA-PSOE|^PARTIDO SOCIALISTA|SOCIALISTES|PARTIT DELS SOCIALISTES|PARTIT SOCIALISTA OBRER ESPANYOL|PARTIDO DOS SOCIALISTAS DE GALICIA-PARTIDO SOCIALI|PARTIDO SOCIALISTA DE EUSKADI-EUSKADIKO EZKERRA \\(P\\)") ~ "PSOE",
        str_detect(name_candidacies, "CANDIDATURA DUNITAT POPULAR-PER LA RUPTURA|CUP-PR") ~ "CUP",
        str_detect(name_candidacies, "PARTIDO POPULAR") ~ "PP",
        str_detect(name_candidacies, "EHBILDU|EH BILDU") ~ "EH-BILDU",
        str_detect(name_candidacies, "!TERUEL EXI") ~ "TE",
        str_detect(name_candidacies, "ALIANZA POPULAR|AP") ~ "AP",
        str_detect(name_candidacies, "ARA|PV") ~ "ARA-PV",
        str_detect(name_candidacies, "BLOQUE NACIONALISTA GALEGO|BNG-NOS|BNG-NOS|NOS") ~ "BNG",
        str_detect(name_candidacies, "NC-CCA-PNC|NC-CC-PNC|NC-CCA|NC-CC|CC-NC-PNC|CC-PNC|CCA-PNC|COALICION CANARIA-NUEVA CANARIAS|NUEVA CANARIAS-COALICION CANARIA") ~ "CC-NC",
        str_detect(name_candidacies, "COMPROMIS") ~ "COMPROMIS",
        str_detect(name_candidacies, "AVANT ADELANTE LOS VERDES|AVANT LOS VERDES|GREENS|LOS VERDES|LV-LV|AVANT-LOS V|VERDES") ~ "LV",
        str_detect(name_candidacies, "DL") ~ "DIL-CDC",
        str_detect(name_candidacies, "M PAIS|MAS PAIS") ~ "MP",
        str_detect(name_candidacies, "IULV-CA|ICV-EUIA") ~ "IU",
        str_detect(name_candidacies, "PSA-PA") ~ "PA",
        str_detect(name_candidacies, "ESQUERRA REPUBLICANA DE CATALUNYA-SOBIRANISTES|ESQUERRA REPUBLICANA") ~ "ERC",
        str_detect(name_candidacies, "RUIZ-MATEOS") ~ "ARM",
        str_detect(name_candidacies, "EA-EUE") ~ "EA",
        str_detect(name_candidacies, "NA\\+") ~ "NA-SUMA",
        str_detect(name_candidacies, "PARTIDO POPULAR") ~ "PP",
        str_detect(name_candidacies, "LIT-CI|LITCI") ~ "LIT-CI",
        TRUE ~ abbrev_candidacies
      )
    )
  
  return(parties_data)
}


