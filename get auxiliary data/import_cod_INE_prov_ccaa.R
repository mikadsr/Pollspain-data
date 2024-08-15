library(readxl)
library(dplyr)
library(stringr)
#Load file with INE codes for provice codes with CCAA codes
cod_INE_ccaa_prov <- read_excel("get auxiliary data/cod_INE_ccaa_prov.xlsx") 

#Load file with INE codes for municipalities
diccionario24 <- read_excel("get auxiliary data/diccionario24.xlsx") |> 
  rename(cod_INE_ccaa = "CODAUTO",
         cod_INE_prov = "CPRO",
         cod_INE_mun= "CMUN",
         mun="NOMBRE") |> 
  select(-DC) 

# Perform the join between two files
cod_INE_mun <- left_join(cod_INE_ccaa_prov, diccionario24, 
                         by = c("cod_INE_ccaa", "cod_INE_prov")) |> 
  mutate(
    cod_INE_ccaa = sprintf("%02d", cod_INE_ccaa),
    cod_MIR_ccaa = sprintf("%02d", cod_MIR_ccaa),
    cod_INE_prov = sprintf("%02d", cod_INE_prov),
    cod_INE_mun = sprintf("%03d", cod_INE_mun)  # Assuming cod_INE_mun should be 3 digits
  ) |> 
  # Create id codes
  mutate(id_INE_mun =
           glue("{cod_INE_ccaa}-{cod_INE_prov}-{cod_INE_mun}"),
         id_MIR_mun =
           glue("{cod_MIR_ccaa}-{cod_INE_prov}-{cod_INE_mun}")) |> 
  relocate(cod_INE_mun, mun, id_INE_mun, id_MIR_mun,cod_INE_prov, prov, cod_INE_ccaa, cod_MIR_ccaa, ccaa)

#Save .rda
save(cod_INE_mun, file ="./get auxiliary data/cod_INE_mun.rda")

rm(diccionario24, cod_INE_ccaa_prov)


