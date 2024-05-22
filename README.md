Contents:
-mhra: folder containing the zip files for each drug on the WHRA website
-combined_df_100_percent_compression.fst: ADR master table
-process_files_190524: R script that takes input all zip files and returns the ADR master table

To read the master table as a variable in R studio:
(given that the fst file is in your current working directoty in R studio)

install.packages('fst')
library(fst)
ADR_master <- read.fst('./combined_df_100_percent_compression.fst')
