# Assuming we have pulled all files from the MHRA Yellow card site
# This script processes all files into one master table (combined_df)

# Dependencies ----
library(future)
library(collapse)
library(fst)
library(future.apply)
library(dplyr)
library(readr)

# Enable parallel processing & Set the working directory to where the zip files are stored ----
future::plan(multisession, workers = 8)
setwd("./mhra")

# Functions ----
# Function to process each zip file
processZippedFiles <- function(zip_file) {
  # Create a temporary directory for the unzipped files
  temp_dir <- tempdir()

  # Unzip files into the temporary directory
  unzip(zip_file, exdir = temp_dir)

  # List all files in the temporary directory
  files <- list.files(temp_dir, full.names = TRUE)

  # Initialize variables
  case_data <- drug_data <- event_data <- info_data <- NULL

  # Load the data from relevant files
  for (file in files) {
    if (grepl("_case.csv$", file)) {
      case_data <- read_csv(file)
    } else if (grepl("_drug.csv$", file)) {
      drug_data <- read_csv(file)
    } else if (grepl("_event.csv$", file)) {
      event_data <- read_csv(file)
    } else if (grepl("_info.csv$", file)) {
      info_data <- read_csv(file)
    }
  }

  # Print a message to indicate processing is complete
  print(paste("Processed", zip_file))

  # Return data
  return(list(case_data = case_data, drug_data = drug_data, event_data = event_data, info_data = info_data))
}

# Function to pivot/compress the dataset to one row per ADR
toAdrLevel <- function(data) {
  data %>%
    # Group by ADR for fmutate
    collapse::fgroup_by(., ADR) %>%
    # Create new col 'row_id' that increments by one for each row within an ADR group
    collapse::fmutate(., row_id = seq_along(ADR)) %>%
    # Pivot wider
    collapse::pivot(., ids = 'ADR', values = names(data[, -1]), names = 'row_id', how = 'wider') %>%
    # Ungroup to enable joining later
    collapse::fungroup()
}

# Function to retreive the drug name for a given folder
getDrugName <- function(data) {
  data %>%
    filter(ITEM == "DRUG_NAME") %>%
    pull(VALUE)
}

# Run functions ----
# Get a list of all zip files in the directory
zip_files <- list.files(pattern = "\\.zip$")

# Apply the function to each zip file
data_list <- lapply(zip_files, processZippedFiles)

# Loop over each folder/drug, concatonate the tables and store as a list of dataframes
joined_data_list <- future_lapply(seq_along(data_list), function(i) {
  x <- data_list[[i]]  # Get the ith element of data_list

  # Process the case, drug and event tables using toAdrLevel
  case <- toAdrLevel(x[['case_data']])
  drug <- toAdrLevel(x[['drug_data']])
  event <- toAdrLevel(x[['event_data']])
  # Get drug name using getDrugName
  drug_name <- getDrugName(x[['info_data']])

  # Join case, drug and event tables into one
  result <- case %>%
    left_join(drug, by = 'ADR') %>%
    left_join(event, by = 'ADR') %>%
    # Add a column for the drug name
    mutate(DRUG = drug_name)

  # Print the iteration number
  cat(sprintf("Processing element %d\n", i))

  return(result)
})

# Save the joined_data_list object list locally
saveRDS(joined_data_list, './joined_data_list_all.rds')
# Read the joined_data_list into the enviornment (if not already loaded)
joined_data_list <- readRDS('./joined_data_list_all.rds')

# Coerce all values to character type in order to bind all dataframes together
joined_data_list_coerced <- future_lapply(joined_data_list, function(df) {
  data.frame(lapply(df, as.character), stringsAsFactors = FALSE)
})

# Combine all dataframes into one master df
combined_df <- collapse::rowbind(joined_data_list_coerced, fill=TRUE)
# Save master ADR file
write.fst(combined_df, './combined_df.fst')
write.fst(combined_df, './combined_df_100_percent_compression.fst', compress = 100)
# NEED TO REORDER COLS

combined_df_load_test <- read.fst('./combined_df_100_percent_compression.fst')

