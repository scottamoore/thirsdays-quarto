library(tidyverse)
library(DBI)
library(duckdb)
library(here)

connect_to_irdata <- function() {
  dbConnect(duckdb::duckdb(),
    dbdir = here("irdw", "ir_data.duckdb"),
    read_only = FALSE
  )
}

connect_to_sis <- function() {
  dbConnect(duckdb::duckdb(),
    dbdir = here("irdw", "sis_db.duckdb"),
    read_only = TRUE
  )
}

connect_to_admissions <- function() {
  dbConnect(duckdb::duckdb(),
    dbdir = here("irdw", "admissions_db.duckdb"),
    read_only = TRUE
  )
}

connect_to_finaid <- function() {
  dbConnect(duckdb::duckdb(),
    dbdir = here("irdw", "financial_aid_db.duckdb"),
    read_only = TRUE
  )
}
