# file: code/warehouse/readers.R

library(tidyverse)
library(DBI)
library(duckdb)
library(here)

source(here("code", "define_connect_to_irdata.R"))
source(here("code", "exploration_functions.R"))

read_from_warehouse <- function(
  con,
  table_name,
  schema_name = "warehouse"
) {
  # Use schema-qualified table name as string instead of DBI::Id()
  table_ref <- paste0(schema_name, ".", table_name)

  df <- con |>
    tbl(table_ref) |>
    collect()
  post_read_processing(table_name, df)
}

post_read_processing <- function(table_name, df) {
  if (table_name == "student") {
    df |>
      mutate(
        gender = factor(gender,
          levels = c("F", "M", "U", "O"),
          labels = c("Female", "Male", "Unknown", "Other"),
          ordered = FALSE
        ),
        student_type = factor(student_type,
          levels = c("FTF", "TR"),
          labels = c("First-time", "Transfer"),
          ordered = FALSE
        ),
        is_in_state = factor(is_in_state,
          levels = c(FALSE, TRUE),
          labels = c("Out-of-state", "In-state"),
          ordered = FALSE
        )
      )
  } else if (table_name == "enrollment") {
    df |>
      mutate(
        grade_ltr = factor(grade_ltr,
          levels = c(
            "A+", "A", "A-", "B+", "B", "B-", "C+", "C",
            "C-", "D+", "D", "D-", "F", "W"
          ),
          ordered = TRUE
        )
      )
  } else if (table_name == "applicant") {
    df |>
      mutate(
        state = factor(state, ordered = FALSE),
        admit_type = factor(admit_type),
        is_in_state = factor(is_in_state,
          levels = c(FALSE, TRUE),
          labels = c("Out-of-state", "In-state"),
          ordered = FALSE
        ),
        intended_major = factor(intended_major, ordered = FALSE),
        has_submitted_essay = factor(has_submitted_essay,
          levels = c(FALSE, TRUE),
          labels = c("No", "Yes"),
          ordered = FALSE
        ),
        has_been_admitted = factor(has_been_admitted,
          levels = c(FALSE, TRUE),
          labels = c("No", "Yes"),
          ordered = FALSE
        ),
        has_fin_aid_offer = factor(has_fin_aid_offer,
          levels = c(FALSE, TRUE),
          labels = c("No", "Yes"),
          ordered = FALSE
        ),
        is_expected_to_enroll = factor(is_expected_to_enroll,
          levels = c(FALSE, TRUE),
          labels = c("No", "Yes"),
          ordered = FALSE
        ),
        is_interested_in_housing = factor(is_interested_in_housing,
          levels = c(FALSE, TRUE),
          labels = c("No", "Yes"),
          ordered = FALSE
        )
      )
  } else if (table_name == "course") {
    df |>
      mutate(
        subject_id = factor(subject_id, ordered = FALSE),
        audience = factor(audience,
          levels = c("GENED", "MAJOR"),
          labels = c("GenEd", "Major"),
          ordered = FALSE
        )
      )
  } else if (table_name == "course_section") {
    df |>
      mutate(
        subject_id = factor(subject_id, ordered = FALSE)
      )
  } else if (table_name == "term") {
    df |>
      mutate(
        season = factor(season,
          levels = c("Spring", "Summer", "Fall"),
          labels = c("Spring", "Summer", "Fall"),
          ordered = TRUE
        )
      )
  } else {
    df
  }
}
