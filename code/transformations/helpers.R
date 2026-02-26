# transformations/helpers.R

library(tidyverse)
library(DBI)
library(duckdb)
library(here)

source(here("code", "define_connect_to_irdata.R"))
source(here("code", "exploration_functions.R"))

assess_table_nulls <- function(df,
                               null_values = c(NA, "none", "", "-"),
                               ignore_case = TRUE,
                               trim_ws = TRUE,
                               include_total = TRUE) {
  stopifnot(is.data.frame(df))
  stopifnot(length(null_values) > 0)

  # Split NA from string tokens
  count_na <- any(is.na(null_values))
  tokens_raw <- null_values[!is.na(null_values)]
  tokens_raw <- as.character(tokens_raw)

  normalize_chr <- function(x) {
    if (trim_ws) x <- stringr::str_trim(x)
    if (ignore_case) x <- stringr::str_to_lower(x)
    x
  }

  tokens_norm <-
    if (length(tokens_raw) > 0) {
      normalize_chr(tokens_raw)
    } else {
      character(0)
    }

  # Build SAFE display names (no "" allowed)
  display_token <- function(x) {
    dplyr::case_when(
      x == "" ~ "<blank>",
      stringr::str_detect(x, "^\\s+$") ~ "<whitespace>",
      TRUE ~ x
    )
  }
  tokens_disp <- if (length(tokens_norm) > 0) display_token(tokens_norm) else character(0)

  # If normalization collapses distinct inputs to same token, we should collapse them too
  # (e.g., "None" and " none " both become "none")
  token_map <- tibble::tibble(token_norm = tokens_norm, token_disp = tokens_disp) |>
    dplyr::distinct()

  n_rows <- nrow(df)
  cols <- names(df)

  # Preallocate result matrix
  col_names <- c(if (count_na) "<NA>" else NULL, token_map$token_disp)
  out <- matrix(0L,
    nrow = length(cols), ncol = length(col_names),
    dimnames = list(cols, col_names)
  )

  for (j in seq_along(cols)) {
    x <- df[[j]]

    if (count_na) {
      out[j, "<NA>"] <- sum(is.na(x))
    }

    if (nrow(token_map) > 0) {
      if (is.character(x) || is.factor(x)) {
        y <- normalize_chr(as.character(x))

        # Match against normalized tokens, then tabulate, then assign into display cols
        m <- match(y, token_map$token_norm, nomatch = 0L)
        if (any(m != 0L)) {
          counts <- tabulate(m, nbins = nrow(token_map))
          out[j, token_map$token_disp] <- counts
        }
      }
    }
  }

  result <- as.data.frame(out, stringsAsFactors = FALSE) |>
    tibble::rownames_to_column("column") |>
    dplyr::as_tibble()

  # Add totals / percents
  if (include_total) {
    result <- result |>
      dplyr::mutate(
        total_rows = n_rows,
        any_null = rowSums(dplyr::across(dplyr::all_of(col_names))),
        pct_any_null = dplyr::if_else(total_rows > 0, any_null / total_rows, NA_real_)
      ) |>
      dplyr::relocate(total_rows, any_null, pct_any_null, .after = column)
  }

  # Attach a legend so you can see what "<blank>" etc. correspond to
  attr(result, "null_token_legend") <- token_map

  result
}

write_data_to_warehouse <- function(con_to, table_name, df) {
  df <- add_import_datetime(df)
  DBI::dbWriteTable(
    con_to,
    DBI::Id(schema = "warehouse", table = table_name),
    df,
    overwrite = TRUE
  )
}

append_data_to_warehouse <- function(con_to, table_name, df) {
  df <- add_import_datetime(df)
  DBI::dbWriteTable(
    con_to,
    DBI::Id(schema = "warehouse", table = table_name),
    df,
    append = TRUE
  )
}

#' Ensure the auditing timestamp column exists as the last field
add_import_datetime <- function(df) {
  if ("ir_add_datetime" %in% names(df)) {
    df <- dplyr::select(df, -ir_add_datetime)
  }

  timestamp <- as.POSIXct(Sys.time(), tz = "UTC")
  df$ir_add_datetime <- timestamp
  df
}

# This returns TRUE if every row in the specified column is unique.
check_unique <- function(table_name, col_name) {
  # table_name: data frame
  # col_name: string (column name)

  x <- table_name[[col_name]]

  length(unique(x)) == nrow(table_name)
}

# This returns TRUE if every row in the specified column has a non-NA value.
check_nonmissing <- function(table_name, col_name) {
  # table_name: data frame
  # col_name: string (column name)

  x <- table_name[[col_name]]

  all(!is.na(x))
}

# This returns TRUE if all values in the specified column are unique and
# non-missing; it returns FALSE if there are any missing values or duplicates.
check_unique_nonmissing <- function(table_name, col_name) {
  # table_name: data frame
  # col_name: string (column name)

  x <- table_name[[col_name]]

  all(!is.na(x)) && length(unique(x)) == nrow(table_name)
}

#' Build a structured validation result
#'
#' @param name A descriptive name for the validation (usually column + check).
#' @param success Logical; `TRUE` when the check passed, `FALSE` otherwise.
#' @param total Integer; total number of records evaluated.
#' @param fails Integer; number of rows that failed the check.
#' @param missing Integer; number of rows with `NA` in the target column.
#' @param message Optional string to explain the validation outcome.
#' @param bad_values Optional tibble with examples of failing values.
#' @return A list containing the validation metadata + diagnostics; suitable for reporting or nested summaries.
#' @keywords internal
make_validation_result <- function(name,
                                   success,
                                   total = NA_integer_,
                                   fails = 0L,
                                   missing = 0L,
                                   message = NULL,
                                   bad_values = NULL) {
  list(
    name = name,
    success = success,
    total = total,
    fails = fails,
    missing = missing,
    message = message,
    bad_values = bad_values
  )
}

#' Collect the most frequent failing values for diagnostics
#'
#' @param values Vector of failed values extracted from a column.
#' @param n Maximum number of examples to return.
#' @return Tibble with `value` and `count`, sorted by frequency.
#' @keywords internal
collect_bad_examples <- function(values, n = 5L) {
  values <- values[!is.na(values)]
  if (length(values) == 0L) {
    return(tibble::tibble(value = character(), count = integer()))
  }

  freq <- tibble::tibble(value = as.character(values))
  counts <- sort(table(freq$value), decreasing = TRUE)
  if (length(counts) > n) {
    counts <- head(counts, n)
  }

  tibble::tibble(
    value = names(counts),
    count = as.integer(counts)
  )
}

#' Stop execution when a validation/reporting check fails
#'
#' @param result Validation result list returned by the helpers.
#' @param abort Logical flag; if `TRUE`, raise an error when `result$success` is `FALSE`.
#' @return Invisibly returns `result` when no error is raised.
#' @keywords internal
abort_on_failure <- function(result, abort) {
  if (!abort || result$success) {
    return(invisible(result))
  }

  failure_message <- result$message
  if (is.null(failure_message) || failure_message == "") {
    failure_message <- paste0(result$name, " validation failed.")
  }

  stop(failure_message, call. = FALSE)
}

#' Compare an object to a supported expected type name
#'
#' @param vec Column vector being checked.
#' @param expected Character string naming the expected type (e.g., `"character"`, `"Date"`).
#' @keywords internal
expected_type_matches <- function(vec, expected) {
  switch(expected,
    character = is.character(vec),
    numeric = is.numeric(vec),
    integer = {
      is.integer(vec) || (
        is.numeric(vec) &&
          all(vec[!is.na(vec)] == floor(vec[!is.na(vec)]), na.rm = TRUE)
      )
    },
    double = is.double(vec),
    logical = is.logical(vec),
    Date = inherits(vec, "Date"),
    POSIXct = inherits(vec, "POSIXct"),
    factor = is.factor(vec),
    stop("Unsupported expected type: ", expected)
  )
}

#' Validate that a column's class matches the expected type(s)
#'
#' @param data Data frame or tibble containing the column to check.
#' @param column Column name (string) to validate.
#' @param expected One or more expected class names.
#' @param abort_on_failure When `TRUE`, throw an error if the check fails.
#' @return Structured validation result; use `result$success` to gate automation.
#' @keywords internal
validate_column_type <- function(data,
                                 column,
                                 expected = c("character", "numeric"),
                                 abort_on_failure = FALSE) {
  vec <- data[[column]]
  total <- length(vec)
  expects <- as.character(expected)
  matches <- any(vapply(expects, expected_type_matches, logical(1), vec = vec))

  result <- make_validation_result(
    name = paste(column, "type"),
    success = matches,
    total = total,
    fails = ifelse(matches, 0L, total),
    missing = sum(is.na(vec)),
    message = if (matches) {
      paste0(column, " is of class ", paste(class(vec), collapse = "/"), ".")
    } else {
      paste0(column, " contains values whose class is ", paste(class(vec), collapse = "/"), ", not one of: ", paste(expects, collapse = ", "), ".")
    },
    bad_values = if (matches) NULL else tibble::tibble(type = paste(class(vec), collapse = "/"))
  )

  abort_on_failure(result, abort_on_failure)
}

#' Check that a column only contains values from an allowed set
#'
#' @param data Source tibble.
#' @param column Target column name.
#' @param allowed_values Character vector of permitted values.
#' @param reference_tbl Optional reference table whose column provides allowed values.
#' @param reference_column Column in `reference_tbl` to read from when `allowed_values` is `NULL`.
#' @param allow_missing If `FALSE`, missing values count as failures.
#' @param abort_on_failure When `TRUE`, stop execution when the check fails.
#' @return Validation result list with summaries and optional bad values.
#' @keywords internal
validate_allowed_values <- function(data,
                                    column,
                                    allowed_values = NULL,
                                    reference_tbl = NULL,
                                    reference_column = column,
                                    allow_missing = TRUE,
                                    abort_on_failure = FALSE) {
  if (is.null(allowed_values) && !is.null(reference_tbl)) {
    allowed_values <- unique(reference_tbl[[reference_column]])
  }

  if (is.null(allowed_values)) {
    stop("Either allowed_values or reference_tbl must be supplied.", call. = FALSE)
  }

  vec <- data[[column]]
  total <- length(vec)
  missing <- sum(is.na(vec))
  bad <- which(!is.na(vec) & !(vec %in% allowed_values))
  bad_values <- collect_bad_examples(vec[bad])

  if (allow_missing) {
    fail_count <- length(bad)
  } else {
    fail_count <- length(bad) + missing
  }

  result <- make_validation_result(
    name = paste(column, "allowed values"),
    success = fail_count == 0L,
    total = total,
    fails = fail_count,
    missing = missing,
    message = if (fail_count == 0L) {
      paste0(column, " is within the allowed set.")
    } else {
      paste0("Found ", fail_count, " invalid value(s) in ", column, ".")
    },
    bad_values = bad_values
  )

  abort_on_failure(result, abort_on_failure)
}

#' Assert that values match a regular expression pattern
#'
#' @param data Source tibble with the column to check.
#' @param column Column name containing the values to validate.
#' @param pattern Regex that describes the allowed format.
#' @param description Human-friendly label (used in messages).
#' @param allow_missing If `FALSE`, `NA` values count as failures.
#' @param abort_on_failure When `TRUE`, raise an error for failing checks.
#' @return Structured validation result.
#' @keywords internal
validate_regex_format <- function(data,
                                  column,
                                  pattern,
                                  description = NULL,
                                  allow_missing = TRUE,
                                  abort_on_failure = FALSE) {
  vec <- data[[column]]
  total <- length(vec)
  missing <- sum(is.na(vec))
  if (is.null(description)) {
    description <- column
  }
  bad <- which(!is.na(vec) & !stringr::str_detect(vec, pattern))
  bad_values <- collect_bad_examples(vec[bad])

  fail_count <- if (allow_missing) {
    length(bad)
  } else {
    length(bad) + missing
  }

  result <- make_validation_result(
    name = paste(column, "format"),
    success = fail_count == 0L,
    total = total,
    fails = fail_count,
    missing = missing,
    message = if (fail_count == 0L) {
      paste0(column, " matches the expected format for ", description, ".")
    } else {
      paste0("Found ", fail_count, " value(s) that fail the ", description, " format check.")
    },
    bad_values = bad_values
  )

  abort_on_failure(result, abort_on_failure)
}

#' Validate that numeric or date values fall within an expected range
#'
#' @param data Source tibble.
#' @param column Column name to inspect.
#' @param min_value Lower bound (inclusive).
#' @param max_value Upper bound (inclusive).
#' @param allow_missing If `FALSE`, `NA`s are treated as failures.
#' @param abort_on_failure Stop execution when the check fails.
#' @return Validation result list.
#' @keywords internal
validate_range <- function(data,
                           column,
                           min_value = NULL,
                           max_value = NULL,
                           allow_missing = TRUE,
                           abort_on_failure = FALSE) {
  vec <- data[[column]]
  total <- length(vec)
  missing <- sum(is.na(vec))
  comparator <- rep(TRUE, length(vec))

  if (!is.null(min_value)) {
    comparator <- comparator & vec >= min_value
  }
  if (!is.null(max_value)) {
    comparator <- comparator & vec <= max_value
  }

  comparator[is.na(vec)] <- TRUE
  bad <- which(!comparator)
  bad_values <- collect_bad_examples(vec[bad])

  fail_count <- if (allow_missing) {
    length(bad)
  } else {
    length(bad) + missing
  }

  range_label <- paste0(
    if (!is.null(min_value)) paste0("≥", min_value) else "",
    if (!is.null(min_value) & !is.null(max_value)) ", " else "",
    if (!is.null(max_value)) paste0("≤", max_value) else ""
  )

  range_label <- trimws(range_label)
  if (range_label == "") {
    range_label <- "unbounded"
  }

  result <- make_validation_result(
    name = paste(column, "range"),
    success = fail_count == 0L,
    total = total,
    fails = fail_count,
    missing = missing,
    message = if (fail_count == 0L) {
      paste0(column, " is within the expected range (", range_label, ").")
    } else {
      paste0("Found ", fail_count, " value(s) outside the allowed range for ", column, ".")
    },
    bad_values = bad_values
  )

  abort_on_failure(result, abort_on_failure)
}

#' Ensure column values match a foreign-key lookup table
#'
#' @param data Source tibble.
#' @param column Column whose values should exist in the lookup.
#' @param lookup_tbl Reference table that defines permitted values.
#' @param lookup_column Column from `lookup_tbl` to compare against.
#' @param allow_missing Whether `NA` values should be counted as failures.
#' @param abort_on_failure If `TRUE`, stop on failure.
#' @return Validation result containing diagnostics.
#' @keywords internal
validate_lookup_match <- function(data,
                                  column,
                                  lookup_tbl,
                                  lookup_column,
                                  allow_missing = TRUE,
                                  abort_on_failure = FALSE) {
  vec <- data[[column]]
  total <- length(vec)
  missing <- sum(is.na(vec))
  allowed <- unique(lookup_tbl[[lookup_column]])
  bad <- which(!is.na(vec) & !(vec %in% allowed))
  bad_values <- collect_bad_examples(vec[bad])

  fail_count <- if (allow_missing) {
    length(bad)
  } else {
    length(bad) + missing
  }

  result <- make_validation_result(
    name = paste(column, "lookup match"),
    success = fail_count == 0L,
    total = total,
    fails = fail_count,
    missing = missing,
    message = if (fail_count == 0L) {
      paste0(column, " matches the lookup table on ", lookup_column, ".")
    } else {
      paste0("Found ", fail_count, " value(s) that do not match the reference lookup for ", column, ".")
    },
    bad_values = bad_values
  )

  abort_on_failure(result, abort_on_failure)
}

#' Apply common checks to university identifier columns
#'
#' @param data Source tibble.
#' @param column Column storing the university identifier.
#' @param lookup_tbl Optional table of allowed identifiers (e.g., from `warehouse` schema).
#' @param lookup_column Column in `lookup_tbl` to compare against; defaults to `column`.
#' @param abort_on_failure If `TRUE`, raise an error when a validation fails.
#' @return List of validation results (type, plus lookup when provided).
#' @keywords internal
validate_university_id <- function(data,
                                   column = "UniversityId",
                                   lookup_tbl = NULL,
                                   lookup_column = NULL,
                                   abort_on_failure = FALSE) {
  if (is.null(lookup_column)) {
    lookup_column <- column
  }
  results <- list(
    type = validate_column_type(data, column, expected = c("integer", "numeric"), abort_on_failure = abort_on_failure)
  )

  if (!is.null(lookup_tbl)) {
    results$lookup <- validate_lookup_match(
      data,
      column,
      lookup_tbl,
      lookup_column,
      abort_on_failure = abort_on_failure
    )
  }

  results
}

#' Validate `RecruitmentYear` values against a sensible range
#'
#' @param data Source tibble.
#' @param column Column name (default `RecruitmentYear`).
#' @param min_year Earliest plausible year.
#' @param allow_missing Whether `NA` values are acceptable.
#' @param abort_on_failure When `TRUE`, stop on failure.
#' @return List containing the `range` validation object.
#' @keywords internal
validate_recruitment_year <- function(data,
                                      column = "RecruitmentYear",
                                      min_year = 2000,
                                      allow_missing = TRUE,
                                      abort_on_failure = FALSE) {
  max_year <- as.integer(format(Sys.Date(), "%Y"))
  range_result <- validate_range(
    data,
    column,
    min_value = min_year,
    max_value = max_year,
    allow_missing = allow_missing,
    abort_on_failure = abort_on_failure
  )

  list(range = range_result)
}

#' Validate gender codes against either a list or lookup
#'
#' @param data Source tibble.
#' @param column Column name storing the gender code.
#' @param allowed Character vector of allowed values.
#' @param allow_missing Whether `NA` is permitted.
#' @param abort_on_failure Stop on failure when `TRUE`.
#' @return Validation result object.
#' @keywords internal
validate_gender <- function(data,
                            column = "Gender",
                            allowed = c("F", "M", "O", "U", "Female", "Male", "Other", "Unknown"),
                            allow_missing = TRUE,
                            abort_on_failure = FALSE) {
  validate_allowed_values(
    data,
    column,
    allowed_values = allowed,
    allow_missing = allow_missing,
    abort_on_failure = abort_on_failure
  )
}

#' Validate race codes via lookup or canned set
#'
#' @param data Source tibble.
#' @param column Name of the race column.
#' @param lookup_tbl Optional reference table for allowed races.
#' @param lookup_column Column name inside `lookup_tbl` (defaults to `column`).
#' @param allow_missing Whether `NA` is acceptable.
#' @param abort_on_failure Stop execution when failing.
#' @return Validation result list.
#' @keywords internal
validate_race <- function(data,
                          column = "Race",
                          lookup_tbl = NULL,
                          lookup_column = NULL,
                          allow_missing = TRUE,
                          abort_on_failure = FALSE) {
  if (!is.null(lookup_tbl)) {
    if (is.null(lookup_column)) {
      lookup_column <- column
    }
    return(validate_lookup_match(
      data,
      column,
      lookup_tbl,
      lookup_column,
      allow_missing = allow_missing,
      abort_on_failure = abort_on_failure
    ))
  }

  validate_allowed_values(
    data,
    column,
    allowed_values = c("White", "Black", "Asian", "Hispanic", "Unknown", "Other"),
    allow_missing = allow_missing,
    abort_on_failure = abort_on_failure
  )
}

#' Ensure ZIP code columns conform to ZIP + optional ZIP+4
#'
#' @param data Source tibble.
#' @param column Column storing ZIP codes.
#' @param allow_missing Whether missing ZIP codes are acceptable.
#' @param abort_on_failure Stop execution when the check fails.
#' @return Validation result object.
#' @keywords internal
validate_zip <- function(data,
                         column = "Zip",
                         allow_missing = TRUE,
                         abort_on_failure = FALSE) {
  validate_regex_format(
    data,
    column,
    pattern = "^[0-9]{5}(-[0-9]{4})?$",
    description = "ZIP or ZIP+4",
    allow_missing = allow_missing,
    abort_on_failure = abort_on_failure
  )
}

#' Wrapper to validate any column against a lookup table
#'
#' @inheritParams validate_lookup_match
#' @return Validation result.
#' @keywords internal
validate_lookup_column <- function(data,
                                   column,
                                   lookup_tbl,
                                   lookup_column = column,
                                   allow_missing = TRUE,
                                   abort_on_failure = FALSE) {
  validate_lookup_match(data, column, lookup_tbl, lookup_column, allow_missing, abort_on_failure)
}

#' Summarize multiple validation results for reporting
#'
#' @param results Named or unnamed list of validation results (each produced by `make_validation_result`).
#' @return Tibble with one row per validation that includes the success flag, failure count, message, and the stored bad value examples.
#' @keywords internal
summarize_validation_results <- function(results) {
  names <- vapply(results, `[[`, character(1), "name")
  messages <- vapply(results, function(res) {
    msg <- res$message
    if (is.null(msg)) {
      ""
    } else {
      msg
    }
  }, character(1))

  tibble::tibble(
    name = names,
    success = vapply(results, `[[`, logical(1), "success"),
    fails = vapply(results, `[[`, integer(1), "fails"),
    message = messages,
    bad_values = lapply(results, `[[`, "bad_values")
  )
}
#' Print section header for pairwise null analysis
#'
#' @param filter_column The column being used to filter on NA values.
#' @param filter_count Sequential count (used for lettering a, b, c, etc.).
#' @param letters Vector of letters for labeling sections (defaults to R's built-in letters vector).
#' @keywords internal
print_pairwise_section_header <- function(filter_column, filter_count, letters = base::letters) {
  cat("=======================================================\n")
  cat(paste0(letters[filter_count], ") filter_column = ", filter_column, "\n"))
  cat("=======================================================\n")
}

#' Print analysis info for pairwise null analysis
#'
#' @param column_to_analyze The column being analyzed for data impact.
#' @param filter_column The column being filtered on NA values.
#' @param analyze_count Sequential count (used for lettering).
#' @param letters Vector of letters for labeling (defaults to R's built-in letters vector).
#' @keywords internal
print_pairwise_analysis_info <- function(column_to_analyze, filter_column, analyze_count, letters = base::letters) {
  cat("-------------------------------------------------------\n")
  cat(paste0(
    "Analyzing relationship between ", column_to_analyze,
    " and ", filter_column, "\n"
  ))
  cat(paste0(letters[analyze_count], ") column_to_analyze = ", column_to_analyze, "\n"))
  cat(paste0("  -- The first column has values of ", column_to_analyze, "\n"))
  cat(paste0("  -- The second column has total counts of ", column_to_analyze, "\n"))
  cat(paste0("  -- The NA_V_R has count of ", column_to_analyze, " when\n     ", filter_column, " is NA\n"))
  cat(paste0("  -- The Has_V_R has count of ", column_to_analyze, " when\n     ", filter_column, " has a value\n"))
}

#' Print section headers for threeway null analysis
#'
#' @param col_with_na_values The column we're removing NAs from.
#' @param outer_count Sequential count for outer loop.
#' @param letters Vector of letters for labeling (defaults to R's built-in letters vector).
#' @keywords internal
print_threeway_section_headers <- function(col_with_na_values, outer_count, letters = base::letters) {
  cat("*****************************************************\n")
  cat("*****************************************************\n")
  cat(paste0(letters[outer_count], ") col_with_na_values = ", col_with_na_values, "\n"))
  cat("-----------------------------------------------------\n")
}

#' Print analysis info for threeway null analysis
#'
#' @param column_to_analyze The column being analyzed.
#' @param filter_column The column being filtered on NA values.
#' @param col_with_na_values The column we're removing NAs from.
#' @param middle_count Count for middle loop.
#' @param inner_count Count for inner loop.
#' @param letters Vector of letters for labeling (defaults to R's built-in letters vector).
#' @keywords internal
print_threeway_analysis_info <- function(column_to_analyze, filter_column, col_with_na_values, middle_count, inner_count, na_filter_state = NULL, letters = base::letters) {
  cat(paste0(
    "To see relationship between ", column_to_analyze,
    " and ", filter_column, "\nin absence of null values in ",
    col_with_na_values, ".\n"
  ))
  cat(paste0("Only include rows for which ", col_with_na_values, " has a value.\n"))
  cat(paste0(letters[middle_count], ") filter_column = ", filter_column, "\n"))
  cat(paste0(letters[inner_count], ") column_to_analyze = ", column_to_analyze, "\n"))
  cat(paste0("  -- The first column has values of ", column_to_analyze, "\n"))
  cat(paste0("  -- The second column has total counts of ", column_to_analyze, "\n"))
  cat(paste0("  -- The NA_V_R has count of ", column_to_analyze, " when\n     ", filter_column, " is NA\n"))
  cat(paste0("  -- The Has_V_R has count of ", column_to_analyze, " when\n     ", filter_column, " has a value\n"))
}

#' Infer whether a column is categorical or presence-based
#'
#' @param data Source data frame.
#' @param column Column name to infer type for.
#'
#' @return Character: either "categorical" (for logical, factor, character) or "presence" (for everything else).
#' @keywords internal
infer_column_type <- function(data, column) {
  vec <- data[[column]]
  if (is.logical(vec) || is.factor(vec) || is.character(vec)) {
    "categorical"
  } else {
    "presence"
  }
}

#' Analyze pairwise filter impact on data
#'
#' Examines how filtering rows by NA values in one column affects the distribution
#' of values in another column. This is a two-way analysis with no exclusions.
#' Column type is automatically inferred from the data.
#'
#' @param data Source data frame.
#' @param column_to_analyze The column whose values will be examined for distribution changes.
#' @param filter_column The column to filter on (NA vs. non-NA).
#'
#' @return A tibble comparing value distributions in the analyzed column when the
#'   filter_column is NA versus when it has a value.
#' @keywords internal
analyze_pairwise_filter_impact <- function(data,
                                           column_to_analyze,
                                           filter_column) {
  column_type <- infer_column_type(data, column_to_analyze)

  if (column_type == "categorical") {
    # Pull values from all rows and filtered rows
    all_vals <- data |> pull(!!sym(column_to_analyze))

    filtered_vals <- data |>
      filter(is.na(!!sym(filter_column))) |>
      pull(!!sym(column_to_analyze))

    # Create comparison table for categorical columns
    comparison <- tibble(
      Value = c("TRUE", "FALSE", "NA"),
      All_Rows = c(
        sum(all_vals == TRUE, na.rm = TRUE),
        sum(all_vals == FALSE, na.rm = TRUE),
        sum(is.na(all_vals))
      ),
      NA_Value_Rows = c(
        sum(filtered_vals == TRUE, na.rm = TRUE),
        sum(filtered_vals == FALSE, na.rm = TRUE),
        sum(is.na(filtered_vals))
      )
    ) |>
      mutate(Has_Value_Rows = All_Rows - NA_Value_Rows)
  } else if (column_type == "presence") {
    # Check value presence/absence patterns (works for dates, numerics, etc.)
    all_data <- data |>
      mutate(has_value = !is.na(!!sym(column_to_analyze)))

    filtered_data <- data |>
      filter(is.na(!!sym(filter_column))) |>
      mutate(has_value = !is.na(!!sym(column_to_analyze)))

    comparison <- tibble(
      Status = c("Has value", "Is NA"),
      All_Rows = c(
        sum(all_data$has_value),
        sum(!all_data$has_value)
      ),
      NA_Value_Rows = c(
        sum(filtered_data$has_value),
        sum(!filtered_data$has_value)
      )
    ) |>
      mutate(Has_Value_Rows = All_Rows - NA_Value_Rows)
  }

  return(comparison)
}

#' Analyze threeway filter impact on data
#'
#' Examines how filtering by NA values in two columns affects the distribution of
#' a third column. First excludes rows where `col_with_na_values` is NA, then
#' filters by `filter_column` to see data loss patterns.
#' Column type is automatically inferred from the data.
#'
#' @param data Source data frame.
#' @param column_to_analyze The column whose values will be examined for distribution changes.
#' @param filter_column The column to filter on (NA vs. non-NA).
#' @param col_with_na_values The column that will be pre-filtered (only rows where this is non-NA).
#'
#' @return A tibble comparing value distributions in the analyzed column, restricted to
#'   rows where `col_with_na_values` is non-NA, then further split by `filter_column` NA status.
#' @keywords internal
analyze_threeway_filter_impact <- function(data,
                                           column_to_analyze,
                                           filter_column,
                                           col_with_na_values) {
  column_type <- infer_column_type(data, column_to_analyze)
  filter_column_type <- infer_column_type(data, filter_column)

  # In all cases, we only care about rows where col_with_na_values is NOT NA
  data <- data |> filter(!is.na(!!sym(col_with_na_values)))

  if (column_type == "categorical") {
    all_vals <- data |> pull(!!sym(column_to_analyze))

    # Check if filter_column is categorical/logical
    if (filter_column_type == "categorical") {
      # Get unique values of filter_column (excluding NA)
      filter_values <- data |>
        pull(!!sym(filter_column)) |>
        unique() |>
        sort()
      filter_values <- filter_values[!is.na(filter_values)]

      # Create comparison table with columns for each filter value + NA_Value_Rows
      comparison <- tibble(
        Value = c("TRUE", "FALSE", "NA"),
        All_Rows = c(
          sum(all_vals == TRUE, na.rm = TRUE),
          sum(all_vals == FALSE, na.rm = TRUE),
          sum(is.na(all_vals))
        )
      )

      # Add NA_Value_Rows column (where filter_column is NA)
      na_filtered_vals <- data |>
        filter(is.na(!!sym(filter_column))) |>
        pull(!!sym(column_to_analyze))

      comparison <- comparison |>
        mutate(
          NA_Value_Rows = c(
            sum(na_filtered_vals == TRUE, na.rm = TRUE),
            sum(na_filtered_vals == FALSE, na.rm = TRUE),
            sum(is.na(na_filtered_vals))
          )
        )

      # Add a column for each unique value in filter_column
      for (val in filter_values) {
        col_name <- paste0("Value=", val)
        filtered_vals <- data |>
          filter(!!sym(filter_column) == val) |>
          pull(!!sym(column_to_analyze))

        comparison <- comparison |>
          mutate(
            !!col_name := c(
              sum(filtered_vals == TRUE, na.rm = TRUE),
              sum(filtered_vals == FALSE, na.rm = TRUE),
              sum(is.na(filtered_vals))
            )
          )
      }

      # Reorder columns: Value, All_Rows, NA_Value_Rows, then Value=* columns
      comparison <- comparison |>
        select(Value, All_Rows, NA_Value_Rows, everything())
    } else {
      # filter_column is presence-based, use original behavior
      filtered_vals <- data |>
        filter(is.na(!!sym(filter_column))) |>
        pull(!!sym(column_to_analyze))

      # Create comparison table for categorical columns
      comparison <- tibble(
        Value = c("TRUE", "FALSE", "NA"),
        All_Rows = c(
          sum(all_vals == TRUE, na.rm = TRUE),
          sum(all_vals == FALSE, na.rm = TRUE),
          sum(is.na(all_vals))
        ),
        NA_Value_Rows = c(
          sum(filtered_vals == TRUE, na.rm = TRUE),
          sum(filtered_vals == FALSE, na.rm = TRUE),
          sum(is.na(filtered_vals))
        )
      ) |>
        mutate(Has_Value_Rows = All_Rows - NA_Value_Rows)
    }
  } else if (column_type == "presence") {
    # Check value presence/absence patterns (works for dates, numerics, etc.)
    all_data <- data |>
      mutate(has_value = !is.na(!!sym(column_to_analyze)))

    # Check if filter_column is categorical/logical
    if (filter_column_type == "categorical") {
      # Get unique values of filter_column (excluding NA)
      filter_values <- data |>
        pull(!!sym(filter_column)) |>
        unique() |>
        sort()
      filter_values <- filter_values[!is.na(filter_values)]

      # Create comparison table with columns for each filter value + NA_Value_Rows
      comparison <- tibble(
        Status = c("Has value", "Is NA"),
        All_Rows = c(
          sum(all_data$has_value),
          sum(!all_data$has_value)
        )
      )

      # Add NA_Value_Rows column (where filter_column is NA)
      na_filtered_data <- data |>
        filter(is.na(!!sym(filter_column))) |>
        mutate(has_value = !is.na(!!sym(column_to_analyze)))

      comparison <- comparison |>
        mutate(
          NA_Value_Rows = c(
            sum(na_filtered_data$has_value),
            sum(!na_filtered_data$has_value)
          )
        )

      # Add a column for each unique value in filter_column
      for (val in filter_values) {
        col_name <- paste0("Value=", val)
        filtered_data <- data |>
          filter(!!sym(filter_column) == val) |>
          mutate(has_value = !is.na(!!sym(column_to_analyze)))

        comparison <- comparison |>
          mutate(
            !!col_name := c(
              sum(filtered_data$has_value),
              sum(!filtered_data$has_value)
            )
          )
      }

      # Reorder columns: Status, All_Rows, NA_Value_Rows, then Value=* columns
      comparison <- comparison |>
        select(Status, All_Rows, NA_Value_Rows, everything())
    } else {
      # filter_column is presence-based, use original behavior
      filtered_data <- data |>
        filter(is.na(!!sym(filter_column))) |>
        mutate(has_value = !is.na(!!sym(column_to_analyze)))

      comparison <- tibble(
        Status = c("Has value", "Is NA"),
        All_Rows = c(
          sum(all_data$has_value),
          sum(!all_data$has_value)
        ),
        NA_Value_Rows = c(
          sum(filtered_data$has_value),
          sum(!filtered_data$has_value)
        )
      ) |>
        mutate(Has_Value_Rows = All_Rows - NA_Value_Rows)
    }
  }

  return(comparison)
}
