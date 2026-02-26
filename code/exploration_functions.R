# Exploration Functions for Database Discovery
#
# A personal library of functions for systematically exploring
# unfamiliar databases. Use these when you start at a new organization
# and need to understand what data you have.
#
# Load these functions with: source("code/exploration_functions.R")

library(dplyr)
library(tibble)
library(tidyr)
library(dbplyr)
library(DBI)
library(duckplyr)
library(here)

# ============================================================
# PART 1: Getting Started
# ============================================================

# Get list of all tables in database
#
# Parameters:
#   con - database connection
#
# Returns: tibble with one row per table
get_table_list <- function(con) {
  tibble(
    table_name = DBI::dbListTables(con)
  )
}


# Describe the structure of a table
#
# Parameters:
#   con - database connection
#   table_name - name of table to describe (without schema prefix)
#   schema_name - name of schema (default: NULL, set to specify schema)
#   column_type - optional filter to show only columns of a certain type (e.g., "character")
#
# Returns: tibble with column names, types, example values, and min/max values
describe_table <- function(con, table_name, schema_name = NULL, column_type = NULL) {
  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- DBI::Id(table = table_name)
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  # Get column names
  column_names <- DBI::dbListFields(con, table_id)

  # Collect the full table for analysis
  tbl_data <- con |>
    tbl(table_id) |>
    collect()

  # Get example value from first row
  example_value <- tbl_data |>
    head(1) |>
    as.list()

  # Get data types from R (including any factors applied by post_read_processing)
  data_types <- sapply(tbl_data, function(col) class(col)[1])

  # Calculate min and max values for each column (only for numeric/date types)
  min_vals <- sapply(column_names, function(col) {
    col_class <- data_types[col]
    # Only calculate min/max for numeric and date types
    if (col_class %in% c("integer", "numeric", "Date", "POSIXct")) {
      tryCatch(
        {
          val <- min(tbl_data[[col]], na.rm = TRUE)
          if (is.infinite(val)) "NA" else as.character(val)
        },
        error = function(e) "NA"
      )
    } else {
      "NA"
    }
  }, USE.NAMES = FALSE)

  max_vals <- sapply(column_names, function(col) {
    col_class <- data_types[col]
    # Only calculate min/max for numeric and date types
    if (col_class %in% c("integer", "numeric", "Date", "POSIXct")) {
      tryCatch(
        {
          val <- max(tbl_data[[col]], na.rm = TRUE)
          if (is.infinite(val)) "NA" else as.character(val)
        },
        error = function(e) "NA"
      )
    } else {
      "NA"
    }
  }, USE.NAMES = FALSE)

  # Create result tibble
  result <- tibble(
    column_name = column_names,
    data_type = as.character(data_types[column_names]),
    example_val = sapply(column_names, function(col) {
      val <- example_value[[col]]
      if (length(val) == 0 || is.na(val)) "NA" else as.character(val)
    }, USE.NAMES = FALSE),
    min_val = min_vals,
    max_val = max_vals
  )

  # Filter by type if requested
  if (!is.null(column_type)) {
    result <- result |> filter(data_type == column_type)
  }

  result
}

get_schema_names <- function(con, cat_name) {
  con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "schemata"
    )) |>
    filter(catalog_name == cat_name) |>
    select(schema_name) |>
    collect() |>
    arrange(schema_name)
}

get_table_names <- function(con, cat_name) {
  con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "tables"
    )) |>
    filter(
      table_catalog == cat_name,
      table_schema != "pg_catalog",
      table_schema != "information_schema"
    ) |>
    select(table_schema, table_name) |>
    collect() |>
    arrange(table_schema, table_name)
}

get_tables_in_schema <- function(con, cat_name, the_schema_name) {
  con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "tables"
    )) |>
    filter(
      table_catalog == cat_name,
      table_schema != "pg_catalog",
      table_schema != "information_schema"
    ) |>
    filter(table_schema == the_schema_name) |>
    select(table_name) |>
    collect() |>
    arrange(table_name)
}


# ============================================================
# PART 2: Understanding What You Have
# ============================================================

# Get statistics about a table
#
# Parameters:
#   con - database connection
#   table_name - name of table to analyze
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: tibble with row count, column count, null count, and null percentage
table_stats <- function(con, table_name, schema_name = NULL) {
  # Parse schema from table_name if provided in "schema.table" format
  if (is.null(schema_name) && grepl("\\.", table_name)) {
    parts <- strsplit(table_name, "\\.")[[1]]
    schema_name <- parts[1]
    table_name <- parts[2]
  }

  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- DBI::Id(table = table_name)
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  tbl_ref <- con %>%
    tbl(table_id) |>
    collect()

  # Get row and column counts
  n_rows <- nrow(tbl_ref)
  n_cols <- length(DBI::dbListFields(con, table_id))

  # Get display name for result
  if (is.null(schema_name)) {
    display_name <- table_name
  } else {
    display_name <- paste0(schema_name, ".", table_name)
  }

  # Calculate null statistics
  num_nulls <- sum(is.na(tbl_ref))
  total_values <- n_rows * n_cols
  perc_nulls <- round(100 * num_nulls / total_values, 2)

  # Get description
  tibble(
    table_name = display_name,
    n_rows = n_rows,
    n_columns = n_cols,
    num_nulls = num_nulls,
    perc_nulls = perc_nulls
  )
}


# Find columns matching a pattern across tables
#
# Parameters:
#   con - database connection
#   pattern - text pattern to search for in column names
#   schema_name - name of schema to search (default: NULL searches all schemas)
#
# Returns: tibble showing which tables have matching columns
find_key_columns <- function(con, pattern, schema_name = NULL) {
  # Query information_schema for columns matching the pattern
  if (is.null(schema_name)) {
    # Search all schemas except system schemas
    columns_info <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "columns"
      )) |>
      filter(
        table_schema != "pg_catalog",
        table_schema != "information_schema"
      ) |>
      select(table_schema, table_name, column_name) |>
      collect()
  } else {
    # Search only the specified schema
    columns_info <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "columns"
      )) |>
      filter(table_schema == schema_name) |>
      select(table_schema, table_name, column_name) |>
      collect()
  }

  # Filter columns matching the pattern
  results <- columns_info |>
    filter(grepl(pattern, column_name, ignore.case = TRUE)) |>
    select(table_name, column_name) |>
    distinct() |>
    arrange(table_name, column_name)

  results
}

# Find already defined primary keys
# Parameters:
#   con - database connection
#   schema_name - name of schema to search (default: NULL searches all schemas)
#
# Returns: tibble showing tables and their primary key columns; note
# that these are only columns that are defined as primary keys in the
# database schema, so there may be other key columns that are not
# formally defined as such.
find_primary_keys <- function(con, schema_name = NULL) {
  # Query information_schema for primary key columns
  if (is.null(schema_name)) {
    pk_info <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "table_constraints"
      )) |>
      filter(constraint_type == "PRIMARY KEY") |>
      select(table_schema, table_name, constraint_name) |>
      collect()
  } else {
    pk_info <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "table_constraints"
      )) |>
      filter(
        constraint_type == "PRIMARY KEY",
        table_schema == schema_name
      ) |>
      select(table_schema, table_name, constraint_name) |>
      collect()
  }

  # Join with key column usage to get column names
  pk_columns <- con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "key_column_usage"
    )) |>
    select(table_schema, table_name, constraint_name, column_name) |>
    collect()

  # Combine and format results
  pk_info |>
    inner_join(pk_columns, by = c("table_schema", "table_name", "constraint_name")) |>
    select(table_schema, table_name, column_name) |>
    arrange(table_schema, table_name)
}

# Test columns to see if they could serve as a primary key (i.e., are unique and are not null)
# Parameters:
#   con - database connection
#   table_name - name of table to test (without schema prefix)
#   column_names - column name as a string (e.g., "id") or vector of column names (e.g., c("id", "school_year")) to test as a key
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: a logical value indicating whether the specified columns could serve as a primary key, along with a message about duplicates if they exist. Note that this function checks for uniqueness and nulls in the current data, but it does not check for formally defined primary keys in the database schema.
test_columns_as_primary_key <- function(con, table_name, column_names, schema_name = NULL) {
  # Ensure column_names is a character vector (handles both single string and vector input)
  column_names <- as.character(column_names)

  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- DBI::Id(table = table_name)
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  tbl_ref <- con |>
    tbl(table_id) |>
    collect()

  # Check for duplicates in the specified columns
  duplicate_count <- tbl_ref |>
    group_by(across(all_of(column_names))) |>
    filter(n() > 1) |>
    summarise(duplicate_rows = n(), .groups = "drop") |>
    summarise(total_duplicates = coalesce(sum(duplicate_rows), 0L)) |>
    pull(total_duplicates)

  if (duplicate_count == 0) {
    cat("Columns", paste(column_names, collapse = ", "), "are unique and could serve as a primary key.\n")
    return_val <- TRUE
  } else {
    cat("Columns", paste(column_names, collapse = ", "), "have", duplicate_count, "duplicate rows and cannot serve as a primary key.\n")
    return_val <- FALSE
  }
  return(return_val)
}

# Find the already-declared primary key for a specific table
# Parameters:
#   con - database connection
#   table_name - name of table to test (without schema prefix)
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: vector of column names that are defined as the primary key for the specified table, or NULL if no primary key is defined. Note that this function checks for formally defined primary keys in the database schema, but it does not check for uniqueness or nulls in the current data.
get_primary_key_for_table <- function(con, table_name, schema_name = NULL) {
  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- DBI::Id(table = table_name)
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }
  pk_info <- con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "table_constraints"
    )) |>
    filter(
      constraint_type == "PRIMARY KEY",
      table_name == !!table_name,
      if (!is.null(schema_name)) table_schema == !!schema_name else TRUE
    ) |>
    select(table_schema, table_name, constraint_name) |>
    collect()
  if (nrow(pk_info) == 0) {
    return(NULL) # No primary key defined
  }
  pk_columns <- con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "key_column_usage"
    )) |>
    select(table_schema, table_name, constraint_name, column_name) |>
    collect()
  pk_info |>
    inner_join(pk_columns, by = c("table_schema", "table_name", "constraint_name")) |>
    pull(column_name)
}


# Define a primary key for a table (note: this function only defines the primary key in the database schema; it does not check for uniqueness or nulls in the current data, so use with caution and ideally after testing the columns with test_columns_as_primary_key())
# Parameters:
#   con - database connection
#   table_name - name of table to modify (without schema prefix)
#   column_names - column name as a string (e.g., "id") or vector of column names (e.g., c("id", "school_year")) to set as the primary key
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: None; this function modifies the database schema to set the specified columns as the primary key for the specified table. Note that this operation may fail if there are existing duplicates or nulls in the specified columns, so it is recommended to test the columns with test_columns_as_primary_key() before running this function.
define_primary_key <- function(con, table_name, column_names, schema_name = NULL) {
  # Check if primary key already exists
  existing_pk <- get_primary_key_for_table(con, table_name, schema_name)

  if (!is.null(existing_pk)) {
    cat("Primary key already exists for table", table_name, "\n")
    return(FALSE)
  }

  # Ensure column_names is a character vector (handles both single string and vector input)
  column_names <- as.character(column_names)

  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- DBI::Id(table = table_name)
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  # Create the SQL statement to add a primary key constraint
  constraint_name <- paste0(table_name, "_pk")
  columns_sql <- paste(column_names, collapse = ", ")
  sql <- sprintf(
    "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
    DBI::dbQuoteIdentifier(con, table_id),
    DBI::dbQuoteIdentifier(con, constraint_name),
    columns_sql
  )

  # Execute the SQL statement
  tryCatch(
    {
      DBI::dbExecute(con, sql)
      cat("Primary key defined for table", table_name, "on columns:", paste(column_names, collapse = ", "), "\n")
      return(TRUE)
    },
    error = function(e) {
      cat("Error defining primary key for table", table_name, ":", conditionMessage(e), "\n")
      return(FALSE)
    }
  )
}

# Test columns to see if they could serve as a foreign key (i.e., all values exist in target table)
# Parameters:
#   con - database connection
#   fk_table - name of table with potential foreign key (without schema prefix)
#   fk_column_names - column name(s) in fk_table as a string or vector
#   fk_schema_name - schema name of fk_table (default: NULL, set to specify schema)
#   target_table - name of target table to reference (without schema prefix)
#   target_column_names - column name(s) in target_table as a string or vector (must match fk_column_names in order)
#   target_schema_name - schema name of target_table (default: NULL, set to specify schema)
#
# Returns: TRUE if all FK values exist in target table, FALSE if orphaned records found.
# Prints a report with the number of unmatched values and examples.
test_columns_as_foreign_key <- function(con, fk_table, fk_column_names,
                                        target_table, target_column_names, fk_schema_name = NULL, target_schema_name = NULL) {
  # Ensure column_names are character vectors
  fk_column_names <- as.character(fk_column_names)
  target_column_names <- as.character(target_column_names)

  # Construct table references as strings for tbl()
  if (is.null(fk_schema_name)) {
    fk_table_ref <- fk_table
  } else {
    fk_table_ref <- paste0(fk_schema_name, ".", fk_table)
  }

  if (is.null(target_schema_name)) {
    target_table_ref <- target_table
  } else {
    target_table_ref <- paste0(target_schema_name, ".", target_table)
  }

  # Get the data from both tables
  fk_data <- con |>
    tbl(fk_table_ref) |>
    select(all_of(fk_column_names)) |>
    collect() |>
    distinct()

  target_data <- con |>
    tbl(target_table_ref) |>
    select(all_of(target_column_names)) |>
    collect() |>
    distinct()

  # Rename target columns to match FK column names for the join
  names(target_data) <- fk_column_names

  # Find orphaned records using anti_join
  orphaned <- fk_data |>
    anti_join(target_data, by = fk_column_names)

  orphan_count <- nrow(orphaned)

  if (orphan_count == 0) {
    cat(
      "✓ Foreign key columns", paste(fk_column_names, collapse = ", "),
      "in", fk_table, "are valid (all values exist in", target_table, ")\n"
    )
    return(TRUE)
  } else {
    cat(
      "✗ Foreign key columns", paste(fk_column_names, collapse = ", "),
      "in", fk_table, "have", orphan_count, "orphaned records.\n"
    )
    cat("Examples of unmatched values:\n")
    print(head(orphaned, n = 5))
    return(FALSE)
  }
}

# Define foreign key constraint in the database schema (note: this function only defines the foreign key in the database schema; it does not check for orphaned records in the current data, so use with caution and ideally after testing the columns with test_columns_as_foreign_key())
# Parameters:
#   con - database connection
#   fk_table - name of table with potential foreign key (without schema prefix)
#   fk_column_names - column name(s) in fk_table as a string or vector
#   fk_schema_name - schema name of fk_table (default: NULL, set to specify schema)
#   target_table - name of target table to reference (without schema prefix)
#   target_column_names - column name(s) in target_table as a string or vector (must match fk_column_names in order)
#   target_schema_name - schema name of target_table (default: NULL, set to specify schema)
#
# Returns: logical; this function modifies the database schema to set the specified columns as a foreign key referencing the target table. Note that this operation may fail if there are existing orphaned records in the specified columns, so it is recommended to test the columns with test_columns_as_foreign_key() before running this function. Returns TRUE if the foreign key was successfully defined, or FALSE if there was an error (e.g., due to orphaned records).
define_foreign_key <- function(con, fk_table, fk_column_names,
                               target_table, target_column_names,
                               fk_schema_name = NULL,
                               target_schema_name = NULL) {
  # Ensure column_names are character vectors
  fk_column_names <- as.character(fk_column_names)
  target_column_names <- as.character(target_column_names)

  # Check if foreign key already exists
  constraint_name <- paste0(fk_table, "_", fk_column_names[1], "_fk")

  existing_fk <- con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "table_constraints"
    )) |>
    filter(
      constraint_type == "FOREIGN KEY",
      constraint_name == !!constraint_name
    ) |>
    collect()

  if (nrow(existing_fk) > 0) {
    cat("Foreign key already exists for table", fk_table, "\n")
    return(FALSE)
  }

  # Construct table references as strings for SQL
  if (is.null(fk_schema_name)) {
    fk_table_ref <- fk_table
  } else {
    fk_table_ref <- paste0(fk_schema_name, ".", fk_table)
  }

  if (is.null(target_schema_name)) {
    target_table_ref <- target_table
  } else {
    target_table_ref <- paste0(target_schema_name, ".", target_table)
  }
  # Create the SQL statement to add a foreign key constraint
  fk_columns_sql <- paste(fk_column_names, collapse = ", ")
  target_columns_sql <- paste(target_column_names, collapse = ", ")
  sql <- sprintf(
    "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
    DBI::dbQuoteIdentifier(con, fk_table_ref),
    DBI::dbQuoteIdentifier(con, constraint_name),
    fk_columns_sql,
    DBI::dbQuoteIdentifier(con, target_table_ref),
    target_columns_sql
  )

  # Execute the SQL statement
  tryCatch(
    {
      DBI::dbExecute(con, sql)
      cat("Foreign key defined for table", fk_table, "on columns:", paste(fk_column_names, collapse = ", "), "\n")
      return(TRUE)
    },
    error = function(e) {
      cat("Error defining foreign key for table", fk_table, ":", conditionMessage(e), "\n")
      return(FALSE)
    }
  )
}

# List all primary and foreign key constraints defined in the database
# Parameters:
#   con - database connection
#   schema_name - name of schema to search (default: NULL searches all schemas)
#
# Returns: tibble showing tables, columns, constraint names, and their key constraints (primary or foreign). Note that this function checks for formally defined keys in the database schema, but it does not check for uniqueness or orphaned records in the current data.
list_all_keys <- function(con, schema_name = NULL) {
  # Get primary keys
  if (is.null(schema_name)) {
    pk_info <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "table_constraints"
      )) |>
      filter(constraint_type == "PRIMARY KEY") |>
      select(table_schema, table_name, constraint_name) |>
      collect()
  } else {
    pk_info <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "table_constraints"
      )) |>
      filter(
        constraint_type == "PRIMARY KEY",
        table_schema == schema_name
      ) |>
      select(table_schema, table_name, constraint_name) |>
      collect()
  }

  # Get primary key columns
  pk_columns <- con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "key_column_usage"
    )) |>
    select(table_schema, table_name, constraint_name, column_name) |>
    collect()

  pk_with_cols <- pk_info |>
    inner_join(pk_columns, by = c("table_schema", "table_name", "constraint_name")) |>
    mutate(key_type = "PRIMARY KEY")

  # Get foreign keys
  fk_info <- con |>
    tbl(DBI::Id(
      schema = "information_schema",
      table = "table_constraints"
    )) |>
    filter(constraint_type == "FOREIGN KEY") |>
    select(table_schema, table_name, constraint_name) |>
    collect()

  # Get foreign key columns
  if (nrow(fk_info) > 0) {
    fk_columns <- con |>
      tbl(DBI::Id(
        schema = "information_schema",
        table = "key_column_usage"
      )) |>
      select(table_schema, table_name, constraint_name, column_name) |>
      collect()

    fk_with_cols <- fk_info |>
      inner_join(fk_columns, by = c("table_schema", "table_name", "constraint_name")) |>
      mutate(key_type = "FOREIGN KEY")
  } else {
    fk_with_cols <- tibble()
  }

  # Combine and format results
  bind_rows(pk_with_cols, fk_with_cols) |>
    select(table_schema, table_name, column_name, constraint_name, key_type) |>
    arrange(table_schema, table_name, constraint_name)
}


# Assess null/missing data in a table
#
# Parameters:
#   con - database connection
#   table_name - name of table to assess
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: tibble with null counts and percentages for each column
assess_nulls <- function(con, table_name, schema_name = NULL) {
  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- table_name
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  tbl_ref <- con |>
    tbl(table_id) |>
    collect()

  # Get total row count
  total_rows <- nrow(tbl_ref)

  # Count nulls in each column
  null_counts <- tbl_ref |>
    summarise(across(everything(), ~ sum(is.na(.)))) |>
    collect() |>
    pivot_longer(everything(), names_to = "column_name", values_to = "null_count")

  # Calculate percentages
  null_counts |>
    mutate(
      null_percentage = round(100 * null_count / total_rows, 2),
      null_status = case_when(
        null_count == 0 ~ "No nulls",
        null_percentage < 5 ~ "Minimal",
        null_percentage < 25 ~ "Moderate",
        TRUE ~ "Significant"
      )
    ) |>
    arrange(desc(null_count))
}


# ==========================================================
# PART 3: Documentation (Optional)
# ==========================================================

# Add a table to the documentation
#
# Parameters:
#   con - database connection
#   table_name - name of table
#   description - brief description of what this table contains
#   data_source - where the data comes from
#   update_frequency - how often it's updated (e.g., "Daily", "Weekly")
#   contact - who to ask about this table
#
# Note: Requires meta.table_definitions table to exist
document_table <- function(con, table_name, description, data_source,
                           update_frequency, contact = NA_character_) {
  # Delete any existing documentation for this
  # table by keeping only other tables
  current_docs <- con |>
    tbl(DBI::Id(schema = "meta", table = "table_definitions")) |>
    filter(table_name != !!table_name) |>
    collect()

  # Clear and re-insert
  DBI::dbExecute(con, "DELETE FROM meta.table_definitions")
  if (nrow(current_docs) > 0) {
    DBI::dbAppendTable(
      con,
      DBI::Id(schema = "meta", table = "table_definitions"),
      current_docs
    )
  }

  # Now add the new documentation
  doc_data <- tibble(
    table_name = table_name,
    description = description,
    data_source = data_source,
    update_frequency = update_frequency,
    contact = contact,
    documented_date = Sys.Date()
  )

  DBI::dbAppendTable(
    con,
    DBI::Id(schema = "meta", table = "table_definitions"),
    doc_data
  )

  cat("Documented:", table_name, "\n")
}


# Add a column to the documentation
#
# Parameters:
#   con - database connection
#   table_name - name of table that contains the column
#   column_name - name of column to document
#   description - what this column means
#   data_type - data type (optional)
#   notes - any additional notes (optional)
#
# Note: Requires meta.column_definitions table to exist
document_column <- function(con, table_name, column_name, description,
                            data_type = NULL, notes = NA_character_) {
  # Delete any existing documentation for this column by keeping only other rows
  current_docs <- con |>
    tbl(DBI::Id(schema = "meta", table = "column_definitions")) |>
    filter(!(table_name == !!table_name & column_name == !!column_name)) |>
    collect()

  # Clear and re-insert
  DBI::dbExecute(con, "DELETE FROM meta.column_definitions")
  if (nrow(current_docs) > 0) {
    DBI::dbAppendTable(
      con,
      DBI::Id(schema = "meta", table = "column_definitions"),
      current_docs
    )
  }

  # Now add the new documentation
  col_data <- tibble(
    table_name = table_name,
    column_name = column_name,
    description = description,
    data_type = data_type,
    notes = notes
  )

  DBI::dbAppendTable(
    con,
    DBI::Id(schema = "meta", table = "column_definitions"),
    col_data
  )

  cat("Documented:", table_name, ".", column_name, "\n")
}


# Get documentation for a table
#
# Parameters:
#   con - database connection
#   table_name - name of table to look up
#
# Returns: formatted kable table with documentation, or message if not found
get_table_documentation <- function(con, table_name) {
  result <- con |>
    tbl(DBI::Id(schema = "meta", table = "table_definitions")) |>
    filter(table_name == !!table_name) |>
    collect()

  if (nrow(result) == 0) {
    cat("No documentation found for:", table_name, "\n")
    return(invisible(NULL))
  }

  # Format for nice table display
  doc <- result[1, ] # In case there are multiple rows

  formatted <- tibble(
    Field = c(
      "Table Name", "Description", "Data Source",
      "Update Frequency", "Contact", "Documented Date"
    ),
    Value = c(
      doc$table_name, doc$description, doc$data_source,
      doc$update_frequency, doc$contact, doc$documented_date
    )
  )

  knitr::kable(formatted)
}

get_documentation <- function(con, tbl_name) {
  con |>
    tbl(DBI::Id(
      schema = "meta",
      table = "table_definitions"
    )) |>
    filter(table_name == tbl_name) |>
    select(
      Description = description,
      Source = data_source,
      Freq = update_frequency,
      Contact = contact,
      DocDate = documented_date
    )
}


# ============================================================
# PART 6: Beyond Basic Exploration
# ============================================================

# Find duplicate rows
#
# Parameters:
#   con - database connection
#   table_name - name of table to check (without schema prefix)
#   columns - column names to check for duplicates (NULL = all columns)
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: tibble of duplicate rows with duplicate count
find_duplicates <- function(con, table_name, columns = NULL, schema_name = NULL) {
  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- table_name
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  tbl_ref <- con |> tbl(table_id)

  # If no columns specified, use all
  if (is.null(columns)) {
    columns <- names(tbl_ref)
  }

  # Find duplicates
  tbl_ref |>
    group_by(across(all_of(columns))) |>
    filter(n() > 1) |>
    mutate(duplicate_count = n()) |>
    arrange(desc(duplicate_count)) |>
    collect()
}


# Get value counts for a column
#
# Parameters:
#   con - database connection
#   table_name - name of table (without schema prefix)
#   column_name - column to analyze
#   schema_name - name of schema (default: NULL, set to specify schema)
#
# Returns: tibble with unique values and their counts
value_counts <- function(con, table_name, column_name, schema_name = NULL) {
  # Construct the table identifier based on schema_name
  if (is.null(schema_name)) {
    table_id <- table_name
  } else {
    table_id <- DBI::Id(schema = schema_name, table = table_name)
  }

  con |>
    tbl(table_id) |>
    group_by(!!sym(column_name)) |>
    count() |>
    arrange(desc(n)) |>
    collect() |>
    rename(value = !!column_name, count = n)
}

create_schema <- function(con) {
  dbExecute(con, "DROP SCHEMA IF EXISTS staging CASCADE")
  dbExecute(con, "DROP SCHEMA IF EXISTS warehouse CASCADE")
  dbExecute(con, "DROP SCHEMA IF EXISTS sandbox CASCADE")
  dbExecute(con, "DROP SCHEMA IF EXISTS archive CASCADE")
  dbExecute(con, "DROP SCHEMA IF EXISTS meta CASCADE")
  dbExecute(con, "CREATE SCHEMA staging")
  dbExecute(con, "CREATE SCHEMA warehouse")
  dbExecute(con, "CREATE SCHEMA sandbox")
  dbExecute(con, "CREATE SCHEMA archive")
  dbExecute(con, "CREATE SCHEMA meta")
}
