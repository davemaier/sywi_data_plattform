#' Load environment variables from .env files
#'
#' Reads .env and .env.local files, with .env.local taking precedence.
#' Searches upward from the current directory to find the files.
#'
#' @return Invisibly returns a named list of loaded variables
#' @noRd
load_dotenv <- function() {
  find_env_file <- function(filename) {
    dir <- getwd()
    while (dir != dirname(dir)) {
      path <- file.path(dir, filename)
      if (file.exists(path)) {
        return(path)
      }
      dir <- dirname(dir)
    }
    return(NULL)
  }

  parse_env_file <- function(path) {
    if (is.null(path) || !file.exists(path)) {
      return(list())
    }

    lines <- readLines(path, warn = FALSE)
    env_vars <- list()

    for (line in lines) {
      line <- trimws(line)
      if (nchar(line) == 0 || startsWith(line, "#")) {
        next
      }

      eq_pos <- regexpr("=", line, fixed = TRUE)
      if (eq_pos > 0) {
        key <- trimws(substr(line, 1, eq_pos - 1))
        value <- substr(line, eq_pos + 1, nchar(line))
        value <- gsub("^[\"']|[\"']$", "", value)
        env_vars[[key]] <- value
      }
    }

    return(env_vars)
  }

  env_path <- find_env_file(".env")
  env_local_path <- find_env_file(".env.local")

  env_vars <- parse_env_file(env_path)
  env_local_vars <- parse_env_file(env_local_path)

  all_vars <- modifyList(env_vars, env_local_vars)

  for (key in names(all_vars)) {
    do.call(Sys.setenv, setNames(list(all_vars[[key]]), key))
  }

  invisible(all_vars)
}


#' Get required environment variable
#' @noRd
get_required_env <- function(name) {
  value <- Sys.getenv(name, unset = "")
  if (value == "") {
    stop(sprintf("Required environment variable %s is not set", name),
      call. = FALSE
    )
  }
  return(value)
}


#' Get environment variable with default
#' @noRd
get_env <- function(name, default = "") {
  value <- Sys.getenv(name, unset = "")
  if (value == "") {
    return(default)
  }
  return(value)
}


#' Setup DuckLake connection
#'
#' Configures an existing DuckDB connection with SYWI DuckLake databases.
#' @param conn A DuckDB connection
#' @return The configured connection
#' @noRd
setup_ducklake <- function(conn) {
  load_dotenv()

  local_catalog_dsn <- get_required_env("DUCKLAKE_CATALOG_DSN")
  local_data_path <- get_required_env("DUCKLAKE_DATA_PATH")
  remote_catalog_dsn <- get_required_env("DUCKLAKE_REMOTE_CATALOG_DSN")
  remote_data_path <- get_required_env("DUCKLAKE_REMOTE_DATA_PATH")

  s3_region <- get_env("DUCKLAKE_REMOTE_S3_REGION", "us-east-1")
  s3_endpoint <- get_required_env("DUCKLAKE_REMOTE_S3_ENDPOINT")
  s3_access_key_id <- get_required_env("DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID")
  s3_secret_access_key <- get_required_env("DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY")
  s3_url_style <- get_env("DUCKLAKE_REMOTE_S3_URL_STYLE", "path")
  s3_use_ssl <- tolower(get_env("DUCKLAKE_REMOTE_S3_USE_SSL", "false")) == "true"

  tryCatch(
    {
      DBI::dbExecute(conn, "INSTALL ducklake; INSTALL postgres; INSTALL httpfs;")
      DBI::dbExecute(conn, "LOAD ducklake; LOAD postgres; LOAD httpfs;")

      DBI::dbExecute(conn, sprintf("SET s3_region = '%s';", s3_region))
      DBI::dbExecute(conn, sprintf("SET s3_endpoint = '%s';", s3_endpoint))
      DBI::dbExecute(conn, sprintf("SET s3_url_style = '%s';", s3_url_style))
      DBI::dbExecute(conn, sprintf("SET s3_access_key_id = '%s';", s3_access_key_id))
      DBI::dbExecute(conn, sprintf("SET s3_secret_access_key = '%s';", s3_secret_access_key))
      DBI::dbExecute(conn, sprintf("SET s3_use_ssl = %s;", tolower(as.character(s3_use_ssl))))

      DBI::dbExecute(
        conn,
        sprintf(
          "ATTACH 'ducklake:%s' AS local (DATA_PATH '%s');",
          local_catalog_dsn, local_data_path
        )
      )

      DBI::dbExecute(
        conn,
        sprintf(
          "ATTACH 'ducklake:postgres:%s' AS remote (DATA_PATH '%s', READ_ONLY);",
          remote_catalog_dsn, remote_data_path
        )
      )
    },
    error = function(e) {
      DBI::dbDisconnect(conn)
      stop(sprintf("Failed to connect to DuckLake: %s", e$message), call. = FALSE)
    }
  )

  return(conn)
}


#' SYWI DuckDB Driver
#'
#' Creates a DuckDB driver that automatically configures SYWI DuckLake connections.
#' Use this as a drop-in replacement for `duckdb::duckdb()`.
#'
#' @return A duckdb driver object that can be used with `DBI::dbConnect()`
#'
#' @details
#' When you connect using this driver, the connection will automatically:
#' - Load environment variables from .env and .env.local files
#' - Install and load required DuckDB extensions (ducklake, postgres, httpfs)
#' - Configure S3 credentials for remote access
#' - Attach `local` database (read-write) pointing to your local DuckLake

#' - Attach `remote` database (read-only) pointing to the production DuckLake
#'
#' Required environment variables:
#' - `DUCKLAKE_CATALOG_DSN`: Local DuckLake catalog path
#' - `DUCKLAKE_DATA_PATH`: Local data path
#' - `DUCKLAKE_REMOTE_CATALOG_DSN`: Remote Postgres connection string
#' - `DUCKLAKE_REMOTE_DATA_PATH`: Remote S3 path
#' - `DUCKLAKE_REMOTE_S3_ENDPOINT`: S3 endpoint
#' - `DUCKLAKE_REMOTE_S3_ACCESS_KEY_ID`: S3 access key
#' - `DUCKLAKE_REMOTE_S3_SECRET_ACCESS_KEY`: S3 secret key
#'
#' Optional environment variables:
#' - `DUCKLAKE_REMOTE_S3_REGION`: S3 region (default: "us-east-1")
#' - `DUCKLAKE_REMOTE_S3_URL_STYLE`: S3 URL style (default: "path")
#' - `DUCKLAKE_REMOTE_S3_USE_SSL`: Use SSL for S3 (default: "false")
#'
#' @examples
#' \dontrun{
#' library(sywi.duckdb)
#'
#' # Connect using standard DBI pattern
#' con <- DBI::dbConnect(sywi_duckdb())
#'
#' # Query local database
#' DBI::dbGetQuery(con, "SELECT * FROM local.my_table")
#'
#' # Query remote database (read-only)
#' DBI::dbGetQuery(con, "SELECT * FROM remote.my_table")
#'
#' # Disconnect when done
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
#' @import duckdb
#' @import DBI
sywi_duckdb <- function() {
  drv <- duckdb::duckdb()
  class(drv) <- c("sywi_duckdb_driver", class(drv))
  return(drv)
}


#' Connect to SYWI DuckLake
#'
#' @param drv A sywi_duckdb driver created by `sywi_duckdb()`
#' @param ... Additional arguments (ignored, for compatibility)
#' @return A DBI connection with local and remote DuckLake attached
#'
#' @rdname sywi_duckdb
#' @export
#' @method dbConnect sywi_duckdb_driver
dbConnect.sywi_duckdb_driver <- function(drv, ...) {
  # Remove our custom class to get the base duckdb driver
  class(drv) <- class(drv)[class(drv) != "sywi_duckdb_driver"]

  # Create connection using base duckdb

  conn <- DBI::dbConnect(drv, ...)

  # Setup DuckLake
  setup_ducklake(conn)

  return(conn)
}
