/* contrib/orc_fdw/orc_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION orc_fdw" to load this file. \quit

CREATE FUNCTION orc_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION orc_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER orc_fdw
  HANDLER orc_fdw_handler
  VALIDATOR orc_fdw_validator;
