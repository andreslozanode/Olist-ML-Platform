-- Tabla RAW + file format + stage para cargar el CSV de Olist.
CREATE FILE FORMAT IF NOT EXISTS ANALYTICS.RAW.CSV_OLIST
    TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    NULL_IF = ('', 'NULL') EMPTY_FIELD_AS_NULL = TRUE;

CREATE STAGE IF NOT EXISTS ANALYTICS.RAW.OLIST_STAGE
    FILE_FORMAT = ANALYTICS.RAW.CSV_OLIST;

CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.OLIST_CUSTOMERS (
    customer_id               VARCHAR,
    customer_unique_id        VARCHAR,
    customer_zip_code_prefix  VARCHAR,   -- string: preserva ceros del CEP
    customer_city             VARCHAR,
    customer_state            VARCHAR(2),
    _loaded_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Carga (ejecutar tras subir el archivo al stage):
--   PUT file://olist_customers_dataset.csv @ANALYTICS.RAW.OLIST_STAGE;
--   COPY INTO ANALYTICS.RAW.OLIST_CUSTOMERS (customer_id, customer_unique_id,
--       customer_zip_code_prefix, customer_city, customer_state)
--   FROM @ANALYTICS.RAW.OLIST_STAGE ON_ERROR = ABORT_STATEMENT;
