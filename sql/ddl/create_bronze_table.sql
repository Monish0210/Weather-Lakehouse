CREATE TABLE IF NOT EXISTS weather_lakehouse.bronze_weather_raw (
    latitude                DOUBLE,
    longitude               DOUBLE,
    generationtime_ms       DOUBLE,
    utc_offset_seconds      INT,
    timezone                STRING,
    timezone_abbreviation   STRING,
    elevation               DOUBLE,

    hourly_units STRUCT
        time:                   STRING,
        temperature_2m:         STRING,
        relative_humidity_2m:   STRING,
        apparent_temperature:   STRING,
        precipitation:          STRING,
        rain:                   STRING,
        wind_speed_10m:         STRING,
        wind_direction_10m:     STRING,
        weather_code:           STRING,
        uv_index:               STRING
    >,

    hourly STRUCT
        time:                   ARRAY<STRING>,
        temperature_2m:         ARRAY<DOUBLE>,
        relative_humidity_2m:   ARRAY<INT>,
        apparent_temperature:   ARRAY<DOUBLE>,
        precipitation:          ARRAY<DOUBLE>,
        rain:                   ARRAY<DOUBLE>,
        wind_speed_10m:         ARRAY<DOUBLE>,
        wind_direction_10m:     ARRAY<INT>,
        weather_code:           ARRAY<INT>,
        uv_index:               ARRAY<DOUBLE>
    >,

    ingestion_timestamp     STRING,
    pipeline_run_date       STRING,
    city_name               STRING,
    city_display_name       STRING,
    country                 STRING
)
PARTITIONED BY (
    city        STRING,
    year        STRING,
    month       STRING,
    day         STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://weather-lakehouse-monish/bronze/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'skip.header.line.count' = '0'
)