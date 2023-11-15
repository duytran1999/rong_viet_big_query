import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def run_query(query):
    query_job = client.query(query)
    return query_job.result().to_dataframe()

query_string ="""
WITH
  cte_data_raw AS (
  SELECT
    *
  FROM
    `idragon23.analytics_345872396.events_*`
  WHERE
    (_table_suffix BETWEEN '20230522'
      AND FORMAT_DATE('%Y%m%d',CURRENT_DATE()) ) )
SELECT
  DISTINCT RAW.user_pseudo_id,
  # Ma nguoi dung theo GA,
  RAW.user_id,
  # Ma khach hang ( C...,  fbid,...) # loai nguoi dung ( KH HH,  Trial,  expire trial) RAW.device.mobile_model_name,
  RAW.device.category,
  RAW.device.operating_system,
  RAW.geo.city,
  RAW.geo.country,
  # DOB # Gender RAW.device.language,
  RAW.app_info.version,
  DATETIME_ADD(PARSE_DATETIME('%s', CAST(TRUNC( start_trial.event_timestamp /1000000) AS STRING)),INTERVAL 7 HOUR) AS start_trial,
  DATETIME_ADD(PARSE_DATETIME('%s', CAST(TRUNC( download_app.event_timestamp/1000000) AS STRING)),INTERVAL 7 HOUR) AS download_app,
  DATETIME_ADD(PARSE_DATETIME('%s', CAST(TRUNC( first_login.event_timestamp /1000000) AS STRING)),INTERVAL 7 HOUR) AS first_login
FROM
  cte_data_raw AS RAW
LEFT JOIN (
  SELECT
    start_trial.user_id,
    start_trial.user_pseudo_id,
    start_trial.event_timestamp
  FROM (
    SELECT
      event_timestamp,
      user_id,
      user_pseudo_id,
      ROW_NUMBER () OVER(PARTITION BY user_id ORDER BY event_timestamp ASC) AS row_num
    FROM
      `idragon23.analytics_345872396.events_*`
    WHERE
      (
      SELECT
        value.string_value
      FROM
        UNNEST(user_properties)
      WHERE
        KEY = 'trial_register') IS NOT NULL ) AS start_trial
  WHERE
    start_trial.row_num =1
    AND STARTS_WITH(start_trial.user_id, "C") = FALSE
    AND start_trial.user_id IS NOT NULL ) AS start_trial
ON
  RAW.user_pseudo_id = start_trial.user_pseudo_id
  AND RAW.user_id = start_trial.user_id
LEFT JOIN (
  SELECT
    XXX.event_timestamp,
    XXX.user_id,
    XXX.user_pseudo_id
  FROM (
    SELECT
      DISTINCT X.event_timestamp,
      Y.user_id,
      Y.user_pseudo_id,
      ROW_NUMBER () OVER(PARTITION BY Y.user_id ORDER BY X.event_timestamp ASC) AS row_num
    FROM (
      SELECT
        DISTINCT event_timestamp,
        user_pseudo_id,
        device.vendor_id
      FROM
        `idragon23.analytics_345872396.events_*`
      WHERE
        event_name = 'first_open' ) AS X,
      (
      SELECT
        DISTINCT user_id,
        user_pseudo_id,
        device.vendor_id
      FROM
        `idragon23.analytics_345872396.events_*`
      WHERE
        user_id IS NOT NULL )AS Y
    WHERE
      X.user_pseudo_id = Y.user_pseudo_id ) AS XXX
  WHERE
    XXX.row_num= 1 ) AS download_app
ON
  RAW.user_pseudo_id = download_app.user_pseudo_id
  AND RAW.user_id = download_app.user_id
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      event_date,
      event_timestamp,
      (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_view') AS page_view,
      event_previous_timestamp,
      user_id,
      (
      SELECT
        value.string_value
      FROM
        UNNEST(user_properties)
      WHERE
        KEY = 'account_name') AS account_name,
      user_pseudo_id,
      ROW_NUMBER () OVER(PARTITION BY user_id ORDER BY event_timestamp ASC) row_num
    FROM
      `idragon23.analytics_345872396.events_*`
    WHERE
      user_id IS NOT NULL ) AS X
  WHERE
    X.row_num = 1 ) AS first_login
ON
  RAW.user_pseudo_id = first_login.user_pseudo_id
  AND RAW.user_id = first_login.user_id
WHERE
  RAW.user_id IS NOT NULL
ORDER BY
  user_id,
  user_pseudo_id
"""

rows = run_query(query_string)

# Print results.
st.write("Rong Viet User iDragon")

st.dataframe(rows)