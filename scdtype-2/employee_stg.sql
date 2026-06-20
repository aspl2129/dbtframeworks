{{ config(
    materialized = 'incremental',
    pre_hook = "truncate table {{ this }}",
    on_schema_change = 'ignore'
)}}

/*
=============================================================================
  STAGING MODEL: stg_employee (SCD Type 2 - Change Detection)

  Purpose:
    This model reads employee data from the source, detects any changes
    in employee attributes over time, and produces one row per "version"
    of each employee. Each version represents a period during which the
    employee's details stayed the same.

  How it works (step by step):
    1. Figure out the date range to load (full history or just today).
    2. Pull employee records from the source within that date range.
    3. Create a hash (fingerprint) of each employee's attributes.
    4. Compare each day's hash with the previous day's hash to spot changes.
    5. Group consecutive unchanged days into a single "version".
    6. Output one row per version with its start and end date.
=============================================================================
*/

-- Step 1: Decide the date range to load.
-- If the target table is empty (first run), go back to SEED_DATE for full history.
-- Otherwise, only load data for today's RUN_DATE (daily incremental).
with last_loaded as (
select
    case when tgt_count = 0 then date '{{ var("SEED_DATE") }}'
    else date '{{ var("RUN_DATE") }}' end as from_dt,
    date '{{ var("RUN_DATE") }}' as to_dt
from (select count(*) as tgt_count from {{ source('marts','dim_employee_scd2') }})
),
-- -- Step 2: Pull employee records from the source within the date range.
-- -- Exclude placeholder IDs ('<None>', '<Unknown>') and the end-of-time
-- -- sentinel date (9999-12-31) as these are not real records.
source_filtered as (
select
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    edh_partition_dt
from {{ source('sources','employee_source')}}
where edh_partition_dt between (select from_dt from last_loaded) and (select to_dt from last_loaded)
and edh_partition_dt<>DATE('9999-12-31')
and employee_id not in ('<None>','<Unknown>')
),

-- Step 3: Create an MD5 hash of all tracked employee attributes.
-- This hash acts like a fingerprint — if any attribute changes, the hash
-- will be different. NULLs are replaced with a placeholder string so
-- they don't break the comparison.
hashed as (
select
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    edh_partition_dt,
    MD5(concat_ws(char(1),
        coalesce(trim(cast(employee_name as string)), '__dbt__null__'),
        coalesce(trim(cast(department as string)), '__dbt__null__'),
        coalesce(trim(cast(designation as string)), '__dbt__null__'),
        coalesce(trim(cast(salary as string)), '__dbt__null__'),
        coalesce(trim(cast(location as string)), '__dbt__null__'),
        coalesce(trim(cast(manager_id as string)), '__dbt__null__'),
        coalesce(trim(cast(employment_status as string)), '__dbt__null__'),
        coalesce(trim(cast(email as string)), '__dbt__null__')
    )) as source_hash_key
from source_filtered
),

-- Step 4: Detect changes by comparing each row's hash with the previous
-- row's hash for the same employee (ordered by date).
-- Same hash as previous day → no change (0). Different → new version (1).
versioned as (
select
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    edh_partition_dt,
    source_hash_key,
    case when lag(source_hash_key) over (partition by employee_id order by edh_partition_dt) = source_hash_key
    then 0 else 1 end as is_new_version 
from hashed
),

-- Step 5: Assign a version number to each row.
-- A running total of "is_new_version" gives a version ID that increments
-- every time a change is detected. Consecutive unchanged rows share
-- the same version ID.
grouped as (
select
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    edh_partition_dt,
    source_hash_key,
    is_new_version,
    sum(is_new_version) over (partition by employee_id order by edh_partition_dt rows unbounded preceding) as version_id
from versioned
),

-- Step 6: Collapse each version into a single row.
-- For each version, get the earliest date (start) and latest date (end).
-- This gives one clean row per period where details remained unchanged.
version_start as (
select
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    source_hash_key,
    version_id,
    min(edh_partition_dt) as edh_partition_dt,
    max(edh_partition_dt) as max_edh_partition_dt
from grouped
group by 
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    source_hash_key,
    version_id
)

-- Final output: One row per employee version with start and end dates.
-- This feeds into the downstream SCD2 dimension table (dim_employee_scd2).
select
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    source_hash_key,
    edh_partition_dt,
    max_edh_partition_dt
from version_start
--order by employee_id
--where employee_id='E002'
--select * from grouped where employee_id='E002'