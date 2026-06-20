-- /*
-- =============================================================================
--   DIMENSION MODEL: dim_employee_scd2

--   Purpose:
--     This model keeps full history of employee changes.
--     It creates new versions when data changes, closes old versions,
--     and marks employees inactive if they are no longer in source.
-- =============================================================================
-- */

{{ config(
    schema = 'marts',
    materialized = 'incremental',
    unique_key = ['employee_id', 'valid_from_date'],
    incremental_strategy = 'merge',
    merge_update_columns = ['valid_to_date','is_active_flag','record_load_timestamp','run_identifier']
) }}

-- Step 1: Take clean data from staging.
-- Remove rows where important columns are NULL.
with base as (
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
from {{ ref('employee_stg') }}
where employee_id is not null
and employee_name is not null
and department is not null
and designation is not null
and salary is not null
and location is not null
and manager_id is not null
and employment_status is not null
),
--select * from base

-- Step 2: Create one row per version.
-- Get earliest date as start and latest observed date.
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
    min(edh_partition_dt) as edh_partition_dt,            --version start
    max(max_edh_partition_dt) as max_edh_partition_dt     --last observed
from base
group by employee_id,
employee_name,
department,
designation,
salary,
location,
manager_id,
employment_status,
email,
source_hash_key
),
--select * from version_start

-- Step 3: Get current active records from target table.
target_ as (
select * from {{ this }} 
where is_active_flag = true
),

-- Step 4: Get latest partition date from source.
-- This helps decide which records are current.
max_date as (
select 
    max(max_edh_partition_dt) as latest_partition
from version_start
),

-- Step 5: Prepare source data and rename hash column.
src as (
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
    source_hash_key as record_hash_key,
    edh_partition_dt,
    max_edh_partition_dt
from version_start
),

-- Step 6: Identify new records not present in target.
-- These will become new active rows.
unioned as (
select
    MD5(CONCAT_WS('||',
        coalesce(cast(s.employee_id as string), '__dbt__null__'),
        coalesce(cast(s.edh_partition_dt as string),'__dbt__null__')
    )) as employee_sk,
    s.edh_partition_dt as valid_from_date,
    to_date('9999-12-31') as valid_to_date,
    true as is_active_flag,
    s.record_hash_key,
    current_timestamp() as record_load_timestamp,
    'employee_source' as record_source_name,
    'model.dbt_scd.dim_employee_scd2'as etl_pipeline_name,
    '{{ var("run_id", invocation_id)}}' as run_identifier,
    s.employee_id,
    s.employee_name,
    s.department,
    s.designation,
    s.salary,
    s.location,
    s.manager_id,
    s.employment_status,
    s.email,
    s.max_edh_partition_dt
from src s
where not exists (select 1 from {{ this }} e where e.record_hash_key = s.record_hash_key 
and e.is_active_flag = true)
),

--select * from unioned
-- Step 7: Close old records.
-- New historical records are immediately closed.
-- Also close existing active rows when a change is detected.
closed as (
select
    employee_sk,
    valid_from_date,
    coalesce(max_edh_partition_dt,(select latest_partition from max_date)-1) as valid_to_date,
    false as is_active_flag,
    record_hash_key,
    current_timestamp() as record_load_timestamp,
    record_source_name,
    etl_pipeline_name,
    '{{ var("run_id",invocation_id) }}' as run_identifier,
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    max_edh_partition_dt
from unioned
--qualify row_number() over (partition by employee_id order by valid_from_date desc) >= 1 
where max_edh_partition_dt <> (select latest_partition from max_date) 
union all
--existing target rows superseded by new versions
select
    t.employee_sk,
    t.valid_from_date,
    coalesce(LAG(u.valid_from_date) over (partition by u.employee_id 
    order by u.valid_from_date desc)-1,(select latest_partition from max_date)-1) as valid_to_date,
    false as is_active_flag,
    t.record_hash_key,
    current_timestamp() as record_load_timestamp,
    t.record_source_name,
    t.etl_pipeline_name,
    '{{ var("run_id",invocation_id) }}' as run_identifier,
    t.employee_id,
    t.employee_name,
    t.department,
    t.designation,
    t.salary,
    t.location,
    t.manager_id,
    t.employment_status,
    t.email,
    u.max_edh_partition_dt
from unioned u
join target_ t 
on u.employee_id = t.employee_id
where u.record_hash_key <> t.record_hash_key
),

-- Step 8: Keep only the latest active record per employee.
open_rows as (
select
    employee_sk,
    valid_from_date,
    valid_to_date,
    true as is_active_flag,
    record_hash_key,
    record_load_timestamp,
    record_source_name,
    etl_pipeline_name,
    run_identifier,
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email,
    max_edh_partition_dt
from unioned
qualify row_number() over (partition by employee_id order by valid_from_date desc) = 1
and max_edh_partition_dt = (select latest_partition from max_date)
)
--select * from open_rows
-- Step 9: Combine all results:
-- closed records + active records + deleted (tombstone) records
select
    employee_sk,
    valid_from_date,
    valid_to_date,
    is_active_flag,
    record_hash_key,
    record_load_timestamp,
    record_source_name,
    etl_pipeline_name,
    run_identifier,
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email
from closed
union all
select
    employee_sk,
    valid_from_date,
    valid_to_date,
    is_active_flag,
    record_hash_key,
    record_load_timestamp,
    record_source_name,
    etl_pipeline_name,
    run_identifier,
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email
from open_rows
union all
-- Tombstone records:
-- Employees that are no longer present in the source.
-- They are marked inactive and closed as of previous day.
select
    employee_sk,
    valid_from_date,
    DATE '{{ var("RUN_DATE") }}'-1 AS valid_to_date,
    false as is_active_flag,
    record_hash_key,
    current_timestamp() as record_load_timestamp,
    record_source_name,
    etl_pipeline_name,
    '{{ var("run_id", invocation_id) }}' as run_identifier,
    employee_id,
    employee_name,
    department,
    designation,
    salary,
    location,
    manager_id,
    employment_status,
    email
from target_
/*where not exists (
    select 1 from {{ ref('employee_stg') }} s
    where s.employee_id = target_.employee_id
)*/
where employee_id not in (select distinct employee_id from {{ ref('employee_stg') }} )