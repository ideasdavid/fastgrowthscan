-- ============================================================
-- Migration: Add sector tracking and enhanced results view
-- Run in Supabase SQL Editor
-- ============================================================

-- 1. Add sector columns to pipeline_runs
alter table pipeline_runs
  add column if not exists sector       varchar(100),
  add column if not exists sector_label varchar(200);

comment on column pipeline_runs.sector       is 'Sector group key e.g. technology';
comment on column pipeline_runs.sector_label is 'Human readable sector label e.g. Technology & Software';

-- 2. Recreate v_qualifying_companies with latest_turnover column
drop view if exists v_qualifying_companies;

create view v_qualifying_companies as
  select
    r.id,
    r.index_year,
    r.rank,
    c.company_number,
    c.company_name,
    c.region,
    c.sic_codes,
    c.incorporated_date,
    r.baseline_period_start,
    r.baseline_period_end,
    r.baseline_turnover,
    r.growth_period_start,
    r.growth_period_end,
    r.growth_turnover,
    r.growth_percent,
    -- Latest turnover = growth year turnover (most recent filed)
    r.growth_turnover                                    as latest_turnover,
    r.growth_period_end                                  as latest_accounts_date,
    r.baseline_document_url,
    r.growth_document_url,
    r.contact_email,
    r.contact_name,
    r.contact_phone,
    r.notes,
    r.review_status,
    r.reviewed_by,
    r.reviewed_at,
    r.confirmed_for_index,
    r.featured,
    r.linkedin_url,
    r.website_url,
    r.processed_at
  from index_results r
  join companies c on c.company_number = r.company_number
  where r.status = 'QUALIFIES'
  order by r.index_year desc, r.rank asc;

-- 3. Recreate v_all_results — full results with turnover for all statuses
drop view if exists v_all_results;

create view v_all_results as
  select
    r.id,
    r.index_year,
    r.rank,
    r.status,
    c.company_number,
    c.company_name,
    c.region,
    c.sic_codes,
    c.incorporated_date,
    r.baseline_period_start,
    r.baseline_period_end,
    r.baseline_turnover,
    r.growth_period_start,
    r.growth_period_end,
    r.growth_turnover,
    r.growth_percent,
    r.growth_turnover                                    as latest_turnover,
    r.growth_period_end                                  as latest_accounts_date,
    r.manual_review_reason,
    r.baseline_document_url,
    r.growth_document_url,
    r.contact_email,
    r.contact_name,
    r.notes,
    r.review_status,
    r.confirmed_for_index,
    r.featured,
    r.linkedin_url,
    r.website_url,
    r.processed_at
  from index_results r
  join companies c on c.company_number = r.company_number
  order by r.index_year desc, r.status, r.rank asc nulls last, r.growth_percent desc nulls last;

comment on view v_all_results is 'All results across all statuses with latest turnover. Primary data source for dashboard tables.';

-- 4. Update v_index_summary to include sector breakdown
drop view if exists v_index_summary;

create view v_index_summary as
  select
    r.index_year,
    count(*) filter (where r.status = 'QUALIFIES')         as qualifies_count,
    count(*) filter (where r.status = 'MANUAL_REVIEW')     as manual_review_count,
    count(*) filter (where r.status = 'DOES_NOT_QUALIFY')  as dnq_count,
    count(*) filter (where r.status = 'ERROR')             as error_count,
    max(r.growth_percent) filter (where r.status = 'QUALIFIES') as highest_growth_percent,
    count(*) filter (where r.status = 'QUALIFIES' and r.confirmed_for_index = true) as confirmed_count,
    count(*) filter (where r.review_status = 'CONTACTED')  as contacted_count,
    (select p.baseline_year from pipeline_runs p
     where p.index_year = r.index_year
     order by p.started_at desc limit 1) as baseline_year,
    (select p.growth_year from pipeline_runs p
     where p.index_year = r.index_year
     order by p.started_at desc limit 1) as growth_year
  from index_results r
  group by r.index_year
  order by r.index_year desc;

-- 5. Sector progress view — which sectors have been run per index year
create or replace view v_sector_progress as
  select
    p.index_year,
    p.sector,
    p.sector_label,
    p.started_at,
    p.completed_at,
    p.candidates_found,
    p.qualifies_count,
    p.manual_review_count,
    case when p.completed_at is not null then true else false end as is_complete
  from pipeline_runs p
  where p.sector is not null
  order by p.index_year desc, p.started_at desc;

comment on view v_sector_progress is 'Shows which sectors have been processed per index year.';
