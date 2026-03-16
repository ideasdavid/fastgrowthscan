-- ============================================================
-- Fast Growth Index — Supabase Schema Setup
-- Paste this entire file into the Supabase SQL Editor and run it.
-- ============================================================


-- ── 1. ENUMS ──────────────────────────────────────────────────────────────

create type result_status as enum (
  'QUALIFIES',
  'DOES_NOT_QUALIFY',
  'MANUAL_REVIEW',
  'ERROR'
);

create type review_status as enum (
  'PENDING',      -- not yet looked at
  'IN_PROGRESS',  -- someone is working on it
  'CONTACTED',    -- team has reached out to the company
  'CONFIRMED',    -- company confirmed for index
  'DECLINED',     -- company declined to participate
  'DISQUALIFIED'  -- manually removed after review
);


-- ── 2. COMPANIES ──────────────────────────────────────────────────────────
-- One row per company number, shared across all index years.

create table companies (
  company_number      varchar(8)    primary key,
  company_name        varchar(500)  not null,
  company_status      varchar(50),
  company_type        varchar(50),
  incorporated_date   varchar(20),
  sic_codes           text,                         -- JSON array e.g. '["62012","63110"]'
  registered_office_address text,                   -- JSON object
  region              varchar(100),
  last_fetched_at     timestamptz   default now()
);

comment on table companies is 'Master company record from Companies House. One row per company, shared across index years.';


-- ── 3. INDEX RESULTS ──────────────────────────────────────────────────────
-- One row per company per index year. Pipeline-generated fields are read-only.
-- Editorial fields are written by your team via the dashboard.

create table index_results (
  id                      bigint        generated always as identity primary key,
  index_year              integer       not null,
  company_number          varchar(8)    not null references companies(company_number),

  -- ── Pipeline-generated (read-only from dashboard) ──────────────────────
  baseline_period_start   varchar(20),
  baseline_period_end     varchar(20),
  baseline_turnover       numeric(15,2),

  growth_period_start     varchar(20),
  growth_period_end       varchar(20),
  growth_turnover         numeric(15,2),

  growth_percent          numeric(8,2),

  baseline_filing_id      varchar(100),
  growth_filing_id        varchar(100),
  baseline_document_url   text,
  growth_document_url     text,

  status                  result_status not null,
  manual_review_reason    text,
  rank                    integer,
  processed_at            timestamptz   default now(),

  -- ── Editorial fields (written by team via dashboard) ───────────────────
  contact_email           varchar(255),             -- primary contact email
  contact_name            varchar(255),             -- contact person name
  contact_phone           varchar(50),              -- phone number
  notes                   text,                     -- free-text internal notes
  review_status           review_status default 'PENDING',
  reviewed_by             varchar(255),             -- email of team member who reviewed
  reviewed_at             timestamptz,
  confirmed_for_index     boolean       default false,  -- manually confirmed inclusion
  featured                boolean       default false,  -- flag for featured/spotlight treatment
  linkedin_url            text,                     -- company LinkedIn
  website_url             text,                     -- company website

  constraint uq_year_company unique (index_year, company_number)
);

comment on table index_results is 'One row per company per index year. Pipeline writes financial data; team writes editorial data.';
comment on column index_results.contact_email       is 'Editorial: primary contact email for outreach';
comment on column index_results.review_status       is 'Editorial: workflow status for this entry';
comment on column index_results.confirmed_for_index is 'Editorial: manually confirmed this company is included in the published index';
comment on column index_results.featured            is 'Editorial: flag for spotlight/feature treatment in published output';


-- ── 4. PIPELINE RUNS ──────────────────────────────────────────────────────

create table pipeline_runs (
  id                      bigint        generated always as identity primary key,
  index_year              integer       not null,
  started_at              timestamptz   default now(),
  completed_at            timestamptz,
  candidates_found        integer,
  qualifies_count         integer,
  manual_review_count     integer,
  does_not_qualify_count  integer,
  error_count             integer,
  notes                   text
);

comment on table pipeline_runs is 'Audit log of every pipeline execution.';


-- ── 5. INDEXES ────────────────────────────────────────────────────────────

create index ix_results_year_status   on index_results (index_year, status);
create index ix_results_year_rank     on index_results (index_year, rank);
create index ix_results_review_status on index_results (review_status);
create index ix_companies_region      on companies (region);


-- ── 6. ROW LEVEL SECURITY ─────────────────────────────────────────────────
-- Only authenticated users can access any data.
-- All authenticated users have full read/write for now.
-- You can tighten this later (e.g. read-only roles, admin-only pipeline tables).

alter table companies       enable row level security;
alter table index_results   enable row level security;
alter table pipeline_runs   enable row level security;

-- Authenticated users can read all companies
create policy "Authenticated users can read companies"
  on companies for select
  to authenticated
  using (true);

-- Authenticated users can read all results
create policy "Authenticated users can read results"
  on index_results for select
  to authenticated
  using (true);

-- Authenticated users can update ONLY the editorial fields on results
-- (Pipeline fields are protected — update is restricted to named columns via app logic)
create policy "Authenticated users can update editorial fields"
  on index_results for update
  to authenticated
  using (true)
  with check (true);

-- Authenticated users can read pipeline runs
create policy "Authenticated users can read pipeline runs"
  on pipeline_runs for select
  to authenticated
  using (true);

-- Service role (used by the Python pipeline) has full access — handled automatically
-- by Supabase when you use the service_role key in your .env


-- ── 7. HELPFUL VIEWS ──────────────────────────────────────────────────────
-- Pre-joined views that Lovable can use directly.

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

comment on view v_qualifying_companies is 'Qualifying companies with all editorial fields. Use this as the primary data source in Lovable.';


create view v_manual_review as
  select
    r.id,
    r.index_year,
    c.company_number,
    c.company_name,
    c.region,
    c.sic_codes,
    r.manual_review_reason,
    r.baseline_document_url,
    r.growth_document_url,
    r.contact_email,
    r.notes,
    r.review_status,
    r.reviewed_by,
    r.reviewed_at,
    r.processed_at
  from index_results r
  join companies c on c.company_number = r.company_number
  where r.status = 'MANUAL_REVIEW'
  order by r.index_year desc, c.company_name asc;

comment on view v_manual_review is 'Companies flagged for manual review. Use this in the Manual Review tab in Lovable.';


create view v_index_summary as
  select
    index_year,
    count(*) filter (where status = 'QUALIFIES')         as qualifies_count,
    count(*) filter (where status = 'MANUAL_REVIEW')     as manual_review_count,
    count(*) filter (where status = 'DOES_NOT_QUALIFY')  as dnq_count,
    count(*) filter (where status = 'ERROR')             as error_count,
    max(growth_percent) filter (where status = 'QUALIFIES') as highest_growth_percent,
    count(*) filter (where status = 'QUALIFIES' and confirmed_for_index = true) as confirmed_count,
    count(*) filter (where review_status = 'CONTACTED')  as contacted_count
  from index_results
  group by index_year
  order by index_year desc;

comment on view v_index_summary is 'Year-by-year summary counts. Use for the stats row at the top of the dashboard.';


-- ── 8. DONE ───────────────────────────────────────────────────────────────
-- Next steps:
-- 1. Copy your Supabase DATABASE_URL (Settings > Database > Connection string > URI)
--    into your .env file as DATABASE_URL=
-- 2. Also copy the service_role key (Settings > API) into .env as SUPABASE_SERVICE_KEY=
-- 3. Run: python run_pipeline.py --year 2026
-- 4. Connect Lovable to this Supabase project and use the views above as your data sources
