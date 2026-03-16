# Fast Growth Index — Lovable Prompt

Paste the text below into Lovable when creating a new project.
Connect your Supabase project first via the Lovable Supabase integration,
then paste this prompt.

---

Build a secure internal dashboard called "Fast Growth Index" for a team that
identifies the UK's fastest growing privately held companies.

## Authentication
Use Supabase Auth. The app should be login-gated — no content visible without
signing in. Use email/password auth. Only invited users should access the app
(disable public signups — team members are invited via Supabase dashboard).

## Data source
All data comes from Supabase. Use these views and tables:

- `v_qualifying_companies` — qualifying companies with editorial fields
- `v_manual_review` — companies flagged for manual review
- `v_index_summary` — year-by-year summary stats
- `index_results` — write editorial field updates back to this table
- `pipeline_runs` — read-only, shows when the pipeline last ran

The fields `rank`, `baseline_turnover`, `growth_turnover`, `growth_percent`,
`status`, `processed_at` are pipeline-generated and must be READ-ONLY.
Never allow editing these fields from the UI.

The following fields are editable by the team:
`contact_email`, `contact_name`, `contact_phone`, `notes`,
`review_status`, `confirmed_for_index`, `featured`,
`linkedin_url`, `website_url`

## Layout
Sidebar navigation with:
- Year selector (from distinct index_year values in index_results)
- Three tabs showing counts from v_index_summary:
  ✓ Qualifies
  ⚑ Manual Review
  ✗ Does Not Qualify

## Main views

### Qualifies tab
Ranked table showing all qualifying companies for the selected year.
Columns: Rank | Company Name | Region | Baseline Turnover | Growth Turnover | Growth % | Review Status
- Rank 1/2/3 should be visually highlighted
- Clicking a row opens a detail/edit panel (see below)
- Sortable columns
- Search by company name
- Filter by region
- Filter by review_status
- Export visible results to CSV

### Manual Review tab
Table of companies needing manual checking.
Columns: Company Name | Region | Reason Flagged | Review Status | Notes
- Clicking a row opens the detail/edit panel
- Show a direct link to their Companies House filing

### Does Not Qualify tab
Simple read-only table. No editing needed here.

## Detail / Edit panel
Slides in from the right when a row is clicked.
Shows all pipeline-generated data read-only at the top, then an editable
section below with these fields:

**Contact details** (editable)
- Contact name
- Contact email
- Contact phone
- Company website URL
- LinkedIn URL

**Internal notes** (editable — large text area)

**Review status** (editable — dropdown)
Options: Pending / In Progress / Contacted / Confirmed / Declined / Disqualified

**Flags** (editable — toggles)
- Confirmed for index (boolean)
- Featured / spotlight (boolean)

Show a "Save changes" button that writes only the editorial fields back to
`index_results` by id. Show a success/error toast on save.

Also show:
- Direct link to Companies House profile
  (https://find-and-update.company-information.service.gov.uk/company/{company_number})
- Direct link to their growth year filing document (growth_document_url)
- Direct link to their baseline filing document (baseline_document_url)

## Pipeline status
In the sidebar footer, show the last pipeline run date and candidate count
from the `pipeline_runs` table (most recent row).

## Design
Dark theme. Professional and data-dense. Use a serif display font for headings
(like Playfair Display) paired with a clean sans-serif for body text.
Gold/amber accent colour. Think financial data terminal meets editorial magazine.
The audience is a small internal team, not the general public.

## Additional notes
- All monetary values displayed in GBP with £ sign and comma formatting
- Growth percentages displayed with one decimal place and + prefix for positive
- The app only needs to support desktop — no need to optimise for mobile
- There will be multiple index years over time (2026, 2027 etc) — the year
  selector must work correctly and filter all views
