# Lovable Prompt — Sector Pipeline Control & Enhanced Filtering

Paste this into Lovable. Run the SQL migration in supabase_sector_migration.sql first.

---

I need several updates to the Fast Growth Index dashboard. Please implement all of these:

## 1. Run Pipeline panel — add sector selection

Update the "Run Pipeline" panel with:

**Sector selector** — a dropdown populated from GET [RAILWAY_URL]/api/pipeline/sectors
showing all available sectors with their labels e.g:
- All Sectors (default, no filter)
- Technology & Software
- Professional Services
- Manufacturing
- Wholesale & Retail
- Health & Life Sciences
- Construction & Property
- Logistics & Transport
- Financial Services
- Media & Entertainment
- Education & Training

When starting a run, include the selected sector key in the POST body as "sector".
If "All Sectors" is selected, omit the sector field entirely.

**Sector progress tracker** — below the start form, show a grid of all 10 sectors
for the currently selected index year. Pull from GET [RAILWAY_URL]/api/pipeline/sectors
and from the v_sector_progress Supabase view.

Each sector card should show:
- Sector name
- ✓ Complete (green) / ○ Not run (grey) / ⟳ Running (amber, only if active run matches)
- If complete: candidates found, qualifies count, date completed

This gives a clear view of coverage — which sectors have been swept and which haven't.

## 2. Results tables — add Latest Turnover column and enhanced filtering

Update ALL three tabs (Qualifies, Manual Review, Does Not Qualify) to:

**Use v_all_results view** as the data source, filtering by status.

**Add "Latest Turnover" column** showing growth_turnover (the most recently filed 
accounts turnover). Format as £X,XXX,XXX. This should be sortable.

**Add "Latest Accounts Date" column** showing growth_period_end formatted as 
MMM YYYY (e.g. "Mar 2025"). This should be sortable.

**Enhanced filter bar** — add these additional filters alongside the existing 
search and region filter:

- **Turnover range** — two number inputs "Min £" and "Max £" filtering on 
  growth_turnover (latest turnover). Allow blank for no limit.
  
- **Growth % range** — two number inputs "Min %" and "Max %" filtering on 
  growth_percent. Only show on Qualifies and Does Not Qualify tabs.

- **SIC code search** — text input that filters on sic_codes field 
  (partial match). Show a small helper text "e.g. 62012 for software".

- **Accounts year** — dropdown showing distinct growth_period_end years 
  present in the data e.g. "2024", "2025", allowing filtering by which 
  year's accounts are shown.

- **Review status** (Manual Review tab only) — dropdown for 
  Pending / In Progress / Contacted / Confirmed / Declined / Disqualified.

**Filter summary bar** — when any filter is active, show a summary row 
above the table like: "Showing 47 companies · Filtered by: London, £1m+, FY24/25"
with an "× Clear all filters" button.

## 3. Export — respect active filters

Update the Export CSV button to export only the currently filtered results,
not all results. The filename should reflect the active filters e.g.
"fast-growth-index-2026-qualifies-london.csv"

## 4. Sector badge on results

In the company detail panel, if the company's SIC code matches a known sector 
group, show a sector badge e.g. "Technology & Software" next to the SIC codes.
Map from these SIC code prefixes:
- 621xx, 631xx → Technology & Software  
- 691xx, 702xx, 711xx, 731xx, 741xx → Professional Services
- 681xx → Construction & Property
- 641xx-661xx → Financial Services
- 861xx-879xx → Health & Life Sciences
