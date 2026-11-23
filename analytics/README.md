# StreamLake Phase 7 Analytics

The SQL in `sql/business_queries.sql` targets the local dbt DuckDB Gold mart at
`data/streamlake_phase3.duckdb`. Run `python scripts/run_phase7_analytics.py` to
execute and validate every query against the actual Gold tables and save a
sample output. The runner attaches actual Phase 6 metrics when available. If
there are no recorded metrics, the rejection trend correctly returns no rows.

Export actual Gold datasets for Power BI:

```zsh
python scripts/export_powerbi_gold.py
```

The exporter writes CSV snapshots to ignored `data/powerbi/`. It reads the
current DuckDB tables and only exports operational metrics when that table
exists. Refresh exports after a successful dbt build. Power BI Desktop requires
Windows, so on macOS use a Windows VM/desktop for the full modeling experience,
or use Power BI Service. Microsoft's current service flow supports creating a
semantic model from an uploaded CSV in a workspace; CSVs stored in OneDrive for
work or SharePoint can stay connected for refresh. See [Power BI Desktop
requirements](https://learn.microsoft.com/en-us/power-bi/fundamentals/desktop-get-the-desktop)
and [CSV import in Power BI Service](https://learn.microsoft.com/en-us/power-bi/connect-data/service-comma-separated-value-files).

For Service, sign in to a workspace, choose **New item → Semantic model → CSV**,
and upload one exported table at a time (or select its OneDrive/SharePoint
location). Create relationships and report pages from
`powerbi/dashboard_spec.md`; repeat for each table needed by a page and keep key
columns typed as text. Service features and sharing/refresh options depend on
tenant licensing. Do not connect Power BI directly to mutable DuckDB files on a
shared/network drive.

See `powerbi/dashboard_spec.md` for report pages and `powerbi/data_dictionary.md`
for grains, keys, and relationships. The local dataset currently has no
dependent-level fact: dependent visuals use the existing `dependent_distribution`
Gold aggregate. Rejection trend history appears only after real executions
record metrics; an empty query result means no observations are available.
