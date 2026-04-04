---
title: Site-Level Trends
---

# Site-Level Trends

```sql filter_pollutants_spark
select distinct pollutant from epa.mart_site_timeseries order by pollutant
```

<Dropdown
    data={filter_pollutants_spark}
    name=sel_pollutant
    value=pollutant
    defaultValue="PM2.5"
    title="Pollutant"
/>

---

## Top 10 Worst Sites in 2023 — {inputs.sel_pollutant.value}

```sql top10_sites_2023
select
    site_id,
    state_name,
    county_name,
    mean_value,
    national_avg
from epa.mart_site_timeseries
where pollutant = '${inputs.sel_pollutant.value}'
  and year = 2023
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by mean_value desc
limit 10
```

```sql top10_timeseries
select
    t.site_id,
    t.state_name,
    t.county_name,
    t.year,
    t.mean_value,
    t.national_avg
from epa.mart_site_timeseries t
where t.pollutant = '${inputs.sel_pollutant.value}'
  and t.site_id in (
      select site_id
      from epa.mart_site_timeseries
      where pollutant = '${inputs.sel_pollutant.value}'
        and year = 2023
        and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
      order by mean_value desc
      limit 10
  )
order by t.site_id, t.year
```

{#each top10_sites_2023 as site}

<LineChart
    data={top10_timeseries.filter(d => d.site_id === site.site_id)}
    x=year
    y=mean_value
    title="{site.state_name} · {site.county_name} ({site.site_id})"
    xAxisTitle="Year"
    yAxisTitle="Concentration"
    markers=true
    referenceLine={site.national_avg}
    referenceLineLabel="National Avg"
    referenceLineColor=grey
/>

{/each}

---

## All Sites — {inputs.sel_pollutant.value} (2023)

```sql all_sites_2023
select
    site_id,
    state_name,
    county_name,
    mean_value,
    unit,
    yoy_change_pct,
    national_avg,
    pct_vs_national
from epa.mart_site_timeseries
where pollutant = '${inputs.sel_pollutant.value}'
  and year = 2023
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by mean_value desc
```

<DataTable data={all_sites_2023} rows=25>
    <Column id=state_name title="State"/>
    <Column id=county_name title="County"/>
    <Column id=site_id title="Site ID"/>
    <Column id=mean_value title="Concentration" fmt=num4/>
    <Column id=unit title="Unit"/>
    <Column id=national_avg title="National Avg" fmt=num4/>
    <Column id=pct_vs_national title="vs National %" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=yoy_change_pct title="YoY Change %" contentType=delta downIsGood=true fmt=pct1/>
</DataTable>
