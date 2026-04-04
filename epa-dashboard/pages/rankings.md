---
title: Site Rankings
---

# Monitoring Site Rankings

```sql filter_pollutants
select distinct pollutant from epa.mart_pollutant_ranking order by pollutant
```
```sql filter_years
select distinct year from epa.mart_pollutant_ranking order by year desc
```
```sql filter_states
select '(All States)' as state_name
union all
select distinct state_name from epa.mart_pollutant_ranking
where state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by state_name
```

<Dropdown
    data={filter_pollutants}
    name=sel_pollutant
    value=pollutant
    defaultValue="PM2.5"
    title="Pollutant"
/>
<Dropdown
    data={filter_years}
    name=sel_year
    value=year
    defaultValue={2023}
    title="Year"
/>
<Dropdown
    data={filter_states}
    name=sel_state
    value=state_name
    defaultValue="(All States)"
    title="State (optional)"
/>

---

## Map — Top 25 Worst Sites

```sql worst_sites_map
select
    m.national_rank,
    m.state_name,
    m.county_name,
    m.site_id,
    m.mean_value,
    m.unit,
    m.latitude,
    m.longitude,
    m.state_name || ' · ' || m.county_name || ' (' || m.site_id || ')' as site_label
from epa.mart_site_map m
where m.pollutant = '${inputs.sel_pollutant.value}'
  and m.year = ${inputs.sel_year.value}
  and m.state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
  and ('${inputs.sel_state.value}' = '(All States)' or m.state_name = '${inputs.sel_state.value}')
order by m.national_rank
limit 25
```

<PointMap
    data={worst_sites_map}
    lat=latitude
    long=longitude
    value=mean_value
    tooltipType=hover
    tooltip={[
        {id: 'national_rank', title: 'Rank'},
        {id: 'site_label', title: 'Site'},
        {id: 'mean_value', title: 'Concentration', fmt: 'num4'},
        {id: 'unit', title: 'Unit'}
    ]}
    title="Top 25 Worst Sites — {inputs.sel_pollutant.value} ({inputs.sel_year.value})"
    colorScale=negative
/>

---

## Top 25 Worst Sites — Highest Concentration

```sql worst_sites
select
    r.national_rank,
    r.state_name,
    r.county_name,
    r.site_id,
    r.mean_value,
    r.unit,
    t.yoy_change_pct
from epa.mart_pollutant_ranking r
left join epa.mart_site_timeseries t
    on r.site_id = t.site_id
    and r.pollutant = t.pollutant
    and r.year = t.year
where r.pollutant = '${inputs.sel_pollutant.value}'
  and r.year = ${inputs.sel_year.value}
  and r.state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
  and ('${inputs.sel_state.value}' = '(All States)' or r.state_name = '${inputs.sel_state.value}')
order by r.national_rank
limit 25
```

<DataTable data={worst_sites} rows=25>
    <Column id=national_rank title="Rank"/>
    <Column id=state_name title="State"/>
    <Column id=county_name title="County"/>
    <Column id=site_id title="Site ID"/>
    <Column id=mean_value title="Concentration" fmt=num4/>
    <Column id=unit title="Unit"/>
    <Column id=yoy_change_pct title="YoY Change %" contentType=delta downIsGood=true fmt=pct1/>
</DataTable>

---

## Top 25 Cleanest Sites — Lowest Concentration

```sql best_sites
select
    r.state_name,
    r.county_name,
    r.site_id,
    r.mean_value,
    r.unit,
    t.yoy_change_pct,
    t.pct_vs_national
from epa.mart_pollutant_ranking r
left join epa.mart_site_timeseries t
    on r.site_id = t.site_id
    and r.pollutant = t.pollutant
    and r.year = t.year
where r.pollutant = '${inputs.sel_pollutant.value}'
  and r.year = ${inputs.sel_year.value}
  and r.state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
  and ('${inputs.sel_state.value}' = '(All States)' or r.state_name = '${inputs.sel_state.value}')
order by r.mean_value asc
limit 25
```

<DataTable data={best_sites} rows=25>
    <Column id=state_name title="State"/>
    <Column id=county_name title="County"/>
    <Column id=site_id title="Site ID"/>
    <Column id=mean_value title="Concentration" fmt=num4/>
    <Column id=unit title="Unit"/>
    <Column id=yoy_change_pct title="YoY Change %" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=pct_vs_national title="vs National %" contentType=delta downIsGood=true fmt=pct1/>
</DataTable>

---

## 5-Year Trend — Top 5 Worst Sites

```sql top5_timeseries
select
    t.site_id,
    t.state_name || ' · ' || t.county_name || ' (' || t.site_id || ')' as site_label,
    t.year,
    t.mean_value,
    t.national_avg
from epa.mart_site_timeseries t
where t.pollutant = '${inputs.sel_pollutant.value}'
  and t.site_id in (
      select site_id
      from epa.mart_pollutant_ranking
      where pollutant = '${inputs.sel_pollutant.value}'
        and year = ${inputs.sel_year.value}
        and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
        and ('${inputs.sel_state.value}' = '(All States)' or state_name = '${inputs.sel_state.value}')
      order by national_rank
      limit 5
  )
order by t.site_id, t.year
```

<LineChart
    data={top5_timeseries}
    x=year
    y=mean_value
    series=site_label
    title="5-Year Trend — Top 5 Worst Sites for {inputs.sel_pollutant.value}"
    xAxisTitle="Year"
    yAxisTitle="Concentration"
    markers=true
    referenceLine={top5_timeseries[0].national_avg}
    referenceLineLabel="National Avg"
    referenceLineColor=grey
/>
