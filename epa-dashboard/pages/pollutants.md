---
title: Pollutant Comparison
---

# Pollutant Deep-Dive

```sql pollutant_list
select distinct pollutant
from epa.mart_aqi_annual_trends
order by pollutant
```

<Dropdown
    data={pollutant_list}
    name=selected_pollutant
    value=pollutant
    defaultValue="PM2.5"
    title="Select Pollutant"
/>

---

```sql pol_kpi_2023
select avg_mean_value, median_mean_value, site_count, yoy_change_pct
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.selected_pollutant.value}'
  and year = 2023
```

```sql pol_kpi_best_year
select year, avg_mean_value
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.selected_pollutant.value}'
order by avg_mean_value asc
limit 1
```

```sql pol_kpi_worst_year
select year, avg_mean_value
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.selected_pollutant.value}'
order by avg_mean_value desc
limit 1
```

<BigValue
    data={pol_kpi_2023}
    value=avg_mean_value
    title="2023 National Avg"
    fmt=num4
    comparison=yoy_change_pct
    comparisonTitle="YoY % change"
    comparisonFmt=pct1
    downIsGood=true
/>
<BigValue
    data={pol_kpi_2023}
    value=median_mean_value
    title="2023 National Median"
    fmt=num4
/>
<BigValue
    data={pol_kpi_2023}
    value=site_count
    title="Monitoring Sites (2023)"
/>
<BigValue
    data={pol_kpi_best_year}
    value=year
    title="Best Year (lowest avg)"
    comparison=avg_mean_value
    comparisonTitle="avg concentration"
    comparisonFmt=num4
/>
<BigValue
    data={pol_kpi_worst_year}
    value=year
    title="Worst Year (highest avg)"
    comparison=avg_mean_value
    comparisonTitle="avg concentration"
    comparisonFmt=num4
/>

---

## National Average Trend · 2019–2023

```sql pol_trend_band
select year, avg_mean_value, median_mean_value, p10_value, p90_value, site_count
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.selected_pollutant.value}'
order by year
```

<LineChart
    data={pol_trend_band}
    x=year
    y=avg_mean_value
    title="National Average Concentration · 2019–2023"
    xAxisTitle="Year"
    yAxisTitle="Concentration"
    markers=true
    labels=true
/>

<LineChart
    data={pol_trend_band}
    x=year
    y={['p10_value','avg_mean_value','p90_value']}
    title="p10 / Avg / p90 Percentile Band"
    xAxisTitle="Year"
    yAxisTitle="Concentration"
    markers=true
/>

---

## Top 15 Most Polluted States (2023)

```sql pol_top_states
select state_name, avg_mean_value, national_avg, pct_above_national, site_count, state_rank
from epa.mart_state_comparison
where pollutant = '${inputs.selected_pollutant.value}'
  and year = 2023
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by avg_mean_value desc
limit 15
```

<BarChart
    data={pol_top_states}
    x=state_name
    y=avg_mean_value
    title="Top 15 States by Concentration (2023)"
    xAxisTitle="State"
    yAxisTitle="Avg Concentration"
    referenceLine={pol_top_states[0].national_avg}
    referenceLineLabel="National Avg"
    referenceLineColor=grey
    swapXY=true
/>

---

## Monitoring Network Size · 2019–2023

```sql pol_sites_trend
select year, site_count
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.selected_pollutant.value}'
order by year
```

<BarChart
    data={pol_sites_trend}
    x=year
    y=site_count
    title="Number of Monitoring Sites Reporting"
    xAxisTitle="Year"
    yAxisTitle="Site Count"
/>

---

## All States Comparison (2023)

```sql pol_all_states
select state_rank, state_name, avg_mean_value, max_mean_value, national_avg, pct_above_national, site_count
from epa.mart_state_comparison
where pollutant = '${inputs.selected_pollutant.value}'
  and year = 2023
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by state_rank
```

<DataTable data={pol_all_states} rows=15 search=true>
    <Column id=state_rank title="Rank"/>
    <Column id=state_name title="State"/>
    <Column id=avg_mean_value title="Avg Conc." fmt=num4/>
    <Column id=max_mean_value title="Max Site" fmt=num4/>
    <Column id=national_avg title="National Avg" fmt=num4/>
    <Column id=pct_above_national title="vs National %" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=site_count title="Sites"/>
</DataTable>
