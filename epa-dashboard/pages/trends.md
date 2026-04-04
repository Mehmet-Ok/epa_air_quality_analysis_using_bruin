---
title: National Trends & Distribution
---

# National Trends & Distribution

```sql filter_pollutants_trends
select distinct pollutant from epa.mart_aqi_annual_trends order by pollutant
```

<Dropdown
    data={filter_pollutants_trends}
    name=sel_pollutant
    value=pollutant
    defaultValue="PM2.5"
    title="Pollutant"
/>

---

## Concentration Distribution Band · {inputs.sel_pollutant.value}

```sql band_data
select
    year,
    p10_value,
    avg_mean_value,
    p90_value
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.sel_pollutant.value}'
order by year
```

<LineChart
    data={band_data}
    x=year
    y={['p10_value', 'avg_mean_value', 'p90_value']}
    title="National Distribution — {inputs.sel_pollutant.value}"
    xAxisTitle="Year"
    yAxisTitle="Concentration"
    markers=true
    labels=true
    seriesLabels={['Best 10% of sites', 'National Average', 'Worst 10% of sites']}
/>

---

## 2023 Snapshot — {inputs.sel_pollutant.value}

```sql snapshot_2023
select
    avg_mean_value,
    median_mean_value,
    p10_value,
    p90_value,
    site_count,
    yoy_change_pct
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.sel_pollutant.value}'
  and year = 2023
```

<BigValue
    data={snapshot_2023}
    value=avg_mean_value
    title="National Average (2023)"
    fmt=num4
/>
<BigValue
    data={snapshot_2023}
    value=median_mean_value
    title="Median Site (2023)"
    fmt=num4
/>
<BigValue
    data={snapshot_2023}
    value=p10_value
    title="Best 10% of Sites (2023)"
    fmt=num4
/>
<BigValue
    data={snapshot_2023}
    value=p90_value
    title="Worst 10% of Sites (2023)"
    fmt=num4
/>
<BigValue
    data={snapshot_2023}
    value=site_count
    title="Monitoring Sites (2023)"
/>
<BigValue
    data={snapshot_2023}
    value=yoy_change_pct
    title="YoY Change (2023)"
    fmt=pct1
    downIsGood=true
/>

---

## Full Historical Data — {inputs.sel_pollutant.value}

```sql all_years
select
    year,
    avg_mean_value,
    median_mean_value,
    p10_value,
    p90_value,
    site_count,
    yoy_change_pct
from epa.mart_aqi_annual_trends
where pollutant = '${inputs.sel_pollutant.value}'
order by year desc
```

<DataTable data={all_years} rows=10>
    <Column id=year title="Year"/>
    <Column id=avg_mean_value title="Avg Concentration" fmt=num4/>
    <Column id=median_mean_value title="Median" fmt=num4/>
    <Column id=p10_value title="P10 (Best 10%)" fmt=num4/>
    <Column id=p90_value title="P90 (Worst 10%)" fmt=num4/>
    <Column id=site_count title="Sites"/>
    <Column id=yoy_change_pct title="YoY Change %" contentType=delta downIsGood=true fmt=pct1/>
</DataTable>
