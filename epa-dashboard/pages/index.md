---
title: US EPA Air Quality — Overview
---

# US EPA Air Quality Dashboard
**2019–2023 · 5 Pollutants · EPA AQS Monitoring Network**

---

```sql kpi_sites
select count(distinct site_id) as total_sites
from epa.mart_site_timeseries
where year = 2023
```

```sql kpi_pollutants
select count(distinct pollutant) as total_pollutants
from epa.mart_aqi_annual_trends
```

```sql kpi_most_improved
select pollutant, avg_mean_value as val_2023, yoy_change_pct
from epa.mart_aqi_annual_trends
where year = 2023 and yoy_change_pct is not null
order by yoy_change_pct asc
limit 1
```

```sql kpi_worst_state_pm25
select state_name, avg_mean_value
from epa.mart_state_comparison
where pollutant = 'PM2.5' and year = 2023
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by avg_mean_value desc
limit 1
```

```sql kpi_worst_yoy
select pollutant, year, yoy_change_pct
from epa.mart_aqi_annual_trends
where year = 2023 and yoy_change_pct is not null
order by yoy_change_pct desc
limit 1
```

<BigValue 
    data={kpi_sites} 
    value=total_sites 
    title="Monitoring Sites (2023)"
/>
<BigValue 
    data={kpi_pollutants} 
    value=total_pollutants 
    title="Pollutants Tracked"
/>
<BigValue 
    data={kpi_most_improved} 
    value=pollutant 
    title="Most Improved Pollutant"
    comparison=yoy_change_pct
    comparisonTitle="YoY % (2023)"
    comparisonFmt=pct1
    downIsGood=true
/>
<BigValue 
    data={kpi_worst_state_pm25} 
    value=state_name 
    title="Most Polluted State · PM2.5"
    comparison=avg_mean_value
    comparisonTitle="µg/m³ (2023)"
/>
<BigValue 
    data={kpi_worst_yoy} 
    value=pollutant 
    title="Biggest YoY Increase (2023)"
    comparison=yoy_change_pct
    comparisonTitle="YoY %"
    comparisonFmt=pct1
    downIsGood=true
/>

---

## National Concentration Trends · 2019–2023

```sql national_trends
select * from epa.mart_aqi_annual_trends
order by pollutant, year
```

<LineChart 
    data={national_trends}
    x=year
    y=avg_mean_value
    series=pollutant
    title="National Average Concentration by Pollutant"
    xAxisTitle="Year"
    yAxisTitle="Avg Concentration"
    markers=true
/>

---

## Year-over-Year % Change by Pollutant

```sql yoy_all
select pollutant, year, yoy_change_pct
from epa.mart_aqi_annual_trends
where yoy_change_pct is not null
order by pollutant, year
```

<LineChart 
    data={yoy_all}
    x=year
    y=yoy_change_pct
    series=pollutant
    title="Year-over-Year % Change (negative = improvement)"
    xAxisTitle="Year"
    yAxisTitle="YoY Change (%)"
    markers=true
    referenceLine=0
    referenceLineColor=grey
    referenceLineLabel="No change"
/>

---

## Most Improved States · 5-Year Change (2019→2023)

```sql most_improved
select state_name, pollutant, avg_2019, avg_2023, pct_change_5yr, site_count_2023
from epa.mart_best_worst_states
where category = 'most_improved'
order by pct_change_5yr asc
limit 20
```

<DataTable data={most_improved} rows=10>
    <Column id=state_name title="State"/>
    <Column id=pollutant title="Pollutant"/>
    <Column id=avg_2019 title="2019 Avg" fmt=num3/>
    <Column id=avg_2023 title="2023 Avg" fmt=num3/>
    <Column id=pct_change_5yr title="5yr Change %" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=site_count_2023 title="Sites (2023)"/>
</DataTable>

---

## Most Worsened States · 5-Year Change (2019→2023)

```sql most_worsened
select state_name, pollutant, avg_2019, avg_2023, pct_change_5yr, site_count_2023
from epa.mart_best_worst_states
where category = 'most_worsened'
order by pct_change_5yr desc
limit 20
```

<DataTable data={most_worsened} rows=10>
    <Column id=state_name title="State"/>
    <Column id=pollutant title="Pollutant"/>
    <Column id=avg_2019 title="2019 Avg" fmt=num3/>
    <Column id=avg_2023 title="2023 Avg" fmt=num3/>
    <Column id=pct_change_5yr title="5yr Change %" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=site_count_2023 title="Sites (2023)"/>
</DataTable>
