---
title: State Deep-Dive
---

# State Air Quality Deep-Dive

```sql state_list
select distinct state_name
from epa.mart_state_comparison
where state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by state_name
```

<Dropdown
    data={state_list}
    name=selected_state
    value=state_name
    defaultValue="California"
    title="Select State"
/>

---

```sql state_kpi_pm25
select state_rank, avg_mean_value, national_avg, pct_above_national, site_count
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
  and pollutant = 'PM2.5'
  and year = 2023
```

```sql state_kpi_best_pollutant
select pollutant, pct_above_national
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
  and year = 2023
order by pct_above_national asc
limit 1
```

```sql state_kpi_worst_pollutant
select pollutant, pct_above_national
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
  and year = 2023
order by pct_above_national desc
limit 1
```

<BigValue
    data={state_kpi_pm25}
    value=state_rank
    title="PM2.5 State Rank (2023)"
    subtitle="1 = most polluted"
/>
<BigValue
    data={state_kpi_pm25}
    value=avg_mean_value
    title="PM2.5 State Avg µg/m³"
    fmt=num4
    comparison=pct_above_national
    comparisonTitle="vs national avg"
    comparisonFmt=pct1
    downIsGood=true
/>
<BigValue
    data={state_kpi_best_pollutant}
    value=pollutant
    title="Best Pollutant vs National"
    comparison=pct_above_national
    comparisonTitle="% vs national"
    comparisonFmt=pct1
    downIsGood=true
/>
<BigValue
    data={state_kpi_worst_pollutant}
    value=pollutant
    title="Worst Pollutant vs National"
    comparison=pct_above_national
    comparisonTitle="% vs national"
    comparisonFmt=pct1
    downIsGood=true
/>
<BigValue
    data={state_kpi_pm25}
    value=site_count
    title="PM2.5 Monitoring Sites (2023)"
/>

---

## State vs National Average · All Pollutants · 2019–2023

```sql state_vs_national
select pollutant || ' (state)'    as series, year, avg_mean_value as value
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
union all
select pollutant || ' (national)' as series, year, national_avg   as value
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
order by series, year
```

<LineChart
    data={state_vs_national}
    x=year
    y=value
    series=series
    title="State Average vs National Average by Pollutant"
    xAxisTitle="Year"
    yAxisTitle="Concentration"
    markers=true
/>

---

## % Above / Below National Average by Pollutant (2023)

```sql state_deviation_2023
select pollutant, pct_above_national, avg_mean_value, national_avg
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
  and year = 2023
order by pct_above_national desc
```

<BarChart
    data={state_deviation_2023}
    x=pollutant
    y=pct_above_national
    title="% Above/Below National Average (2023) — positive = worse"
    xAxisTitle="Pollutant"
    yAxisTitle="% vs National Avg"
    referenceLine=0
    referenceLineColor=grey
/>

---

## All Years × All Pollutants

```sql state_all_years
select year, pollutant, avg_mean_value, national_avg, pct_above_national, max_mean_value, site_count, state_rank
from epa.mart_state_comparison
where state_name = '${inputs.selected_state.value}'
order by pollutant, year
```

<DataTable data={state_all_years} rows=25>
    <Column id=year title="Year"/>
    <Column id=pollutant title="Pollutant"/>
    <Column id=avg_mean_value title="State Avg" fmt=num4/>
    <Column id=national_avg title="National Avg" fmt=num4/>
    <Column id=pct_above_national title="vs National %" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=max_mean_value title="Worst Site" fmt=num4/>
    <Column id=site_count title="Sites"/>
    <Column id=state_rank title="State Rank"/>
</DataTable>
