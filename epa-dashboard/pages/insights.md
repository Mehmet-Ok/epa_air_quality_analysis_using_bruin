---
title: Key Insights (2019–2023)
---

# Key Insights (2019–2023)

---

## The Big Picture

```sql big_picture_pm25
select avg_mean_value
from epa.mart_aqi_annual_trends
where pollutant = 'PM2.5' and year = 2023
```

```sql most_improved_pollutant
select pollutant, yoy_change_pct
from epa.mart_aqi_annual_trends
where year = 2023 and yoy_change_pct is not null
order by yoy_change_pct asc
limit 1
```

```sql worst_pollutant_trend
select pollutant, yoy_change_pct
from epa.mart_aqi_annual_trends
where year = 2023 and yoy_change_pct is not null
order by yoy_change_pct desc
limit 1
```

<BigValue
    data={big_picture_pm25}
    value=avg_mean_value
    title="National PM2.5 Average (2023)"
    fmt=num4
/>
<BigValue
    data={most_improved_pollutant}
    value=pollutant
    title="Most Improved Pollutant (2023)"
    comparison=yoy_change_pct
    comparisonTitle="YoY Change"
    comparisonFmt=pct1
    downIsGood=true
/>
<BigValue
    data={worst_pollutant_trend}
    value=pollutant
    title="Worst Trend Pollutant (2023)"
    comparison=yoy_change_pct
    comparisonTitle="YoY Change"
    comparisonFmt=pct1
    downIsGood=true
/>

Across the 2019–2023 period, the EPA monitoring network tracked meaningful shifts in air quality across the United States. PM2.5 fine particulate matter — the pollutant most closely tied to health outcomes — showed notable regional variation, with western wildfire corridors driving elevated readings in certain years. Overall, several pollutants trended downward over the five-year window, reflecting continued benefits from clean-air regulations and fleet electrification, though year-over-year progress was uneven. The data highlights a persistent divide between high-burden counties — often in the West and industrial Midwest — and cleaner-air regions, underscoring that national averages can mask significant local disparities.

---

## Winners & Losers — PM2.5 State Performance (5-Year Change)

```sql top5_improved
select state_name, avg_2019, avg_2023, pct_change_5yr, site_count_2023
from epa.mart_best_worst_states
where category = 'most_improved' and pollutant = 'PM2.5'
order by pct_change_5yr asc
limit 5
```

```sql top5_worsened
select state_name, avg_2019, avg_2023, pct_change_5yr, site_count_2023
from epa.mart_best_worst_states
where category = 'most_worsened' and pollutant = 'PM2.5'
order by pct_change_5yr desc
limit 5
```

<Grid cols=2>

<div>

**Most Improved States**

<DataTable data={top5_improved}>
    <Column id=state_name title="State"/>
    <Column id=avg_2019 title="2019 Avg" fmt=num3/>
    <Column id=avg_2023 title="2023 Avg" fmt=num3/>
    <Column id=pct_change_5yr title="5yr Change" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=site_count_2023 title="Sites"/>
</DataTable>

</div>

<div>

**Most Worsened States**

<DataTable data={top5_worsened}>
    <Column id=state_name title="State"/>
    <Column id=avg_2019 title="2019 Avg" fmt=num3/>
    <Column id=avg_2023 title="2023 Avg" fmt=num3/>
    <Column id=pct_change_5yr title="5yr Change" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=site_count_2023 title="Sites"/>
</DataTable>

</div>

</Grid>

---

## Pollutant Scoreboard — 2023 YoY Change

```sql pollutant_scoreboard
select
    pollutant,
    yoy_change_pct,
    case when yoy_change_pct < 0 then 'Improved' else 'Worsened' end as trend
from epa.mart_aqi_annual_trends
where year = 2023 and yoy_change_pct is not null
order by yoy_change_pct asc
```

<BarChart
    data={pollutant_scoreboard}
    x=pollutant
    y=yoy_change_pct
    series=trend
    title="Year-over-Year % Change by Pollutant (2023)"
    xAxisTitle="Pollutant"
    yAxisTitle="YoY Change (%)"
    colorPalette={['#16a34a', '#dc2626']}
    referenceLine=0
    referenceLineColor=grey
    referenceLineLabel="No change"
    labels=true
/>

---

## State Environmental Scorecard — 2023

```sql state_scorecard
select
    state_name,
    pollutant,
    avg_mean_value,
    pct_above_national,
    state_rank
from epa.mart_state_comparison
where year = 2023
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by pct_above_national desc
```

<DataTable data={state_scorecard} rows=25 search=true>
    <Column id=state_name title="State"/>
    <Column id=pollutant title="Pollutant"/>
    <Column id=avg_mean_value title="Avg Concentration" fmt=num4/>
    <Column id=pct_above_national title="vs National Avg" contentType=delta downIsGood=true fmt=pct1/>
    <Column id=state_rank title="State Rank"/>
</DataTable>
