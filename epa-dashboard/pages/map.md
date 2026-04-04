---
title: Pollution Hotspot Map
---

# Pollution Hotspot Map

```sql filter_pollutants_map
select distinct pollutant from epa.mart_site_map order by pollutant
```
```sql filter_years_map
select distinct year from epa.mart_site_map order by year desc
```

<Dropdown
    data={filter_pollutants_map}
    name=sel_pollutant
    value=pollutant
    defaultValue="PM2.5"
    title="Pollutant"
/>
<Dropdown
    data={filter_years_map}
    name=sel_year
    value=year
    defaultValue={2023}
    title="Year"
/>

---

```sql map_sites
select
    site_id,
    state_name,
    county_name,
    pollutant,
    year,
    mean_value,
    unit,
    national_rank,
    state_rank,
    latitude,
    longitude
from epa.mart_site_map
where pollutant = '${inputs.sel_pollutant.value}'
  and year = ${inputs.sel_year.value}
  and latitude is not null
  and longitude is not null
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by national_rank
```

<PointMap
    data={map_sites}
    lat=latitude
    long=longitude
    value=mean_value
    tooltipType=hover
    tooltip={[
        {id: 'state_name', title: 'State'},
        {id: 'county_name', title: 'County'},
        {id: 'mean_value', title: 'Concentration', fmt: 'num4'},
        {id: 'unit', title: 'Unit'},
        {id: 'national_rank', title: 'National Rank'}
    ]}
    title="Monitoring Sites — {inputs.sel_pollutant.value} ({inputs.sel_year.value})"
    colorScale=negative
/>

---

## Top 25 Worst Sites — {inputs.sel_pollutant.value} ({inputs.sel_year.value})

```sql top25_sites
select
    national_rank,
    state_name,
    county_name,
    site_id,
    mean_value,
    unit,
    state_rank
from epa.mart_site_map
where pollutant = '${inputs.sel_pollutant.value}'
  and year = ${inputs.sel_year.value}
  and national_rank <= 25
  and state_name not in ('Country Of Mexico','Virgin Islands','Unknown')
order by national_rank
```

<DataTable data={top25_sites} rows=25>
    <Column id=national_rank title="National Rank"/>
    <Column id=state_rank title="State Rank"/>
    <Column id=state_name title="State"/>
    <Column id=county_name title="County"/>
    <Column id=site_id title="Site ID"/>
    <Column id=mean_value title="Concentration" fmt=num4/>
    <Column id=unit title="Unit"/>
</DataTable>
