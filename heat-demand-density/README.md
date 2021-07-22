# Heat Demand Density

- Load:
    - Valuation Office Floor Areas
    - Residential Small Area Building Energy Ratings
    - CIBSE TM46 & Guide F Energy Benchmarks 
    - Valuation Office Uses linked to Benchmark Categories
    - Local Authority Boundaries
    - Small Area 2016 Boundaries
- Link Small Areas to Local Authorities
- Link Valuation Office to Small Areas
- Apply CIBSE benchmarks to Valuation Office Floor Areas to estimate Non-Residential Heat Demand - *assuming a typical boiler efficiency of 90%*
- Extract individual building DEAP annual space heat and hot water demand estimates to estimate Residential Heat Demand
- Amalgamate Heat Demands from individual building level to Small Area level
- Calculate Demand Density by dividing Small Area Demands by Small Area Polygon Area (km2)
- Link Small Area Demands to Local Authorities
- Save Small Area Demands as a GIS-compatible `geojson` map.

## Setup
(**Note**: Skip if running in binder or deepnote)

Via [conda](https://github.com/conda-forge/miniforge):

- Minimal
```
conda env create --file environment.yml
conda activate hdd
```
gi
- Development
```
conda env create --file environment.dev.yml
conda activate hdd
```