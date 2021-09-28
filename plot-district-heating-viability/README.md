# Heat Demand Density

## What `pipeline.py` is doing:

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

## Caveats

To fully reproduce the pipeline the user must:
- Have access to the codema-dev s3 to access the underlying Codema-only datasets - *i.e. they need an authenticated AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY*
- Be comfortable enough with the command line to create a conda environment from an `environment.yml` and run the pipeline with `python pipeline.py`

To open up this methodology to the public the pipeline could be adapted to default to public data if it can't connect to s3, however, this would only be worthwhile if a UI entrypoint to the ETL was created to mitigate the need for command line usage


## Setup
(**Note**: Skip if running in binder or deepnote)

Via [conda](https://github.com/conda-forge/miniforge):

- Minimal
```bash
conda env create --file environment.yml
conda activate hdd
```

- Or development - *i.e. contains some helper libraries for linting, formatting & testing not required to run the code*
```bash
conda env create --file environment.dev.yml
conda activate hdd
```

Now run the pipeline:

```bash
python pipeline.py
```