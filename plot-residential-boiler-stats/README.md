# Plot Residential Boiler Stats

## What `pipeline.yaml` is doing:

![pipeline.png](pipeline.png)

## Setup

| :exclamation:  Skip unless running this locally |
|-------------------------------------------------|

Via [conda](https://github.com/conda-forge/miniforge):

```bash
conda env create --file environment.yml --name plot-residential-boiler-stats
conda activate plot-residential-boiler-stats
```

## Run 

Now run the pipeline:

```bash
ploomber build
```