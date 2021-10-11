---
jupytext:
  cell_metadata_filter: -all
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.12.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Aggregate Urban Atlas footprints to Small Areas

<details>
<summary>⚠️ Before running the pipeline you must first fetch the `open-access` copernicus data</summary>

> - Create an account with copernicus [here](https://land.copernicus.eu) and download the `open-access` 2018 Dublin Urban Atlas data [here](https://land.copernicus.eu/local/urban-atlas)
> - Unzip the data, extract the file `IE001L1_DUBLIN_UA2018_v013.gpkg` from within the `Data` folder and zip it to `IE001L1_DUBLIN_UA2018_v013.zip` 
> - Create a new folder called `data` and drag & drop `IE001L1_DUBLIN_UA2018_v013.zip` to a new folder within it called `raw`
</details>

+++

## What `pipeline.yaml` is doing:

![pipeline.png](pipeline.png)

+++

## Run pipeline

On Binder:
```{code-cell} bash
!ploomber build
```

OR on your Terminal:
```{code-cell} bash
ploomber build
```
