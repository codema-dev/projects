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


# Extract Dublin Gas Lines

<details>
<summary>⚠️ Before running the pipeline you must first fetch the  Gas Networks Ireland CAD Network files</summary>

- Compress `SHP ITM` to zip file `SHP ITM.zip`

- Create a new folder called `data` and drag & drop `SHP ITM.zip` to a new folder within it called `raw`
</details>

## What `pipeline.py` is doing:

![pipeline.png](pipeline.png)

## Run pipeline

On Binder:

```{code-cell} ipython3
!ploomber build
```

OR on your Terminal:

```{code-cell} ipython3
ploomber build
```
