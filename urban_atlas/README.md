# Urban Atlas

Calculate the area of each urban atlas polygon type (i.e. Discontinuous Dense Urban Fabric, Continuous Urban Fabric etc) in each CSO Small Area boundary

> **Caveat**: You must manually download the Urban Atlas data from https://land.copernicus.eu/local/urban-atlas and save it to a new folder called data/raw

## Setup

Via [conda](https://github.com/conda-forge/miniforge):

```bash
conda env create --name urban-fabric --file environment.yml
conda activate urban-fabric
```

Now run the pipeline:

```bash
ploomber build
```