# dublin-building-stock

Creates a live, open Dublin building stock at Postcode level by merging <2011 Census data with the BER Public Search.

## Download

[Dublin Residential Building Stock up to 2011](https://zenodo.org/record/4552498) - Central Statistics Office (CSO) Small Area cross-tabulation of period built | dwelling type anonymised to Postcode level 
## Installation

Clone a local copy
```bash
git clone https://github.com/codema-dev/dublin-building-stock
```

> If you're using Windows use [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/) to avoid having to install any compilers (some of this repo's dependencies on are written in C)

Via [conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/) & [poetry](https://python-poetry.org/docs/) (recommended)
```bash
conda env create --file environment.yml
poetry install
```

Via [poetry](https://python-poetry.org/docs/)
```
poetry install
```
