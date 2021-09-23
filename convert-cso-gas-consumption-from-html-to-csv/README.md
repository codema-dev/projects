# Convert CSO Gas Consumption from HTML to CSV

- Download the annual CSO Gas consumption data as a HTML file
- Extract, clean and convert the tables to CSV

## Setup

Via [conda](https://github.com/conda-forge/miniforge):

```bash
conda env create --name convert-cso-gas-from-html-to-csv --file environment.yml
conda activate convert-cso-gas-from-html-to-csv
```

Now run the pipeline:

```bash
ploomber build
```