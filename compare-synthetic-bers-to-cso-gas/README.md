# Compare Synthetic BERs to CSO Gas Consumption

- Download County & Dublin Postal District annual CSO Gas consumption data
- (closed-access) Download Synthetic Dublin Building Energy Ratings (BERs)
- Concatenate County Dublin gas consumption with Dublin Postal District consumption
- Amalgamate the BERs to postal districts
- Plot gas vs estimated heat 

## Setup

Via [conda](https://github.com/conda-forge/miniforge):

```bash
conda env create --name compare-bers-to-cso-gas --file environment.yml
conda activate compare-bers-to-cso-gas
```

Now run the pipeline:

```bash
ploomber build
```