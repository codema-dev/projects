# %%
from pathlib import Path

import dask.dataframe as dd
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.linear_model import LinearRegression
import numpy as np

sns.set()
data_dir = Path("../data")

# %% [markdown]
# # Get >2011 Dublin Building Stock
# ... BER Public Search stock is complete from ~2011 onwards

# %%
ber_public = dd.read_parquet(data_dir / "BERPublicsearch_parquet")

# %%
stock_built_post_2011 = (
    ber_public.loc[:, ["CountyName", "Year_of_Construction"]]
    .loc[ber_public["CountyName"].str.contains("Dublin")]
    .query("Year_of_Construction > 2010 and Year_of_Construction < 2021")
    .loc[:, "Year_of_Construction"]
    .value_counts()
    .compute()
    .sort_index()
)

# %% [markdown]
# # Plot Stock Growth Trend to 2030

# %% [markdown]
# ## Plot ESRI Stock Growth Trend to 2030
# https://www.esri.ie/news/around-28000-new-houses-needed-per-year-over-the-long-term-to-keep-up-with-population-growth

# %%
esri_housing_demand_projections = pd.DataFrame(
    {
        "num_built": [
            725,
            400,
            956,
            1813,
            2469,
            4471,
            5914,
            7470,
            8513,
            3562,
            4333 + 1930 + 2152 + 2058,
            3278 + 1636 + 1381 + 1491,
            3415 + 1550 + 977 + 1233,
        ],
        "years": [
            2011,
            2012,
            2013,
            2014,
            2015,
            2016,
            2017,
            2018,
            2019,
            2020,
            2021,
            2026,
            2031,
        ],
    }
).set_index("years")
years_2011_to_2030 = np.arange(2011, 2032)
esri_housing_demand_projections = esri_housing_demand_projections.reindex(
    years_2011_to_2030
).interpolate()

# %%
f, ax = plt.subplots(figsize=(20, 20))

plt.bar(
    esri_housing_demand_projections.index,
    esri_housing_demand_projections["num_built"],
    alpha=0.5,
)
plt.bar(
    [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020],
    [
        725,
        400,
        956,
        1813,
        2469,
        4471,
        5914,
        7470,
        8513,
        3562,
    ],
)

plt.xticks(rotation=60)
plt.title("ESRI Dublin Residential Building Construction Trend to 2030")
plt.xlabel("Year of Construction")
plt.ylabel("Number of Buildings")

# %%
f.savefig(data_dir / "esri-residential-building-growth-to-2030.png")

# %%
esri_cumsum_from_2018 = esri_housing_demand_projections["num_built"].cumsum()

# %%
f, ax = plt.subplots(figsize=(20, 20))
plt.bar(esri_housing_demand_projections.index, esri_cumsum_from_2018, alpha=0.5)
plt.bar(
    [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020],
    [725, 1125, 2081, 3894, 6363, 10834, 16748, 24218, 32731, 36293],
)
plt.xticks(rotation=60)
plt.title("ESRI Dublin Residential Building 'New Build' Growth to 2030")
plt.xlabel("Year of Construction")
plt.ylabel("Number of Buildings")

# %%
f.savefig(data_dir / "esri-cumulative-residential-building-growth-2018-to-2030.png")

# %% [markdown]
# ## Plot Derived Stock Growth Trend to 2030

# %%
X = stock_built_post_2011.index.to_numpy().reshape(-1, 1)
y = stock_built_post_2011.to_numpy()
to_predict = np.concatenate([X, np.arange(2021, 2031).reshape(-1, 1)])

# %%
linear_model = LinearRegression()
linear_model.fit(X, y)
prediction = linear_model.predict(to_predict)

# %%
best_fit_from_2011 = prediction
high_scenario_from_2011 = best_fit_from_2011 * 1.5
low_scenario_from_2011 = best_fit_from_2011 * 0.5
years_from_2011 = to_predict.reshape(-1)

# %%
f, ax = plt.subplots(figsize=(20, 20))
plt.bar(years_from_2011, best_fit_from_2011, alpha=0.5)
plt.bar(X.reshape(-1), y)
plt.plot(years_from_2011, best_fit_from_2011)
plt.plot(years_from_2011, high_scenario_from_2011)
plt.plot(years_from_2011, low_scenario_from_2011)
plt.xticks(rotation=60)
plt.title("Dublin Residential Building Construction Trend to 2030")
plt.xlabel("Year of Construction")
plt.ylabel("Number of Buildings")

# %%
f.savefig(data_dir / "residential-building-growth-2011-to-2030.png")

# %%
actual_cumsum = np.cumsum(y)
predicted_cumsum = np.cumsum(prediction)
high_scenario = predicted_cumsum * 1.5
low_scenario = predicted_cumsum * 0.5

# %%
f, ax = plt.subplots(figsize=(20, 20))
plt.bar(years_from_2011, predicted_cumsum, alpha=0.5)
plt.bar(
    X.reshape(-1),
    actual_cumsum,
)
plt.plot(years_from_2011, predicted_cumsum)
plt.plot(years_from_2011, high_scenario)
plt.plot(years_from_2011, low_scenario)
plt.xticks(rotation=60)
plt.title("Dublin Residential Building 'New Build' Growth to 2030")
plt.xlabel("Year of Construction")
plt.ylabel("Number of Buildings")

# %%
f.savefig(data_dir / "cumulative-residential-building-growth-2011-to-2030.png")

# %%
