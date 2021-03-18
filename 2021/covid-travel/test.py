import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

pd.set_option("display.max_rows", None)
sns.set_theme(style="ticks")

df = pd.read_csv("all_by_age_race.csv")
print(df.head())
print(df.dtypes)

dff = (
    df[["age_group", "race_ethnicity", "prob_death_full", "odds_full"]]
    .copy()
    .dropna()
)
dff.columns = ["age_group", "race_ethnicity", "prob_death", "odds"]
dff["full"] = True

dfm = (
    df[["age_group", "race_ethnicity", "prob_death_middle", "odds_middle"]]
    .copy()
    .dropna()
)
dfm.columns = ["age_group", "race_ethnicity", "prob_death", "odds"]
dfm["full"] = False

dfg = dff.append(dfm, ignore_index=True)
print(dfg)


g = sns.catplot(
    x="age_group",
    y="prob_death",
    col="race_ethnicity",
    col_wrap=3,
    hue="full",
    marker="o",
    palette="husl",
    kind="swarm",
    data=dfg,
)
g.set_xticklabels(rotation=30)
g.set_xlabels("Age group")
g.set_ylabels("Probability of dying")

g.savefig("covid_and_air_travel.png")

plt.show()
