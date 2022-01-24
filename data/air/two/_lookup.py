"""TODO module docstring."""

import pandas as pd

from oiflib.air.two.transform import forward_fill_NCFormat_column

df_raw: pd.DataFrame = pd.read_excel(
    io="https://uk-air.defra.gov.uk/assets/documents/reports/cat09/2006160834_DA_GHGI_1990-2018_v01-04.xlsm",  # noqa Stop black from complaining about long URLs
    sheet_name="England By Source",
    usecols="B:AA",
    skiprows=16,
)

df_lookup: pd.DataFrame = df_raw.pipe(forward_fill_NCFormat_column)[
    ["NCFormat", "IPCC"]
].assign(OIF_category=None)

df_lookup.loc[
    df_raw.NCFormat.str.contains("Land use") & df_raw.IPCC.str.contains("4[A|G]"),
    "OIF_category",
] = "Forestry sink"

df_lookup.loc[df_raw.NCFormat == "Agriculture", "OIF_category"] = "Agriculture"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Business")
    & df_raw.IPCC.str.contains("2[E-F]|2G[1-2]"),
    "OIF_category",
] = "Fluorinated gases"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Industrial")
    & df_raw.IPCC.str.contains("2B9|2C[3-4]"),
    "OIF_category",
] = "Fluorinated gases"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Residential") & df_raw.IPCC.str.contains("2F4"),
    "OIF_category",
] = "Fluorinated gases"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Land use") & df_raw.IPCC.str.contains("4[B-E|_]"),
    "OIF_category",
] = "Land use & land use change"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Business") & df_raw.IPCC.str.contains("5C"),
    "OIF_category",
] = "Waste"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Residential") & df_raw.IPCC.str.contains("5[B-C]"),
    "OIF_category",
] = "Waste"

df_lookup.loc[
    df_raw.NCFormat.str.contains("Waste") & df_raw.IPCC.str.contains("5[A-D]"),
    "OIF_category",
] = "Waste"

df_lookup.to_csv(
    path_or_buf="/home/edfawcetttaylor/repos/oiflib/data/air/two/lookup.csv",
    index=False,
)
