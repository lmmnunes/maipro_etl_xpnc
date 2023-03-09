import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from utils import *


def transform_geral(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming FACIS geral")

    #select and save
    file = "Geral_faci.csv"
    select_list = ["Sgo/Nproj", 'N_Doc', "Analise/Pontuacao", "Analise/Crit_A", "Analise/Crit_B",
                   "Analise/Crit_C", "Analise/Crit_D", "Analise/Parecer"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    df = df.rename(columns={"Sgo/Nproj": "N_Proj", "Analise/Pontuacao": "faci_pont",
                            "Analise/Crit_A": "faci_pont_a", "Analise/Crit_B": "faci_pont_b",
                            "Analise/Crit_C": "faci_pont_c", "Analise/Crit_D": "faci_pont_d",
                            "Analise/Parecer": "projs_aprov_prom"})

    df = df[(df["projs_aprov_prom"] == "E")]

    df['max_N_Doc'] = df.groupby(["N_Proj"])['N_Doc'].transform(max)
    df = df[(df['N_Doc'] == df['max_N_Doc'])]

    #drops
    to_drop = ["N_Doc", "max_N_Doc"]
    df = df.drop(columns=to_drop)

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_faci(agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path,
                   transformed_not_derived_merged_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming FACIS")
    if agency == params.iapmei:
        folder = params.iapmei_faci
    else:
        folder = params.aicep_faci

    dfs = []

    dfs.append(transform_geral(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))

    previous_df = dfs[0]
    if len(dfs) > 1:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Merging FACIS")

        for df in dfs[1:]:
            previous_df = previous_df.merge(df, on=["N_Proj"], how="left")

    if previous_df['N_Proj'].duplicated().any():
        raise Exception('Found duplicates in merge!')

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving all FACIS")
    previous_df.to_excel(transformed_not_derived_merged_path + "N_Proj_facis.xlsx", index=False, encoding='utf-8-sig')
