import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from utils import *


def transform_lista_eventos_ppi(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming PPIS lista eventos")

    #select and save
    file = "Lista-Eventos_ppi.csv"

    if agency == params.iapmei:
        select_list = ["N_Proj", 'N_Doc', "N_Linha", "Evt_Data"]
    else:
        select_list = ["N_Proj", 'N_Doc_A', "N_Linha", "Evt_Data"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    if agency == params.aicep:
        df = df.rename(columns={'N_Doc_A': 'N_Doc', "Evt_Data": "data_ppi"})
    else:
        df = df.rename(columns={"Evt_Data": "data_ppi"})

    df['max_N_Linha'] = df.groupby(["N_Proj", 'N_Doc'])['N_Linha'].transform(max)

    df = df[(df["N_Linha"] == df["max_N_Linha"])].drop_duplicates()

    if df[["N_Proj", 'N_Doc']].duplicated().any():
        raise Exception('Found duplicates!')

    #drops
    to_drop = ["N_Linha", "max_N_Linha"]
    df = df.drop(columns=to_drop)

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_ppi(agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path,
                  transformed_not_derived_merged_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming PPIS")
    if agency == params.iapmei:
        folder = params.iapmei_ppi
    else:
        folder = params.aicep_ppi

    dfs = []

    dfs.append(transform_lista_eventos_ppi(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))

    previous_df = dfs[0]
    if len(dfs) > 1:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Merging PPIS")

        for df in dfs[1:]:
            previous_df = previous_df.merge(df, on=["N_Proj", "N_Doc"], how="left")

        if previous_df[['N_Proj', "N_Doc"]].duplicated().any():
            raise Exception('Found duplicates in merge!')

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving all PPIS")
    previous_df.to_excel(transformed_not_derived_merged_path + "N_Proj_N_Doc_ppis.xlsx", index=False, encoding='utf-8-sig')
