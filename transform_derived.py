import datetime as dt
import os
import pandas as pd
from pandas import Timestamp
from sqlalchemy import create_engine
import numpy as np
import datetime
from IPython.display import display
import params
from params import *


def transform_derived(agency, type, df, transformed_derived_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Starting derived transformations")
    init_shape = df.shape

    #target
    df["inelegivel"] = 0

    cond = (df["Desp_Eleg"] > df["An_Eleg_V"])
    df["inelegivel"] = np.where(cond, 1, df["inelegivel"])

    cond = (pd.isnull(df["An_Eleg_V"]) &
            pd.notnull(df["Desp_Eleg"]) &
            ((df["An_Eleg_E"] == 'N') | (df["An_Eleg_E"] == '?')))
    df["inelegivel"] = np.where(cond, 1, df["inelegivel"])

    #dias_entre_data_fat_e_pagam
    df["dias_entre_data_fat_e_pagam"] = (df["data_pagamento"] - df["data_fatura"]).dt.days

    #AFTER CORR fornc_e_consult_proj
    df["fornc_e_consult_proj"] = np.where((df["nif_consultora"] == df["nif_fornecedor"]), 1, 0)

    #AFTER CORR consultora_em_quantos_projs: número de projetos em que a consultora aparece (campo nif_consultora)
    df["nif_consultora"] = pd.to_numeric(df["nif_consultora"], errors='raise')
    df_n_consult = df[["N_Proj", "nif_consultora"]].groupby(["N_Proj", "nif_consultora"]).first() \
        .groupby("nif_consultora").size() \
        .reset_index(name='consultora_em_quantos_projs')
    df = df.merge(df_n_consult[["nif_consultora", "consultora_em_quantos_projs"]], on=["nif_consultora"], how='left')

    #AFTER CORR projs_prom_e_consult: número de projetos em que o promotor é também consultora.
    df_count_consul = df[["nif_consultora", "consultora_em_quantos_projs"]].rename(
        columns={"consultora_em_quantos_projs": "projs_prom_e_consult"})
    df_count_consul = df_count_consul.dropna(subset=["nif_consultora"])
    df_count_consul = df_count_consul.drop_duplicates()
    df["nif_promotor"] = pd.to_numeric(df["nif_promotor"], errors='raise')
    df = df.merge(df_count_consul, left_on=["nif_promotor"], right_on=["nif_consultora"], how="left")
    df["projs_prom_e_consult"] = df["projs_prom_e_consult"].fillna(0)
    df = df.drop(columns=["nif_consultora_y"])
    df = df.rename(columns={"nif_consultora_x": "nif_consultora"})

    #projs_prom_e_fornec: número de projetos em que o promotor é também fornecedor.
    df_grouped = df.groupby(["N_Proj", "nif_fornecedor"])["N_Proj"].count().reset_index(name="n_rubricas_fornec")
    df_grouped_2 = df_grouped.groupby(["nif_fornecedor"])["nif_fornecedor"].count().reset_index(
        name="projs_prom_e_fornec")
    df_grouped_2["nif_fornecedor"] = df_grouped_2["nif_fornecedor"].astype(str).str.strip()
    df["nif_promotor"] = df["nif_promotor"].astype(str).str.strip()
    df = df.merge(df_grouped_2, left_on=["nif_promotor"], right_on=["nif_fornecedor"], how="left")
    df["projs_prom_e_fornec"] = df["projs_prom_e_fornec"].fillna(0)
    df = df.drop(columns=["nif_fornecedor_y"])
    df = df.rename(columns={"nif_fornecedor_x": "nif_fornecedor"})

    #fornc_e_socio_proj
    folder = params.iapmei_cand
    file = "Socios_candidaturas.csv"
    select_list = ["N_Proj", "Nif"]

    full_df = pd.read_csv(folder + file)
    df_socios = full_df[select_list].drop_duplicates()

    df_socios["Nif"] = df_socios["Nif"].astype(str).str.strip()
    df["nif_fornecedor"] = df["nif_fornecedor"].astype(str).str.strip()
    df = df.merge(df_socios, left_on=["N_Proj", "nif_fornecedor"], right_on=["N_Proj", "Nif"], how="left")
    df["fornc_e_socio_proj"] = np.where((pd.notnull(df["nif_fornecedor"]) & (df["nif_fornecedor"] == df["Nif"])), 1, 0)


    # fornc_menos_3_anos: 1 se o ano de data_fatura - o ano mais antigo entregue da ies desse fornecedor <=3, nulo se um dos campos for nulo, 0 c.c.
    nif_type = "nif_fornecedor"
    ies_name = "fornec"

    if agency == "iapmei":
        folder = params.iapmei_ies

        ies_singular = "racios_" + ies_name + "_singulares.xlsx"
        ies_coletivo = "racios_" + ies_name + "_coletivos.xlsx"

        df_ies_singular = pd.read_excel(folder + ies_singular, engine='openpyxl')
        df_ies_coletivo = pd.read_excel(folder + ies_coletivo, engine='openpyxl')

        df_ies = pd.concat([df_ies_singular, df_ies_coletivo])[["nif", "ano"]] \
            .rename(columns={"nif": nif_type, "ano": "ano_ies"})
    else:
        ies_aicep = "racios_aicep.xlsx"
        df_ies = pd.read_excel(params.aicep_ies + ies_aicep, engine='openpyxl')[["nif", "ano"]] \
            .rename(columns={"nif": nif_type, "ano": "ano_ies"})

    try:
        df_ies[nif_type] = df_ies[nif_type].astype(str).str.strip()
        df[nif_type] = df[nif_type].astype(str).str.strip()
    except Exception:
        df_ies[nif_type] = df_ies[nif_type].astype(object)
        df[nif_type] = df[nif_type].astype(object)

    df_ies['min_ano'] = df_ies.groupby([nif_type])['ano_ies'].transform(min)
    df_ies_grouped = df_ies[(df_ies['ano_ies'] == df_ies['min_ano'])]
    df_ies_grouped = df_ies_grouped[[nif_type,"ano_ies"]].drop_duplicates()

    df = df.merge(df_ies_grouped, on=nif_type, how='left')

    df["ano_fatura"] = df["data_fatura"].apply(lambda data: data.year).astype(int)

    df["anos_ate_fatura"] = df["ano_fatura"] - df["ano_ies"]

    df["fornc_menos_3_anos"] = np.where((df["anos_ate_fatura"] <= 3), 1, 0)
    df["fornc_menos_3_anos"] = np.where(pd.isnull(df["anos_ate_fatura"]), np.nan, df["fornc_menos_3_anos"])

    df["nif_fornecedor"] = df["nif_fornecedor"].replace(["None", np.nan, "nan"], "")

    df = df.drop(columns=["Nif", "ano_fatura", "ano_ies", "Poc_Data", "Quit_Data"])

    # checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving derived transformations")
    df.to_excel(transformed_derived_path, index=False, encoding='utf-8-sig')
    return df

