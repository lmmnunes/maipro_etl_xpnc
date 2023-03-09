
import datetime as dt
import os
import pandas as pd
from pandas import Timestamp
from sqlalchemy import create_engine
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import datetime
from matplotlib.ticker import MaxNLocator
from matplotlib.pyplot import figure as fig
import matplotlib.ticker as mticker
from IPython.display import display
import sys

import params
from params import *


def drop_unwanted(agency, type, df, df_type, transformed_not_derived_merged_path, save_path):
    # selecionar apenas as colunas de features e retirar os projetos não elegíveis,
    # o que não é amostra e as rubricas com classe de despesa 999
    print(dt.datetime.now().strftime("%H:%M:%S"), "Dropping unwanted rows and columns")

    ies_not = ["ano","ano_ies", "prom_ano_t-1", "prom_ano_t-2", "consul_ano_t-1", "consul_ano_t-2",
                "fornec_ano_t-1", "fornec_ano_t-2", "fornec_CAE_t-1", "fornec_CAE_t-2",
                "consul_CAE_t-1", "consul_CAE_t-2", "prom_CAE_t-2","prom_CAE_t-1"]

    anul_not = ["nif_consultora","nif_promotor","projs_aprov_prom"] + ies_not
    ineleg_not = ["Poc_Conta","amostra","projs_aprov_prom","data_fatura", "nif_fornecedor", "Poc_Data", "Quit_Data", "data_pagamento",
                  "An_Eleg_E", "An_Eleg_V", "tipo_amostra", "classe_grupo", "data_ppi",
                  "ano_candidatura", "nif_consultora", "data_inicio_projeto", "data_fim_projeto",
                  "nif_promotor", "abertura_atividade_prom_cand", "ano_abertura_atividade_prom_cand",
                  "nome_grupo_cae_proj"] + ies_not
    init_shape = df.shape
    if type == params.anul:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Creating projs_aprov_prom for", params.anul)

        transformed_not_derived_merged_path = transformed_not_derived_merged_path.replace(params.anul, params.inelg)
        projs_aprov_prom = pd.read_csv(transformed_not_derived_merged_path+"\\N_Proj_facis.xlsx")

        df = df.merge(projs_aprov_prom[["N_Proj","projs_aprov_prom"]], on=["N_Proj"], how='left')
        df = df[df["projs_aprov_prom"] == "E"]

        # checks
        final_shape = df.shape
        if init_shape[0] != final_shape[0]:
            raise Exception('Number of initial rows and after transformation rows is not the same!')

        df_sel = df[[c for c in df.columns if c not in anul_not]]
    else:
        df = df[df["projs_aprov_prom"] == "E"]
        df = df[df["amostra"] == 1]
        df = df[df["Classe"] != "999"]
        df = df[df["Classe"] != 999]

        df_sel = df[[c for c in df.columns if c not in ineleg_not]]

    # drop and stats
    print(dt.datetime.now().strftime("%H:%M:%S"), "Creating dropped stats")
    path = params.save_root_folder + agency + "_" + type + "_" + "drop_stats_" + df_type + ".txt"
    orig_stdout = sys.stdout
    f = open(path, 'w')
    sys.stdout = f
    # print stats antes drop
    print("Antes drop: nº colunas:", df_sel.shape[1], "nº linhas:", df_sel.shape[0])
    print("Análise por target:")
    if type == "inelegibilidades":
        print(df_sel[["N_Proj","inelegivel"]].groupby(["inelegivel"]).count())
    else:
        print(df_sel[["N_Proj","target"]].groupby(["target"]).count())
    print()
    cols_before = df_sel.columns
    df_sel = df_sel.dropna(axis=1, thresh=(len(df)-(len(df)*0.05)))
    df_sel = df_sel.dropna(axis=0)
    cols_removed = [c for c in cols_before if c not in df_sel.columns]
    # print stats depois drop
    print("Depois drop: nº colunas:", df_sel.shape[1], "nº linhas:", df_sel.shape[0])
    print("Análise por target:")
    if type == "inelegibilidades":
        print(df_sel[["N_Proj","inelegivel"]].groupby(["inelegivel"]).count())
    else:
        print(df_sel[["N_Proj","target"]].groupby(["target"]).count())
    print()
    # print colunas retiradas
    print("Colunas que caíram:")
    for c in cols_removed:
        print(c)
    print()
    f.close()
    sys.stdout = orig_stdout

    #save
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving dropped dataset")
    writer = pd.ExcelWriter(save_path, engine='xlsxwriter')
    df_sel.to_excel(writer, index = False, header=True, encoding='utf-8-sig')
    writer.book.use_zip64()
    writer.save()

    return df_sel
