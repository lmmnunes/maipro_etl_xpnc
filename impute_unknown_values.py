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
import params
import sys
from params import *


def impute_unknown_values(agency, type, df, transformed_not_derived_merged_path, mean_path, zero_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Imputing unknown values")
    init_shape = df.shape
    df_original = df
    if type == params.anul:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Creating extra vars for", params.anul)

        transformed_not_derived_merged_path = transformed_not_derived_merged_path.replace(params.anul, params.inelg)
        df_prom_dates = pd.read_csv(transformed_not_derived_merged_path+"\\N_Proj_candidaturas.xlsx")

        df_original = df_original.merge(
            df_prom_dates[["N_Proj", "emp_menos_4_anos_cand", "emp_recente", "nif_consultora", "nif_promotor"]],
            on=["N_Proj"], how='left')

        # checks
        final_shape = df_original.shape
        if init_shape[0] != final_shape[0]:
            raise Exception('Number of initial rows and after transformation rows is not the same!')


    impute_always_0_prom = ["prom_ano_t-1",
                            "prom_CAE_t-1",
                            "prom_ano_t-2",
                            "prom_CAE_t-2"]
    impute_always_0_consul = ["consul_ano_t-1",
                              "consul_CAE_t-1",
                              "consul_ano_t-2",
                              "consul_CAE_t-2"]
    impute_always_0_fornec = [
        "fornec_ano_t-1",
        "fornec_CAE_t-1",
        "fornec_ano_t-2",
        "fornec_CAE_t-2"]
    racios_prom = [
        "prom_n_trabalhadores_t-1",
        "prom_volume_negocios_t-1",
        "prom_ativo_total_t-1",
        "prom_emprestimos_obtidos_passivo_cor_t-1",
        "prom_emprestimos_obtidos_passivo_ncor_t-1",
        "prom_autonomia_financeira_t-1",
        "prom_ebitda/vn_t-1",
        "prom_resultado_liquido/vn_t-1",
        "prom_alavancagem_financeira_t-1",
        "prom_vab/trabalhador_t-1",
        "prom_liquidez_geral_t-1",
        "prom_rentabilidade_capitais_proprios_t-1",
        "prom_cmvmc/fornecedores_t-1",
        "prom_cmvmc/inventario_t-1",
        "prom_resultadoliquido/ativo_t-1",
        "prom_clientes/vn_t-1",
        "prom_crescimento_vn_t-1",
        "prom_dsri_t-1",
        "prom_aqi_t-1",
        "prom_depi_t-1",
        "prom_sgai_t-1",
        "prom_lvgi_t-1",
        "prom_tata_t-1",
        "prom_mscore_t-1",
        "prom_n_trabalhadores_t-2",
        "prom_volume_negocios_t-2",
        "prom_ativo_total_t-2",
        "prom_emprestimos_obtidos_passivo_cor_t-2",
        "prom_emprestimos_obtidos_passivo_ncor_t-2",
        "prom_autonomia_financeira_t-2",
        "prom_ebitda/vn_t-2",
        "prom_resultado_liquido/vn_t-2",
        "prom_alavancagem_financeira_t-2",
        "prom_vab/trabalhador_t-2",
        "prom_liquidez_geral_t-2",
        "prom_rentabilidade_capitais_proprios_t-2",
        "prom_cmvmc/fornecedores_t-2",
        "prom_cmvmc/inventario_t-2",
        "prom_resultadoliquido/ativo_t-2",
        "prom_clientes/vn_t-2",
        "prom_crescimento_vn_t-2",
        "prom_dsri_t-2",
        "prom_aqi_t-2",
        "prom_depi_t-2",
        "prom_sgai_t-2",
        "prom_lvgi_t-2",
        "prom_tata_t-2",
        "prom_mscore_t-2"]
    racios_fornec = [
        "fornec_n_trabalhadores_t-1",
        "fornec_volume_negocios_t-1",
        "fornec_ativo_total_t-1",
        "fornec_emprestimos_obtidos_passivo_cor_t-1",
        "fornec_emprestimos_obtidos_passivo_ncor_t-1",
        "fornec_autonomia_financeira_t-1",
        "fornec_ebitda/vn_t-1",
        "fornec_resultado_liquido/vn_t-1",
        "fornec_alavancagem_financeira_t-1",
        "fornec_vab/trabalhador_t-1",
        "fornec_liquidez_geral_t-1",
        "fornec_rentabilidade_capitais_proprios_t-1",
        "fornec_cmvmc/fornecedores_t-1",
        "fornec_cmvmc/inventario_t-1",
        "fornec_resultadoliquido/ativo_t-1",
        "fornec_clientes/vn_t-1",
        "fornec_crescimento_vn_t-1",
        "fornec_dsri_t-1",
        "fornec_aqi_t-1",
        "fornec_depi_t-1",
        "fornec_sgai_t-1",
        "fornec_lvgi_t-1",
        "fornec_tata_t-1",
        "fornec_mscore_t-1",
        "fornec_n_trabalhadores_t-2",
        "fornec_volume_negocios_t-2",
        "fornec_ativo_total_t-2",
        "fornec_emprestimos_obtidos_passivo_cor_t-2",
        "fornec_emprestimos_obtidos_passivo_ncor_t-2",
        "fornec_autonomia_financeira_t-2",
        "fornec_ebitda/vn_t-2",
        "fornec_resultado_liquido/vn_t-2",
        "fornec_alavancagem_financeira_t-2",
        "fornec_vab/trabalhador_t-2",
        "fornec_liquidez_geral_t-2",
        "fornec_rentabilidade_capitais_proprios_t-2",
        "fornec_cmvmc/fornecedores_t-2",
        "fornec_cmvmc/inventario_t-2",
        "fornec_resultadoliquido/ativo_t-2",
        "fornec_clientes/vn_t-2",
        "fornec_crescimento_vn_t-2",
        "fornec_dsri_t-2",
        "fornec_aqi_t-2",
        "fornec_depi_t-2",
        "fornec_sgai_t-2",
        "fornec_lvgi_t-2",
        "fornec_tata_t-2",
        "fornec_mscore_t-2"]
    racios_consul = [
        "consul_n_trabalhadores_t-1",
        "consul_volume_negocios_t-1",
        "consul_ativo_total_t-1",
        "consul_emprestimos_obtidos_passivo_cor_t-1",
        "consul_emprestimos_obtidos_passivo_ncor_t-1",
        "consul_autonomia_financeira_t-1",
        "consul_ebitda/vn_t-1",
        "consul_resultado_liquido/vn_t-1",
        "consul_alavancagem_financeira_t-1",
        "consul_vab/trabalhador_t-1",
        "consul_liquidez_geral_t-1",
        "consul_rentabilidade_capitais_proprios_t-1",
        "consul_cmvmc/fornecedores_t-1",
        "consul_cmvmc/inventario_t-1",
        "consul_resultadoliquido/ativo_t-1",
        "consul_clientes/vn_t-1",
        "consul_crescimento_vn_t-1",
        "consul_dsri_t-1",
        "consul_aqi_t-1",
        "consul_depi_t-1",
        "consul_sgai_t-1",
        "consul_lvgi_t-1",
        "consul_tata_t-1",
        "consul_mscore_t-1",
        "consul_n_trabalhadores_t-2",
        "consul_volume_negocios_t-2",
        "consul_ativo_total_t-2",
        "consul_emprestimos_obtidos_passivo_cor_t-2",
        "consul_emprestimos_obtidos_passivo_ncor_t-2",
        "consul_autonomia_financeira_t-2",
        "consul_ebitda/vn_t-2",
        "consul_resultado_liquido/vn_t-2",
        "consul_alavancagem_financeira_t-2",
        "consul_vab/trabalhador_t-2",
        "consul_liquidez_geral_t-2",
        "consul_rentabilidade_capitais_proprios_t-2",
        "consul_cmvmc/fornecedores_t-2",
        "consul_cmvmc/inventario_t-2",
        "consul_resultadoliquido/ativo_t-2",
        "consul_clientes/vn_t-2",
        "consul_crescimento_vn_t-2",
        "consul_dsri_t-2",
        "consul_aqi_t-2",
        "consul_depi_t-2",
        "consul_sgai_t-2",
        "consul_lvgi_t-2",
        "consul_tata_t-2",
        "consul_mscore_t-2"]

    if type == params.anul:
        cols = racios_prom + racios_consul + impute_always_0_prom + impute_always_0_consul
    else:
        cols = racios_prom + racios_fornec + racios_consul + \
               impute_always_0_prom + impute_always_0_consul + impute_always_0_fornec


    df = df_original.copy()


    df_stats = df[cols].describe()
    df_stats = df_stats.loc[["mean", "min"]]
    df_stats_T = df_stats.T
    df_stats_T["value"] = df_stats_T["min"] - df_stats_T["mean"]
    df_stats_T


    values = df_stats_T[["value"]].T
    for c in impute_always_0_prom + impute_always_0_consul + impute_always_0_fornec:
        values[c] = 0
    values

    # for c in values.columns:
    #     print(c, " ", values.loc["value", c])

    cols_history_base = ["crescimento_vn",
                         "dsri",
                         "aqi",
                         "depi",
                         "sgai",
                         "lvgi",
                         "tata",
                         "mscore"]

    cols_history = [c for c in df.columns for b in cols_history_base if b in c]

    # print(cols_history)

    if type == params.anul:
        if not (len(cols_history) == 32):
            raise Exception("O tamanho da lista tem que ser 32 porque são 8 vars * 2 tipos * 2 anos")
    else:
        if not (len(cols_history) == 48):
            raise Exception("O tamanho da lista tem que ser 48 porque são 8 vars * 3 tipos * 2 anos")

    # print(df.columns)

    cond_prom = ((df["emp_recente"] == 1) | \
                 (df["nif_prom_valido"] == 0) | \
                 (df["prom_falta_IES_t-1"] == 1) | \
                 (df["prom_falta_IES_t-2"] == 1))

    cond_consul = (pd.isnull(df["nif_consultora"]) | \
                   (df["nif_consul_valido"] == 0) | \
                   (df["consul_falta_IES_t-1"] == 1) | \
                   (df["consul_falta_IES_t-2"] == 1))

    cond_fornec = None
    if type == params.inelg:
        cond_fornec = (pd.isnull(df["nif_fornecedor"]) | \
                       (df["nif_fornec_valido"] == 0) | \
                       (df["tipo_fornec_estrangeiro"] == 1) | \
                       (df["fornec_falta_IES_t-1"] == 1) | \
                       (df["fornec_falta_IES_t-2"] == 1))

    cond_prom_2 = (df["emp_menos_4_anos_cand"] == 1)

    # versão 1

    def impute_mean(cols, cond_1, cond_2=None):
        for c in cols:
            df[c] = np.where(pd.isnull(df[c]) & cond_1, values.loc["value", c], df[c])
            if cond_2 is not None and c in cols_history:
                df[c] = np.where(pd.isnull(df[c]) & cond_2, values.loc["value", c], df[c])

    if type == params.anul:
        impute_mean(racios_prom + impute_always_0_prom, cond_prom, cond_prom_2)
        impute_mean(racios_consul + impute_always_0_consul, cond_consul)
    else:
        impute_mean(racios_prom + impute_always_0_prom, cond_prom, cond_prom_2)
        impute_mean(racios_consul + impute_always_0_consul, cond_consul)
        impute_mean(racios_fornec + impute_always_0_fornec, cond_fornec)

    # display(df)
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving mean impute")
    writer = pd.ExcelWriter(mean_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, header=True, encoding='utf-8-sig')
    writer.book.use_zip64()
    writer.save()

    df_mean = df.copy()

    # versão 2

    df = df_original.copy()

    def impute_0(cols, cond_1, cond_2=None):
        for c in cols:
            df[c] = np.where(pd.isnull(df[c]) & cond_1, 0, df[c])
            if cond_2 is not None and c in cols_history:
                df[c] = np.where(pd.isnull(df[c]) & cond_2, 0, df[c])

    if type == params.anul:
        impute_0(racios_prom + impute_always_0_prom, cond_prom, cond_prom_2)
        impute_0(racios_consul + impute_always_0_consul, cond_consul)
    else:
        impute_0(racios_prom + impute_always_0_prom, cond_prom, cond_prom_2)
        impute_0(racios_consul + impute_always_0_consul, cond_consul)
        impute_0(racios_fornec + impute_always_0_fornec, cond_fornec)

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving zero impute")
    writer = pd.ExcelWriter(zero_path, engine='xlsxwriter')
    df.to_excel(writer, index=False, header=True, encoding='utf-8-sig')
    writer.book.use_zip64()
    writer.save()

    df_zero = df.copy()

    return df_mean, df_zero
