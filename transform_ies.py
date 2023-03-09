import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from utils import *
from IPython.display import display

# Criar tabelas t-x e t-y com os rácios da IES a partir do nif e de uma data,
# a data pode ser a data do ppi (fornecedores e consultores para inelegibilidades)
# ou data da candidatura (promotores para inelegibilidades e anulações e consultores para anulações)
#sendo que x e y são o t-1 ou o t-2 (podiam ser também os dois ultimos anos disponiveis)

def get_racios_ies(ies_name, folder, agency):
    if agency == "iapmei":
        ies_singular = "racios_"+ ies_name +"_singulares.xlsx"
        ies_coletivo  ="racios_"+ ies_name +"_coletivos.xlsx"
        df_ies_singular = pd.read_excel(folder + ies_singular,engine='openpyxl')
        df_ies_coletivo = pd.read_excel(folder + ies_coletivo,engine='openpyxl')
        df_ies = pd.concat([df_ies_singular, df_ies_coletivo])
    else:
        ies_aicep = "racios_aicep.xlsx"
        df_ies = pd.read_excel(folder + ies_aicep,engine='openpyxl')
    return df_ies


def get_df_and_keys(nif_type, df, racios_type, transformed_not_derived_merged_path):
    if racios_type == params.anul:
        date_name = "ano_candidatura"
        df = pd.read_excel(transformed_not_derived_merged_path + "N_Proj_candidaturas.xlsx", engine='openpyxl')
        final_keys = ["N_Proj"]
        df["ano"] = df[date_name]
        df = df[final_keys+[nif_type] + ["ano"]]
    else:
        final_keys = ['N_Proj','N_Doc', 'N_Linha']
        if nif_type == "nif_promotor":
            date_name = "ano_candidatura"
            df["ano"] = df[date_name]
        else:
            date_name = "data_ppi"
            df["ano"] = df[date_name].apply(lambda data: data.year).astype(int)
        df = df[final_keys+[nif_type] + ["ano"]]
    return final_keys, df


def criar_racios(nif_type, ies_name, df, table, folder, save_folder, racios_type, agency, transformed_not_derived_merged_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming", nif_type, "ies")
    # last_year_available = "ult"
    # penultimate_year_available = "penult"
    last_year_available = "t-1"
    penultimate_year_available = "t-2"
    ies_nif_key_1 = ies_name + '_nif_' + last_year_available
    ies_year_key_1 = ies_name + '_ano_' + last_year_available
    ies_nif_key_2 = ies_name + '_nif_' + penultimate_year_available
    ies_year_key_2= ies_name + '_ano_' + penultimate_year_available

    df_ies = get_racios_ies(ies_name, folder, agency)
    final_keys, df = get_df_and_keys(nif_type, df, racios_type, transformed_not_derived_merged_path)

    #casts
    df_ies["ano"] = df_ies["ano"].astype(str).str.strip().astype(int)
    try:
        df_ies["nif"] = df_ies["nif"].astype('Int64')
        df[nif_type] = df[nif_type].astype('Int64')
    except Exception:
        df_ies["nif"] = df_ies["nif"].astype(str).str.strip()
        df[nif_type] = df[nif_type].astype(str).str.strip()
    finally:
         df_ies["nif"] = df_ies["nif"].astype(object)
         df[nif_type] = df[nif_type].astype(object)

    df_temp = df.merge(df_ies[["ano","nif"]].rename(columns={"ano": "ano_ies", "nif": "nif_ies"}),
                                left_on=[nif_type], right_on=["nif_ies"], how='left')

    #ir buscar o ultimo e penultimos anos da ies disponiveis relativamente ao ano do ppi/candidatura
    # df_temp_1 = df_temp.loc[(df_temp["ano"] - 1 >= df_temp["ano_ies"])].groupby(final_keys)["ano_ies"].max()\
    #     .reset_index().rename(columns={"ano_ies": last_year_available})
    # df_temp_2 = df_temp.loc[(df_temp["ano"] - 2 >= df_temp["ano_ies"])]\
    #     .groupby(final_keys)["ano_ies"].max().reset_index().rename(columns={"ano_ies": penultimate_year_available})

    #ir buscar o t-1 e t-2 anos da ies disponiveis relativamente ao ano do ppi/candidatura
    df_temp_1 = df_temp.loc[(df_temp["ano"] - 1 == df_temp["ano_ies"])].groupby(final_keys)["ano_ies"].max()\
        .reset_index().rename(columns={"ano_ies": last_year_available})
    df_temp_2 = df_temp.loc[(df_temp["ano"] - 2 == df_temp["ano_ies"])]\
        .groupby(final_keys)["ano_ies"].max().reset_index().rename(columns={"ano_ies": penultimate_year_available})

    df = df.merge(df_temp_1, on=final_keys, how='left')
    df = df.merge(df_temp_2, on=final_keys, how='left')

    #criar duas ies renomeadas
    df_ies_last = df_ies.rename(columns=lambda c: ies_name +'_'+ c +'_' + last_year_available)
    df_ies_penultimate = df_ies.rename(columns=lambda c: ies_name +'_'+ c +'_' + penultimate_year_available)

    # display(df)
    #guardar uma linha por proj, nif para data audit
    df_grouped = df[["N_Proj",nif_type, last_year_available, penultimate_year_available]].groupby(["N_Proj", nif_type]).first().reset_index()
    df_ies_all_last = df_grouped.merge(df_ies_last, left_on=[nif_type, last_year_available], right_on=[ies_nif_key_1, ies_year_key_1], how='left')
    df_ies_all_both = df_ies_all_last.merge(df_ies_penultimate, left_on=[nif_type,penultimate_year_available], right_on=[ies_nif_key_2, ies_year_key_2], how='left')
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving for audit", nif_type, "ies")
    df_ies_all_both.drop(columns=[ies_nif_key_1, ies_nif_key_2, last_year_available, penultimate_year_available])\
        .to_excel(save_folder.replace(".xlsx","") + "_"+ies_name + '.xlsx', index = False, encoding='utf-8-sig')

    #selecionar só as colunas que vão cruzar com as features e cruzar
    ies_cols_to_keep = pd.read_excel(folder + "escolhidos.xlsx",engine='openpyxl')["Coluna"].tolist()
    ies_cols_to_keep_1 = [ies_name +'_'+ c +'_' + last_year_available for c in ies_cols_to_keep]
    ies_cols_to_keep_2 = [ies_name +'_'+ c +'_' + penultimate_year_available for c in ies_cols_to_keep]

    df_merged_last = df.merge(df_ies_last[ies_cols_to_keep_1], left_on=[nif_type,last_year_available], right_on=[ies_nif_key_1, ies_year_key_1], how='left')
    df_merged_both = df_merged_last.merge(df_ies_penultimate[ies_cols_to_keep_2], left_on=[nif_type,penultimate_year_available], right_on=[ies_nif_key_2, ies_year_key_2], how='left')

    df_merged_both = df_merged_both.drop(columns=[nif_type, "ano", ies_nif_key_1, ies_nif_key_2, last_year_available, penultimate_year_available])

    if(table is not None):
        table = table.merge(df_merged_both, on=final_keys, how='left')
    else:
        table = df_merged_both
    return table


def create_missing_IES_variables(table, racios_type):
    table["prom_falta_IES_t-1"] = np.where(pd.isnull(table["prom_ano_t-1"]),1,0)
    table["prom_falta_IES_t-2"] = np.where(pd.isnull(table["prom_ano_t-2"]),1,0)
    table["consul_falta_IES_t-1"] = np.where(pd.isnull(table["consul_ano_t-1"]),1,0)
    table["consul_falta_IES_t-2"] = np.where(pd.isnull(table["consul_ano_t-2"]),1,0)
    if racios_type == "inelegibilidades":
        table["fornec_falta_IES_t-1"] = np.where(pd.isnull(table["fornec_ano_t-1"]),1,0)
        table["fornec_falta_IES_t-2"] = np.where(pd.isnull(table["fornec_ano_t-2"]),1,0)
    return table


def transform_ies(agency, type, df, source_path, transformed_ies_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Starting ies transformation")
    table = None
    if agency == params.iapmei:
        folder = params.iapmei_ies
    else:
        folder = params.aicep_ies

    table = criar_racios("nif_promotor", "prom", df, table, folder, transformed_ies_path, type, agency, source_path)
    table = criar_racios("nif_consultora", "consul", df, table, folder, transformed_ies_path, type, agency, source_path)
    if type == params.inelg:
        table = criar_racios("nif_fornecedor", "fornec", df, table, folder, transformed_ies_path, type, agency, source_path)

    table.replace([np.inf, -np.inf, "inf","-inf", "erro", "*****"], np.nan, inplace=True)

    table = create_missing_IES_variables(table, type)

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving ies")
    table.to_excel(transformed_ies_path, index = False, encoding='utf-8-sig')
    return table
