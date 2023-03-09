import pandas as pd
import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from params import *
from IPython.display import display
import params
import sys
import sklearn.preprocessing as prep
from sklearn.preprocessing import Normalizer
from params import *


#replace nones with 0 and values with 1
def value_to_binary(df, cols):
    for col in cols:
        df.loc[df[col].notnull(), col] = 1
        df[col].fillna(0, inplace=True)


#e.g.: rename cols [x, y, z] to [prefix_x, prefix_y, prefix_z]
def rename_dummies_names(df, curr_names, prefix):
    new_names = [prefix + "_" + str(n) for n in curr_names]
    df.rename(columns=dict(zip(curr_names, new_names)), inplace=True)


#e.g.: transform values from [x, y, z] to [1, 2, 3]
def categoric_to_numeric(df, cols):
    df[cols] = df[cols].astype('category')
    df[cols] = df[cols].apply(lambda x: x.cat.codes)


def fill_na(df, value):
    return df.fillna(value)


def get_dummies(df, cols):
    return pd.get_dummies(df, columns = cols, dummy_na = False)


def valida_nif(numero):
    EXPECTED_DIGITS = 9
    if numero == "<NA>" or pd.isnull(numero) or numero == "nan" or not numero or numero is None:
        return None
    elif not numero.isdigit() or len(numero) != EXPECTED_DIGITS:
        return 0
    else:
        soma = sum([int(dig) * (EXPECTED_DIGITS - pos) for pos, dig in enumerate(numero)])
        resto = soma % 11
        if (numero[-1] == '0' and resto == 1):
            resto = (soma + 10) % 11
    return 1 if resto == 0 else 0


def tipo_nif(numero, test):
    primeiro = str(numero)[0]
    res = "outro"
    if numero == "nan" or pd.isnull(numero) or numero == "<NA>" or not numero or numero is None:
        return None
    elif primeiro in ["1","2"]:
        res = "singular"
    elif primeiro in ["3","4"]:
        res = "estrangeiro"
    elif primeiro == "5":
        res = "coletivo"
    return 1 if res == test else 0


def avaliar_nif(nif, type):
    nif_valido = valida_nif(nif)
    if nif_valido:
        return tipo_nif(nif, type)
    return nif_valido


def classificar_nif(df, nif, name, type):
    df.insert(df.columns.get_loc('nif_'+name+'_valido') + 1,'tipo'+'_'+name+'_'+type, df[nif].apply(lambda nif: avaliar_nif(str(nif).strip(), type)))


def merge_transformed(transformed_not_derived_merged_path, transformed_not_derived_merged_all_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Merging transformed data")

    file = "N_Proj_N_Doc_N_Linha_appis_facies.xlsx"
    df = pd.read_excel(transformed_not_derived_merged_path + file, engine='openpyxl')

    init_shape = df.shape

    files_keys = {
        "N_Proj_N_Doc_ppis.xlsx": [["N_Proj", "Ppi"], ["N_Proj", "N_Doc"]],
        "N_Proj_facis.xlsx":[["N_Proj"], ["N_Proj"]],
        "N_Proj_candidaturas.xlsx": [["N_Proj"], ["N_Proj"]],
    }

    for file, keys in files_keys.items():
        print(dt.datetime.now().strftime("%H:%M:%S"), "Merging with", file)
        df_2 = pd.read_excel(transformed_not_derived_merged_path + file, engine='openpyxl')
        df = df.merge(df_2, how='left', left_on=keys[0], right_on=keys[1])

    # checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')

    df = df.drop(columns=["N_Doc_y"])
    df = df.rename(columns={"N_Doc_x": "N_Doc"})

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving merged data")
    df.to_excel(transformed_not_derived_merged_all_path, index=False, encoding='utf-8-sig')
    return df


def generate_null_stats(df, path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Generating nulls stats")
    orig_stdout = sys.stdout
    f = open(path, 'w')
    sys.stdout = f
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        display(df.isna().sum())
    f.close()
    sys.stdout = orig_stdout


def impute_known_values(agency, type, df, imputed_known_values_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Imputing known values")
    cols_tip_dom = []
    for c in df.columns:
        prefix = c.split("_")[0]
        if prefix == "tipologia" or prefix == "dominio":
            cols_tip_dom.append(c)
    cols= ["Doc_Valor", "Desp_Valor", "Desp_Inv", "Desp_Eleg", "Quit_Eleg", "Pg_Valor", "Pg_Valor_Doc",
           "apresenta_socios", "vendas_merc_ext_sup_0_pre_proj", "consultora_em_quantos_projs"]
    cols = cols + cols_tip_dom
    df[cols] = fill_na(df[cols],0)
    df["Classe"] = df["Classe"].astype('Int64')
    dummies_list = ["Cert_Sn", "Classe", "dimensao_emp", "Loc_Investimento", "Loc_Sede", "tipo_CAE"]
    for c in dummies_list:
        df[c] = df[c].astype(str)
    df[dummies_list] = fill_na(df[dummies_list], "nulo")
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving imputed known values")
    df.to_excel(imputed_known_values_path, index=False, encoding='utf-8-sig')
    return df


def merge_with_ies(type, df, ies, transformed_ies_path, merged_with_ies):
    if ies is None:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Reading ies")
        ies = pd.read_excel(transformed_ies_path, engine='openpyxl')
    print(dt.datetime.now().strftime("%H:%M:%S"), "Merging data with ies")
    init_shape = df.shape
    if type == params.anul:
        keys = ['N_Proj']
    else:
        keys = ['N_Proj', 'N_Doc', 'N_Linha']
    df = df.merge(ies, on=keys, how='left')
    # checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving merged data with ies")
    df.to_excel(merged_with_ies, index=False, encoding='utf-8-sig')
    return df


def create_dummies(agency, type, df, save_path):
    if type == params.inelg:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Creating dummies")
        create_list = ["Cert_Sn",  "Classe",  "dimensao_emp",  "Loc_Investimento",  "Loc_Sede",  "tipo_CAE"]
        df["temp"] = df["dimensao_emp"]
        df = get_dummies(df, create_list)
        df = df.rename(columns={'temp': 'dimensao_emp'})
        print(dt.datetime.now().strftime("%H:%M:%S"), "Saving dummied dataset")
        writer = pd.ExcelWriter(save_path, engine='xlsxwriter')
        df.to_excel(writer, index = False, header=True, encoding='utf-8-sig')
        writer.book.use_zip64()
        writer.save()
    return df


def normalize(agency, type, df, save_path):
    if type == params.inelg:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Normalizing dataset")

        normalize_list = ["Doc_Valor",  "Desp_Valor",  "Desp_Inv",  "Desp_Eleg",
                          "Quit_Eleg",  "Pg_Valor",
                          "Pg_Valor_Doc",  "total_desp_eleg_proj",  "total_desp_eleg_ppi",
                          "total_desp_eleg_fornc_proj",  "total_desp_eleg_fornc_ppi",  "total_desp_eleg_fornc",
                          "investimento_elegivel_aprovado"]

        cols_ies = ["_volume_negocios_", "_ativo_total_", "_emprestimos_obtidos_passivo_cor_",
                    "_emprestimos_obtidos_passivo_ncor_", "_vab/trabalhador_"]
        for c in df.columns:
            for i in cols_ies:
                if i in c:
                    normalize_list.append(c)
        normalize_list = [c for c in normalize_list if c in df.columns]
        for c in normalize_list:
            df[c] = df[c].astype("float64")
        df[normalize_list] = prep.normalize(df[normalize_list])
        print(dt.datetime.now().strftime("%H:%M:%S"), "Saving normalized dataset")
        writer = pd.ExcelWriter(save_path, engine='xlsxwriter')
        df.to_excel(writer, index = False, header=True, encoding='utf-8-sig')
        writer.book.use_zip64()
        writer.save()
    return df


def merge_with_text_mining(agency, type, df, save_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Reading text mining data")
    if agency == params.iapmei:
        if type == params.inelg:
            path = params.iapmei_text_mining_ineleg_features
        else:
            path = params.iapmei_text_mining_anul_features
    else:
        if type == params.inelg:
            path = params.aicep_text_mining_ineleg_features
        else:
            path = params.aicep_text_mining_anul_features
    df_tm = pd.read_csv(path)
    print(dt.datetime.now().strftime("%H:%M:%S"), "Merging data with text mining")
    init_shape = df.shape
    keys = ['N_Proj']
    df = df.merge(df_tm, on=keys, how='left')
    # checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving merged data with text mining")
    df.to_excel(save_path, index=False, encoding='utf-8-sig')
    return df

