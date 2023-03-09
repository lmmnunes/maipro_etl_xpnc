import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from utils import *
from pandas import Timestamp


def check_date_format(date):
    has_date_format = True
    cleaned_date = date
    if not isinstance(cleaned_date, (Timestamp, datetime.date)):
        try:
            cleaned_date = datetime.datetime.strptime(date.strip(), '%Y-%m-%d')
        except:
            try:
                cleaned_date = datetime.datetime.strptime(date.strip(), '%d-%m-%Y')
            except:
                try:
                    cleaned_date = datetime.datetime.strptime(date.strip(), '%Y-%m-%d  %H:%M:%S')
                except:
                    try:
                        cleaned_date = datetime.datetime.strptime(date.strip(), '%d-%m-%Y  %H:%M:%S')
                    except:
                        has_date_format = False
                        print("escolheu-se a data mais recente destas datas:", date)
                        # raise Exception("conversão para timestamp mal sucedida: ", date)
    if not isinstance(cleaned_date, (Timestamp, datetime.date)):
        has_date_format = False
    return [has_date_format, cleaned_date]


def clean_date(date):
    cleaned_date = date
    if pd.notnull(date):
        has_date_format, cleaned_date = check_date_format(date)
        if not has_date_format:
            # print("tipo: ", type(date))
            # print("entre estas: ", date)
            cleaned_dates = date.split("e")
            new_cleaned_dates = []
            for ts in cleaned_dates:
                has_date_format_i, cleaned_date_i = check_date_format(ts)
                if not has_date_format_i:
                    raise Exception("erro: ", cleaned_date_i)
                new_cleaned_dates.append(cleaned_date_i)
            new_cleaned_dates.sort()
            cleaned_date = new_cleaned_dates[-1]
            # print("escolheu-se: ", cleaned_date)
    return cleaned_date


def correct_dates(agency, df):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Correcting dates")
    init_shape = df.shape
    # Pela análise dos dados, conseguiu-se fazer estas correções com base noutras datas da rubrica
    if agency == "iapmei":
        df["Poc_Data"] = df["Poc_Data"].replace(["10-04-215"], "10-04-2015")
        df["Poc_Data"] = df["Poc_Data"].replace(["/"], "-", regex=True)
        df["Quit_Data"] = df["Quit_Data"].replace(["04-04-207"], "04-04-2007")
        df["Quit_Data"] = df["Quit_Data"].replace(["201901-28"], "2019-01-28")
        df["Quit_Data"] = df["Quit_Data"].replace(["/"], "-", regex=True)

        # 2010-01-14
        df["data_fatura"] = np.where( \
            (df["N_Proj"] == 43029) & \
            (df["N_Doc"] == 2) & \
            (df["N_Linha"] == 53), "2020-01-14", df["data_fatura"])
    else:
        # 21-011-2016
        df["data_pagamento"] = np.where( \
            (df["N_Proj"] == 21659) & \
            (df["N_Doc"] == 2) & \
            (df["N_Linha"] == 47), "21-11-2016", df["data_pagamento"])

        # 1985-05-23
        df["data_pagamento"] = np.where( \
            (df["N_Proj"] == 9398) & \
            (df["N_Doc"] == 4) & \
            ((df["N_Linha"] == 51) | (df["N_Linha"] == 52) | (df["N_Linha"] == 53)), "2016-05-23", df["data_pagamento"])

        # 2106-03-23
        df["data_pagamento"] = np.where( \
            (df["N_Proj"] == 2594) & \
            (df["N_Doc"] == 7) & \
            (df["N_Linha"] == 77), "2016-03-23", df["data_pagamento"])

        # 2106-02-02
        df["data_pagamento"] = np.where( \
            (df["N_Proj"] == 7212) & \
            (df["N_Doc"] == 4) & \
            (df["N_Linha"] == 84), "2016-02-02", df["data_pagamento"])

        # 2026-10-24
        df["data_pagamento"] = np.where( \
            (df["N_Proj"] == 1051) & \
            (df["N_Doc"] == 9) & \
            (df["N_Linha"] == 49), "2016-10-24", df["data_pagamento"])

        # 0024-06-07
        df["data_fatura"] = np.where( \
            (df["N_Proj"] == 1633) & \
            (df["N_Doc"] == 3) & \
            (df["N_Linha"] == 2), "2016-06-07", df["data_fatura"])

        # 1913-09-08
        df["data_fatura"] = np.where( \
            (df["N_Proj"] == 20268) & \
            (df["N_Doc"] == 3) & \
            (df["N_Linha"] == 16), "2017-03-31", df["data_fatura"])

        # 2008-02-18
        df["data_fatura"] = np.where( \
            (df["N_Proj"] == 24504) & \
            (df["N_Doc"] == 4) & \
            (df["N_Linha"] == 133), "2018-02-18", df["data_fatura"])

        # 2011-11-09
        df["data_fatura"] = np.where( \
            (df["N_Proj"] == 2213) & \
            (df["N_Doc"] == 3) & \
            (df["N_Linha"] == 147), "2016-11-09", df["data_fatura"])

    df["data_fatura"] = np.where((pd.isnull(df["data_fatura"])), df["Poc_Data"], df["data_fatura"])
    df["data_fatura"] = np.where((pd.isnull(df["data_fatura"])), df["data_pagamento"], df["data_fatura"])

    df["data_pagamento"] = np.where((pd.isnull(df["data_pagamento"])), df["Quit_Data"], df["data_pagamento"])
    df["data_pagamento"] = np.where((pd.isnull(df["data_pagamento"])), df["Poc_Data"], df["data_pagamento"])
    df["data_pagamento"] = np.where((pd.isnull(df["data_pagamento"])), df["data_fatura"], df["data_pagamento"])

    dates_list = ["data_pagamento", "data_fatura", "Poc_Data", "Quit_Data", "data_ppi"]

    for date in dates_list:
        df[date] = df[date].apply(lambda d: clean_date(d))

    df["ano_pagamento"] = df["data_pagamento"].apply(lambda date: date.year)
    df["ano_fatura"] = df["data_fatura"].apply(lambda date: date.year)

    cond_base = ((df["data_fatura"] > df["data_pagamento"]) & (df["ano_pagamento"] < df["ano_candidatura"]))
    cond_1 = ((df["ano_candidatura"] < 2020) & (df["ano_pagamento"] + 10 < 2020) & \
              (df["ano_pagamento"] + 10 >= df["ano_candidatura"]))
    cond_2 = (df["ano_candidatura"] == 2020)
    cond_3 = (df["ano_fatura"] >= df["ano_candidatura"])

    def dp_mais_10(date):
        try:
            if date.month == 2 and date.day == 29:
                date = date.replace(day=28)
            return date.replace(year=date.year + 10)
        except:
            raise Exception("dp_mais_10 ", date)

    def dp_cand(row):
        try:
            if row["data_pagamento"].month == 2 and row["data_pagamento"].day == 29:
                row["data_pagamento"] = row["data_pagamento"].replace(day=28)
            return row["data_pagamento"].replace(year=row["ano_candidatura"])
        except:
            raise Exception("dp_cand ", row["data_pagamento"])

    def dp_fat(row):
        try:
            if row["data_pagamento"].month == 2 and row["data_pagamento"].day == 29:
                row["data_pagamento"] = row["data_pagamento"].replace(day=28)
            return row["data_pagamento"].replace(year=row["ano_fatura"])
        except:
            raise Exception("dp_fat ", row["data_pagamento"])

    df["dp_+10"] = df["data_pagamento"].apply(lambda date: dp_mais_10(date))

    df["dp_cand"] = df[["data_pagamento", "ano_candidatura"]].apply(lambda row: dp_cand(row), axis=1)

    df["dp_fat"] = df[["data_pagamento", "ano_fatura"]].apply(lambda row: dp_fat(row), axis=1)

    df["data_pagamento"] = np.where(cond_base & cond_1, df["dp_+10"], df["data_pagamento"])
    df["data_pagamento"] = np.where(cond_base & ~cond_1 & cond_2, df["dp_cand"], df["data_pagamento"])
    df["data_pagamento"] = np.where(cond_base & ~cond_1 & ~cond_2 & cond_3, df["dp_fat"], df["data_pagamento"])
    df["data_pagamento"] = np.where(cond_base & ~cond_1 & ~cond_2 & ~cond_3, df["dp_cand"], df["data_pagamento"])

    df["data_ppi"] = np.where(pd.isnull(df["data_ppi"]), df["data_fatura"], df["data_ppi"])

    df = df.drop(columns=["ano_pagamento", "ano_fatura", "dp_+10", "dp_cand", "dp_fat"])

    # checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')

    return df

def correct_socios(df):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Correcting sócios")
    df["socios"] = np.where((pd.isnull(df["socios"]) | (df["socios"] == 0)) &
                            ((df["Promotor/Nat_Jur"] == 55) |
                            (df["Promotor/Nat_Jur"] == 10) |
                            (df["Promotor/Nat_Jur"] == 15)), 1, df["socios"])
    df = df.drop(columns=["Promotor/Nat_Jur"])
    return df


def correct_nif_consultora(agency, df):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Correcting nif_consultora")
    init_shape = df.shape
    # Imputar nifs de consultores que estão a nulo com nifs de fornecedores que não estão a nulo em rubricas que temos grande certeza de serem consultores:
    # campo Poc_Conta (faturas appi e facie) a 442, 661, 622 ou 626
    # Atualizar todas as variáveis relativas a consultores.
    # Criar variável consultor_imputado que significa que o nif de consultora foi imputado
    # IAPMEI:
    # campo codigo_classe_investimento a 220, 331, 332, 334 ou 999
    # (fonte IES) nifs de fornecedores que tenham CAE pertencente a ["74900","69200","82990","70220","73200","70100","82190","70210","82110"]
    # AICEP:
    # campo codigo_classe_investimento a 352 ou 312
    # (fonte IES) nifs de fornecedores que tenham CAE pertencente a ["74900","69200","82990","70220","73200","70100","82190","70210","82110"]
    df["Poc_Conta"] = df["Poc_Conta"].astype(str)
    if agency == params.iapmei:
        df_cruz = df[pd.isnull(df["nif_consultora"]) &
                     pd.notnull(df["nif_fornecedor"]) &
                     ((df["Classe"] == 220) | (df["Classe"] == 331) | (df["Classe"] == 332) |
                      (df["Classe"] == 334) | (df["Classe"] == 999)) &
                     ((df["Poc_Conta"] == "442") | (df["Poc_Conta"] == "661") | (df["Poc_Conta"] == "622") |
                      (df["Poc_Conta"] == "626"))]
        file = params.iapmei_ies + "\\fornecedores consultores iapmei.xlsx"
        df_fornc_cons_all = pd.read_excel(file, engine='openpyxl')[["NIF_Fornecedor"]]
        df_fornc_cons_all = df_fornc_cons_all.rename(columns={"NIF_Fornecedor": "nif_fornecedor"})
    else:
        df_cruz = df[pd.isnull(df["nif_consultora"]) &
                     pd.notnull(df["nif_fornecedor"]) &
                     ((df["Classe"] == 352) | (df["Classe"] == 312)) &
                     ((df["Poc_Conta"] == "442") | (df["Poc_Conta"] == "661") | (df["Poc_Conta"] == "622") |
                     (df["Poc_Conta"] == "626"))]
        file = params.aicep_ies + "\\racios_aicep.xlsx"
        df_fornc_cons_all = pd.read_excel(file, engine='openpyxl')[["nif", "CAE"]]
        df_fornc_cons_all = df_fornc_cons_all.drop_duplicates()
        df_fornc_cons_all = df_fornc_cons_all.rename(columns={"nif": "nif_fornecedor"})
        df_fornc_cons_all = df_fornc_cons_all.replace([np.inf, -np.inf, "erro", "*****"], np.nan)
        df_fornc_cons_all["CAE"] = df_fornc_cons_all["CAE"].astype('Int64')
        cae_poss_consultores = [74900, 69200, 82990, 70220, 73200, 70100, 82190, 70210, 82110]
        df_fornc_cons_all = df_fornc_cons_all[df_fornc_cons_all["CAE"].isin(cae_poss_consultores)][
            ["nif_fornecedor"]].drop_duplicates()

    # print("df_cruz:", df_cruz.shape, "\ndf_fornc_cons_all", df_fornc_cons_all.shape)

    if df_cruz.size > 0 and df_fornc_cons_all.size > 0:
        # print("tamanhos > 0")
        df_cruz["nif_fornecedor"] = df_cruz["nif_fornecedor"].astype(str)
        df_fornc_cons_all["nif_fornecedor"] = df_fornc_cons_all["nif_fornecedor"].astype(str)
        df_fornc_cons = df_cruz[["N_Proj", "N_Doc", "N_Linha", "nif_fornecedor"]].merge(df_fornc_cons_all, \
                                                                                         on=["nif_fornecedor"],
                                                                                         how='inner')
        # print("df_fornc_cons:", df_fornc_cons.shape)
        df_fornc_cons["consultor_imputado"] = 1

        df = df.merge(df_fornc_cons[["N_Proj", "N_Doc", "N_Linha", "consultor_imputado"]], \
                      on=["N_Proj", "N_Doc", "N_Linha"], how='left')

        df["consultor_imputado"] = df["consultor_imputado"].fillna(0)
        df["nif_consultora"] = np.where((df["consultor_imputado"] == 1), \
                                        df["nif_fornecedor"], df["nif_consultora"])
        df["nif_consul_valido"] = np.where((df["consultor_imputado"] == 1), \
                                           df["nif_fornec_valido"], df["nif_consul_valido"])
        df["tipo_consul_outro"] = np.where((df["consultor_imputado"] == 1), \
                                           df["tipo_fornec_outro"], df["tipo_consul_outro"])
        df["tipo_consul_coletivo"] = np.where((df["consultor_imputado"] == 1), \
                                              df["tipo_fornec_coletivo"], df["tipo_consul_coletivo"])
        df["tipo_consul_estrangeiro"] = np.where((df["consultor_imputado"] == 1), \
                                                 df["tipo_fornec_estrangeiro"], df["tipo_consul_estrangeiro"])
        df["tipo_consul_singular"] = np.where((df["consultor_imputado"] == 1), \
                                              df["tipo_fornec_singular"], df["tipo_consul_singular"])
    else:
        df["consultor_imputado"] = 0

        # checks
        final_shape = df.shape
        if init_shape[0] != final_shape[0]:
            raise Exception('Number of initial rows and after transformation rows is not the same!')
    return df


def correct_spaces_commas(df, cols):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Correcting spaces and commas")
    def correct_spaces_commas(x):
        x = str(x)
        x = x.replace(" ", "")
        x = x.replace(",", ".")
        if x == "224.08.":
            x = "224.08"
        x = float(x)
        return x
    for c in cols:
        df[c] = df[c].apply(lambda x: correct_spaces_commas(x))
    return df


def correct(agency, type, df, cleaned_all_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Starting corrections")
    df = correct_socios(df)
    df = correct_dates(agency, df)
    df = correct_nif_consultora(agency, df)
    df = correct_spaces_commas(df, ["Pg_Valor_Doc"])
    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving corrections")
    df.to_excel(cleaned_all_path, index=False, encoding='utf-8-sig')
    return df
