import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from utils import *


def aux_appi_facie_concat(file_appi, file_facie, select_list_appi, select_list_facie, print_text,
                          ppi_folder, appi_folder, facie_folder, agency, type, test_new,
                          selected_cols_path, cleaned_path, transformed_not_derived_path):

    if test_new:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming PPIS " + print_text)
    else:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming APPIS and FACIES " + print_text)

    # select and save appi
    full_df = pd.read_csv(appi_folder + file_appi)
    df_appi = full_df[select_list_appi]
    try:
        df_appi.to_excel(selected_cols_path + file_appi.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    except Exception:
        df_appi.to_csv(selected_cols_path + file_appi, index=False, encoding='utf-8-sig')

    # select and save facie and concat with appi
    if agency == params.iapmei:

        full_df = pd.read_csv(facie_folder + file_facie)
        df_facie = full_df[select_list_facie]
        df_facie.to_excel(selected_cols_path + file_facie.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')

        # selecionar projs das appis que não estão nas facies para não haver duplicados
        list_projs_in_facie = df_facie["N_Proj"].unique().tolist()

        df_appi = df_appi[~(df_appi["N_Proj"].isin(list_projs_in_facie))]

        df = pd.concat([df_appi, df_facie])
    else:
        # aicep tem problemas com a numeração dos documentos
        df = df_appi
        df['N_Doc_a'] = df['N_Doc'].apply(lambda x: x.split("_")[0])
        df['N_Doc_b'] = df['N_Doc'].apply(lambda x: x.split("_")[1] if len(x.split("_")) > 1 else 0)
        df['max_N_Doc_b'] = df.groupby(['N_Proj','N_Doc_a'])['N_Doc_b'].transform(max)
        df = df[(df['N_Doc_b'] == df['max_N_Doc_b'])]
        df = df.drop(columns=["N_Doc_b", "max_N_Doc_b"])
        df['max_N_Doc_a'] = df.groupby(['N_Proj'])['N_Doc_a'].transform(max)
        df = df[(df['N_Doc_a'] == df['max_N_Doc_a'])]
        df["N_Doc"] = df['N_Doc_a']
        df = df.drop(columns=["max_N_Doc_a", 'N_Doc_a'])
    if agency == params.iapmei:
        df['max_N_Doc'] = df.groupby(['N_Proj'])['N_Doc'].transform(max)
        df = df[(df['N_Doc'] == df['max_N_Doc'])]
        df = df.drop(columns=["max_N_Doc"])

    return df.drop_duplicates()


def transform_geral(ppi_folder, appi_folder, facie_folder, agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path):

    df = aux_appi_facie_concat("Geral_appi.csv", "Geral_facie.csv", ["N_Proj", 'N_Doc', "Contexto/Elegivel"],
                          ["N_Proj", 'N_Doc', "Contexto/Elegivel"], "geral", ppi_folder, appi_folder,
                          facie_folder,agency, type, test_new, selected_cols_path, cleaned_path,
                          transformed_not_derived_path)

    df = df.rename(columns={"Contexto/Elegivel": "investimento_elegivel_aprovado"})

    if df[["N_Proj"]].duplicated().any():
        raise Exception('Found duplicates!')

    df.to_excel(transformed_not_derived_path + "ppi_appi_facie_geral.xlsx", index=False, encoding='utf-8-sig')
    return df


def transform_inv(ppi_folder, appi_folder, facie_folder, agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path):

    df = aux_appi_facie_concat("Inv_appi.csv", "Inv_facie.csv", ["N_Proj", "N_Doc", "N_Ordem", "Classe"],
                          ["N_Proj", "N_Doc", "N_Ordem", "Classe"], "investimentos", ppi_folder, appi_folder,
                          facie_folder, agency, type, test_new, selected_cols_path, cleaned_path,
                          transformed_not_derived_path)

    if agency == params.iapmei:

        def aux(c):
            if c in [411,412]:
                c = "Construção / Remodelação de Edifícios"
            elif c in [331,335,334,332,320,220,333]:
                c = "Estudos, Diagnósticos, Licenças e Serviços de Engenharia"
            elif c in [110]:
                c = "Máquinas e Equipamentos"
            elif c in [230,120]:
                c = "Software e Equipamentos Informáticos"
            else:
                c = "Outras Despesas"
            return c

        df["classe_grupo"] = df["Classe"].apply(lambda c: aux(c))

    else:

        def aux(c):
            if c in [330]:
                c = "Campanhas"
            elif c in [130]:
                c = "Contratacao de Técnicos"
            elif c in [352]:
                c = "Estudos e Diagnósticos"
            elif c in [211, 212, 221, 222, 231, 232]:
                c = "Feiras e Exposicoes"
            elif c in [381, 382, 383]:
                c = "Presença na Web"
            elif c in [311, 312, 313, 321, 322, 323, 324]:
                c = "Prospeção"
            else:
                c = "Outras Despesas"
            return c

        df["classe_grupo"] = df["Classe"].apply(lambda c: aux(c))

    if df[["N_Proj", "N_Doc", "N_Ordem"]].duplicated().any():
        raise Exception('Found duplicates!')

    df.to_excel(transformed_not_derived_path + "ppi_appi_facie_inv.xlsx", index=False, encoding='utf-8-sig')
    return df


def transform_faturas(ppi_folder, appi_folder, facie_folder, agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path):

    select_list_appi = ["N_Proj", "N_Doc", "N_Linha","C_An", "Ppi", "N_Ordem", "Tipo", "Doc_Data", "Doc_Pais",
                        "Doc_Nif",  "Doc_Valor", "Desp_Valor", "Desp_Inv", "Desp_Eleg", "Poc_Data",
                        "Quit_Data", "Quit_Eleg", "Pg_Data", "Pg_Valor", "Pg_Valor_Doc",
                        "Cert_Sn", "Amostra", "An_Eleg_E", "An_Eleg_V", "An_Vd_V", "Poc_Conta"]

    select_list_facie = select_list_appi

    df = aux_appi_facie_concat("Facturas_appi.csv", "Facturas_facie.csv", select_list_appi,
                               select_list_facie, "faturas", ppi_folder, appi_folder,
                               facie_folder, agency, type, test_new, selected_cols_path, cleaned_path,
                               transformed_not_derived_path)

    df = df[(df["C_An"] != "X") & ((df["Tipo"] == "FT") | (df["Tipo"] == "VE") | (df["Tipo"] == "CL"))]

    init_shape = df.shape

    #amostra
    df.rename(columns={"An_Vd_V":"tipo_amostra"}, inplace=True)
    df.insert(df.columns.get_loc("tipo_amostra") + 1,"amostra", np.where(
        (pd.notnull(df["tipo_amostra"]) | pd.notnull(df["Amostra"])), 1, 0))
    df.insert(df.columns.get_loc("tipo_amostra") + 2,"amostra_aleatoria", np.where(df["tipo_amostra"] == 'A', 1, 0))
    df.insert(df.columns.get_loc("tipo_amostra") + 3,"amostra_outra_razao", np.where(
        ((pd.notnull(df["tipo_amostra"]) & (df["tipo_amostra"] != 'A')) |
         (pd.isnull(df["tipo_amostra"]) & pd.notnull(df["Amostra"]))), 1, 0))

    #nif fornecedor
    def classificar_pais(pais):
        if pd.isnull(pais) or pais == "nan" or not pais or pais == "<NA>" or pais is None:
            return None
        elif pais == 'PT':
            return 1
        return 0

    df.rename(columns={"Doc_Nif": "nif_fornecedor"}, inplace=True)
    df["nif_fornecedor"] = fill_na(df["nif_fornecedor"], 0)
    df.insert(df.columns.get_loc("Doc_Pais") + 1, "fornec_pt",
              df["Doc_Pais"].apply(lambda pais: classificar_pais(pais)))
    df.insert(df.columns.get_loc("nif_fornecedor") + 1, 'nif_fornec_valido',
              df["nif_fornecedor"].apply(lambda nif: valida_nif(str(nif).strip())))

    def avaliar_nif_estrangeiro(fornec_pt):
        if fornec_pt != 0 and (fornec_pt == "nan" or pd.isnull(
                fornec_pt) or fornec_pt == "<NA>" or not fornec_pt or fornec_pt is None):
            return None
        return int(not fornec_pt)

    def classificar_nif_fornec(nif, name, type):
        if type == "estrangeiro":
            df.insert(df.columns.get_loc('nif_' + name + '_valido') + 1, 'tipo' + '_' + name + '_' + type,
                      df["fornec_pt"].apply(lambda fornec_pt: avaliar_nif_estrangeiro(fornec_pt)))
        else:
            df.insert(df.columns.get_loc('nif_' + name + '_valido') + 1, 'tipo' + '_' + name + '_' + type,
                      df[nif].apply(lambda nif: avaliar_nif(str(nif).strip(), type)))

    nif_types = ["singular", "estrangeiro", "coletivo", "outro"]

    for type in nif_types:
        classificar_nif_fornec("nif_fornecedor", "fornec", type)

    df["nif_fornec_valido"] = np.where((df["tipo_fornec_estrangeiro"] == 1), 1, df["nif_fornec_valido"])
    df["tipo_fornec_singular"] = np.where(((pd.isnull(df["tipo_fornec_singular"])) &
                                        (df["tipo_fornec_estrangeiro"] == 1)), 0, df["tipo_fornec_singular"])
    df["tipo_fornec_coletivo"] = np.where(((pd.isnull(df["tipo_fornec_coletivo"])) &
                                        (df["tipo_fornec_estrangeiro"] == 1)), 0, df["tipo_fornec_coletivo"])
    df["tipo_fornec_outro"] = np.where(((pd.isnull(df["tipo_fornec_outro"])) &
                                        (df["tipo_fornec_estrangeiro"] == 1)), 0, df["tipo_fornec_outro"])


    df["apresenta_fornecedor"] = np.where(pd.notnull(df["nif_fornecedor"]), 1, 0)

    # pares_fornc_dif_projs
    df1 = df[["N_Proj", "nif_fornecedor"]].drop_duplicates()
    df_merged = df1.merge(df1.copy(), how="left", on=["N_Proj"])
    df_merged = df_merged[df_merged["nif_fornecedor_x"] != df_merged["nif_fornecedor_y"]]

    df_merged_grouped = df_merged.groupby(["nif_fornecedor_x", "nif_fornecedor_y"])["N_Proj"].count() \
        .reset_index(name="n_pares_fornc_dif_projs")

    df_merged_grouped["pares_fornc_dif_projs"] = np.where((df_merged_grouped["n_pares_fornc_dif_projs"] > 1), 1, 0)

    df_temp = df[["N_Proj","nif_fornecedor"]].drop_duplicates()\
        .merge(df_merged_grouped, left_on=["nif_fornecedor"], right_on=["nif_fornecedor_x"], how='left')
    df_temp = df_temp[["N_Proj","nif_fornecedor","pares_fornc_dif_projs"]]\
        .drop_duplicates(subset=["N_Proj","nif_fornecedor"])

    df = df.merge(df_temp, on=["N_Proj","nif_fornecedor"], how='left')
    df["pares_fornc_dif_projs"] = np.where(
        (pd.isnull(df["pares_fornc_dif_projs"]) &
        pd.notnull(df["nif_fornecedor"])), 0, df["pares_fornc_dif_projs"])

    # num_rubricas_fornec num_rubricas_fornec_proj num_rubricas_fornec_ppi
    # projs_fornc_fornece fornc_so_fornece_este_proj
    temp_df = df.groupby(["nif_fornecedor"])["nif_fornecedor"].count().reset_index(name="num_rubricas_fornec")
    df = df.merge(temp_df, on=["nif_fornecedor"], how='left')

    temp_df = df.groupby(["N_Proj", "nif_fornecedor"])["N_Proj"].count().reset_index(name="num_rubricas_fornec_proj")
    df = df.merge(temp_df, on=["N_Proj", "nif_fornecedor"], how='left')

    temp_df = df.groupby(["N_Proj", "nif_fornecedor"])["N_Proj"].count().reset_index(name="num_rubricas_fornec_proj") \
        .groupby(["nif_fornecedor"])["nif_fornecedor"].count().reset_index(name="projs_fornc_fornece")
    df = df.merge(temp_df, on=["nif_fornecedor"], how='left')

    df["fornc_so_fornece_este_proj"] = np.where((df["projs_fornc_fornece"] == 1), 1, 0)

    temp_df = df.groupby(["N_Proj", "Ppi", "nif_fornecedor"])["Ppi"].count().reset_index(name="num_rubricas_fornec_ppi")
    df = df.merge(temp_df, on=["N_Proj", "Ppi", "nif_fornecedor"], how='left')

    # datas
    df = df.rename(columns={"Pg_Data": "data_pagamento", "Doc_Data": "data_fatura"})

    # despesa
    temp_df = df.groupby(["N_Proj"])["Desp_Eleg"].sum().reset_index(name="total_desp_eleg_proj")
    df = df.merge(temp_df, on=["N_Proj"], how='left')

    temp_df = df.groupby(["N_Proj", "Ppi"])["Desp_Eleg"].sum().reset_index(name="total_desp_eleg_ppi")
    df = df.merge(temp_df, on=["N_Proj", "Ppi"], how='left')

    temp_df = df.groupby(["N_Proj", "nif_fornecedor"])["Desp_Eleg"].sum().reset_index(name="total_desp_eleg_fornc_proj")
    df = df.merge(temp_df, on=["N_Proj", "nif_fornecedor"], how='left')

    temp_df = df.groupby(["N_Proj", "Ppi", "nif_fornecedor"])["Desp_Eleg"].sum().reset_index(name="total_desp_eleg_fornc_ppi")
    df = df.merge(temp_df, on=["N_Proj", "Ppi", "nif_fornecedor"], how='left')

    temp_df = df.groupby(["nif_fornecedor"])["Desp_Eleg"].sum().reset_index(name="total_desp_eleg_fornc")
    df = df.merge(temp_df, on=["nif_fornecedor"], how='left')

    df["contrib_fornc_desp_proj"] = df["total_desp_eleg_fornc_proj"] / df["total_desp_eleg_proj"]
    df["contrib_fornc_desp_ppi"] = df["total_desp_eleg_fornc_ppi"] / df["total_desp_eleg_ppi"]
    df["contrib_rubrica_ppi"] = df["Desp_Eleg"] / df["total_desp_eleg_ppi"]

    # em encerramento
    if agency == params.iapmei:
        projs_in_facie = pd.read_csv(facie_folder + "Geral_facie.csv")[["N_Proj"]].drop_duplicates()
    else:
        projs_in_facie = pd.read_excel(params.aicep_em_encerramento, engine='openpyxl')[["N_Proj"]].drop_duplicates()
    projs_in_facie = projs_in_facie.rename(columns={"N_Proj": "projs_enc"})
    df = df.merge(projs_in_facie, how='left', left_on=["N_Proj"], right_on=["projs_enc"])
    df["em_encerramento"] = np.where(pd.notnull(df["projs_enc"]), 1, 0)

    #drops
    to_drop = ["projs_enc", "C_An", "Tipo", "Doc_Pais", "Amostra"]
    df = df.drop(columns=to_drop)

    # checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')

    df.to_excel(transformed_not_derived_path + "ppi_appi_facie_faturas.xlsx", index=False, encoding='utf-8-sig')
    return df


#se for um teste com novos dados, os dados vêm dos ppis, mas o código será parecido,
#por isso fica no mesmo ficheiro para reduzir a probabilidade de erros
def transform_ppi_appi_facie(agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path,
                             transformed_not_derived_merged_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming PPIS, APPIS and FACIES")
    if agency == params.iapmei:
        ppi_folder = params.iapmei_ppi
        appi_folder = params.iapmei_appi
        facie_folder = params.iapmei_facie
    else:
        ppi_folder = params.aicep_ppi
        appi_folder = params.aicep_appi
        facie_folder = params.aicep_facie

    dfs_N_Proj_N_Doc = []
    dfs_N_Proj_N_Doc_N_Ordem = []

    dfs_N_Proj_N_Doc.append(transform_faturas(ppi_folder, appi_folder, facie_folder, agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs_N_Proj_N_Doc.append(transform_geral(ppi_folder, appi_folder, facie_folder, agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs_N_Proj_N_Doc_N_Ordem.append(transform_inv(ppi_folder, appi_folder, facie_folder, agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path))

    previous_df = dfs_N_Proj_N_Doc[0]
    if len(dfs_N_Proj_N_Doc) > 1:
        if test_new:
            print(dt.datetime.now().strftime("%H:%M:%S"), "Merging PPIS")
        else:
            print(dt.datetime.now().strftime("%H:%M:%S"), "Merging APPIS and FACIES")

        for df in dfs_N_Proj_N_Doc[1:]:
            previous_df = previous_df.merge(df, on=['N_Proj', "N_Doc"], how="left")

        if previous_df[['N_Proj', "N_Doc", "N_Linha"]].duplicated().any():
            raise Exception('Found duplicates in merge!')

    if len(dfs_N_Proj_N_Doc_N_Ordem) > 0:
        for df in dfs_N_Proj_N_Doc_N_Ordem:
            previous_df = previous_df.merge(df, on=['N_Proj', "N_Doc", "N_Ordem"], how="left")
        if previous_df[['N_Proj', "N_Doc", "N_Linha"]].duplicated().any():
            raise Exception('Found duplicates in merge!')

    def aux_name(name):
        if test_new:
            print(dt.datetime.now().strftime("%H:%M:%S"), "Saving all PPIS")
            name = name + "_ppis_test_new" + ".xlsx"
        else:
            print(dt.datetime.now().strftime("%H:%M:%S"), "Saving all APPIS and FACIES")
            name = name + "_appis_facies" + ".xlsx"
        return name

    previous_df.to_excel(transformed_not_derived_merged_path + aux_name("N_Proj_N_Doc_N_Linha"), index=False, encoding='utf-8-sig')