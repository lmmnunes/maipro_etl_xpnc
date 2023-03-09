import datetime as dt
import pandas as pd
import numpy as np
import datetime
import params
from utils import *


def transform_geral(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas geral")

    #select and save
    file = "Geral_candidaturas.csv"
    select_list = ["N_Proj", "Promotor/Dt_Inicio_Act", "Promotor/Dt_Const", "Parametros/Ano_Cand",
            "Consultora/Nif", "Dadosprojecto/Dt_Inicio", "Dadosprojecto/Dt_Fim", "Resumo/Dimensao",
            "Promotor/Concelho_D", "Resumo/Concelho_D", "Promotor/Nif", "Promotor/Nat_Jur"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    init_shape = df.shape

    #rename nif_promotor
    df = df.rename(columns={"Promotor/Nif": "nif_promotor"})

    #localização do investimento e da sede
    df = df.rename(columns={"Promotor/Concelho_D": "Loc_Investimento"})
    df = df.rename(columns={"Resumo/Concelho_D": "Loc_Sede"})

    """
    # mapeamento NUTS
    variaveis = pd.read_excel(params.NUTS, engine='openpyxl')
    name_mapping = dict(zip(variaveis["Concelho"], variaveis["NUTS II CURTO"]))
    df["Loc_Investimento"] = df["Loc_Investimento"].map(name_mapping)
    df["Loc_Sede"] = df["Loc_Sede"].map(name_mapping)

    """


    #data de inicio e de fim de projeto
    df = df.rename(columns={"Dadosprojecto/Dt_Inicio": "data_inicio_projeto",
                            "Dadosprojecto/Dt_Fim": "data_fim_projeto"})

    #dimensao_emp
    df = df.rename(columns={"Resumo/Dimensao": "dimensao_emp"})
    df["dimensao_emp"] = df["dimensao_emp"].astype(str)
    df["dimensao_emp"] = np.where((df["dimensao_emp"] == "1"), "Micro", df["dimensao_emp"])
    df["dimensao_emp"] = np.where((df["dimensao_emp"] == "2"), "Pequena", df["dimensao_emp"])
    df["dimensao_emp"] = np.where((df["dimensao_emp"] == "3"), "Média", df["dimensao_emp"])
    df["dimensao_emp"] = np.where((df["dimensao_emp"] == "4"), "Não PME", df["dimensao_emp"])

    #apresenta_consultora
    df = df.rename(columns={"Consultora/Nif": "nif_consultora"})
    df["apresenta_consultora"] = np.where(pd.notnull(df["nif_consultora"]), 1, 0)

    #nifs
    df["nif_consultora"] = fill_na(df["nif_consultora"], 0)
    df["nif_consultora"] = df["nif_consultora"].astype('Int64')
    df.insert(df.columns.get_loc("nif_promotor") + 1, 'nif_prom_valido',
              df["nif_promotor"].apply(lambda nif: valida_nif(str(nif).strip())))
    df.insert(df.columns.get_loc("nif_consultora") + 1, 'nif_consul_valido',
              df["nif_consultora"].apply(lambda nif: valida_nif(str(nif).strip())))
    nif_types = ["singular", "estrangeiro", "coletivo", "outro"]
    for type in nif_types:
        classificar_nif(df, "nif_promotor", "prom", type)
        classificar_nif(df, "nif_consultora", "consul", type)

    #datas
    df["data_inic"] = df["Promotor/Dt_Inicio_Act"].apply(
        lambda data: datetime.datetime.strptime(data, '%Y-%m-%d'))
    df["data_const"] = np.where(pd.isnull(df["Promotor/Dt_Const"]),
                                df["Promotor/Dt_Inicio_Act"], df["Promotor/Dt_Const"])
    df["data_const"] = df["data_const"].apply(
        lambda data: datetime.datetime.strptime(data, '%Y-%m-%d'))

    df["abertura_atividade_prom_cand"] = np.where(
        (df["data_const"] <= df["data_inic"]), df["data_const"],
        df["data_inic"])

    df["ano_abertura_atividade_prom_cand"] = df["abertura_atividade_prom_cand"].apply(
        lambda data: data.year).astype(int)

    df = df.rename(columns={"Parametros/Ano_Cand": "ano_candidatura"})
    df["anos_ate_cand"] = df["ano_candidatura"] - df["ano_abertura_atividade_prom_cand"]

    df["emp_menos_3_anos_cand"] = np.where((df["anos_ate_cand"] <= 3), 1, 0)
    df["emp_menos_3_anos_cand"] = np.where(pd.isnull(df["anos_ate_cand"]), np.nan,
                                           df["emp_menos_3_anos_cand"])

    df["emp_menos_4_anos_cand"] = np.where((df["anos_ate_cand"] <= 4), 1, 0)
    df["emp_menos_4_anos_cand"] = np.where(pd.isnull(df["anos_ate_cand"]), np.nan,
                                           df["emp_menos_4_anos_cand"])

    df["emp_recente"] = np.where((df["anos_ate_cand"] <= 0), 1, 0)
    df["emp_recente"] = np.where(pd.isnull(df["anos_ate_cand"]), np.nan,
                                 df["emp_recente"])

    #drops
    to_drop = ["data_inic", "data_const", "Promotor/Dt_Inicio_Act", "Promotor/Dt_Const"]
    df = df.drop(columns=to_drop)

    #checks
    final_shape = df.shape
    if init_shape[0] != final_shape[0]:
        raise Exception('Number of initial rows and after transformation rows is not the same!')

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_financ(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas financiamentos")

    #select and save
    file = "Financ_candidaturas.csv"
    select_list = ["N_Proj", "Rubrica",'Rubrica_D', 'Perc', 'Val_P1', 'Val_0', 'Val_1', 'Val_2', 'Val_3',
                   'Val_4', 'Val_5', 'Val_6', 'Val_7']

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    #financiamentos
    df_financ = df[(df["Rubrica"] == 1) | (df["Rubrica"] == 2) | (df["Rubrica"] == 4)]
    df_financ['soma'] = df_financ[
        ['Val_P1', 'Val_0', 'Val_1', 'Val_2', 'Val_3', 'Val_4', 'Val_5', 'Val_6', 'Val_7']].sum(axis=1)
    df_financ = df_financ[['N_Proj', 'Rubrica', 'Rubrica_D', 'Perc', 'soma']]
    df_financ_grouped = df_financ.groupby(['N_Proj', 'Rubrica'])['soma'].sum().reset_index()
    soma_total = df_financ.groupby(['N_Proj'])['soma'].sum().reset_index(name='soma_total')
    df_financ_grouped = df_financ_grouped.merge(soma_total, how="left", on=["N_Proj"])
    df_financ_grouped['percentagem'] = (df_financ_grouped['soma'] / df_financ_grouped['soma_total'] * 100).round(2)
    df_financ_grouped = df_financ_grouped[["N_Proj", "Rubrica", "percentagem"]]
    temp = df_financ_grouped.groupby(["N_Proj", "Rubrica"])["percentagem"].first().unstack(
        fill_value=np.nan).reset_index().rename_axis(None, axis=1)
    df = fill_na(temp, 0)
    df = df.rename(columns={1: "cap_proprios_calc", 2: "autofinanciamento_calc", 4: "financiamentos_calc"})

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_proj_cae(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas proj_cae")

    #select and save
    file = "ProjCae_candidaturas.csv"
    select_list = ["N_Proj", "Cae", "Cae_D", "Perc"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    # existem vários CAE com pesos iguais para um mesmo projeto, então é dificil escolher uma CAE única

    # primeiro fez-se um mapeamento que também agrega os CAE, depois a escolha foi o CAE com a descrição maior
    # podemos fazer uma estatística de quantos CAE com o mesmo peso é que os projetos têm

    # descobrir o CAE com percentagem maior para cada projeto
    df_single_max_perc = df.groupby(["N_Proj"])["Perc"].max().reset_index(name="max_perc")
    # criar coluna que tem para cada linha, a maior percentagem desse projeto
    df_max_perc = df[["N_Proj", "Cae", "Cae_D", "Perc"]].merge(df_single_max_perc, on="N_Proj", how='left')
    # descartar linhas que têm percentagens inferiores à maior, essas não são candidatas à CAE principal
    df_many_max_perc = df_max_perc[df_max_perc['Perc'] == df_max_perc['max_perc']]

    

    # mapeamento novo com bónus de agregação
    variaveis = pd.read_excel(params.CAE, engine='openpyxl')
    group_mapping = dict(zip(variaveis["codigo"], variaveis["grupo"]))
    name_mapping = dict(zip(variaveis["grupo"], variaveis["nome grupo"]))
    df_many_max_perc["grupo_cae_proj"] = df_many_max_perc["Cae"].map(group_mapping)
    df_many_max_perc["nome_grupo_cae_proj"] = df_many_max_perc["grupo_cae_proj"].map(name_mapping)
    # df_many_max_perc.to_excel(r'csvs created\\' + 'CAE_proj.xlsx', index = False, encoding='utf-8-sig')
    if df_many_max_perc["grupo_cae_proj"].isnull().sum() > 0:
        raise Exception('Faltam mapeamentos, criar ficheiro excel para verificação')
    df_many_max_perc = df_many_max_perc[['N_Proj', "grupo_cae_proj", "nome_grupo_cae_proj"]]

    # contar quantos CAE principal tem cada projeto
    df_many_max_perc = df_many_max_perc.drop_duplicates()
    df_many_max_perc_grouped = df_many_max_perc.groupby("N_Proj").size().reset_index(name="Numero_CAE_principal")

    df_many_max_perc_grouped_twice = df_many_max_perc_grouped.groupby(["Numero_CAE_principal"]).size().reset_index(
        name="Numero_projetos_com_x_CAE_principal")
    df_many_max_perc_grouped_twice["Perc_projetos_com_x_CAE_principal"] = df_many_max_perc_grouped_twice[
                                                                              "Numero_projetos_com_x_CAE_principal"] / \
                                                                          df_many_max_perc_grouped_twice[
                                                                              "Numero_projetos_com_x_CAE_principal"].sum() * 100

    # df_many_max_perc_grouped_twice.to_excel(r'csvs created\\' + 'stats_CAE_proj.xlsx', index = False, encoding='utf-8-sig')

    # descartar as duplicadas que têm a descrição mais curta
    
    df_many_max_perc["nome_grupo_cae_proj_string_size"] = df_many_max_perc["nome_grupo_cae_proj"].str.len()
    new_df = pd.DataFrame(columns=df_many_max_perc.columns)
    for proj in df_many_max_perc['N_Proj'].unique():
        df_proj = df_many_max_perc[df_many_max_perc["N_Proj"] == proj]
        df_proj_first = df_proj.sort_values("nome_grupo_cae_proj_string_size", ascending=False).head(1)
        new_df = new_df.append(df_proj_first)

    df = new_df[["N_Proj", "grupo_cae_proj", "nome_grupo_cae_proj"]]
    df["tipo_CAE"] = df["grupo_cae_proj"].apply(lambda val: "servicos" if val >= 37 else "industria")


    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_socios(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas socios")

    #select and save
    file = "Socios_candidaturas.csv"
    select_list = ["N_Proj", "Nif"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    df = df[df["Nif"].notnull()]
    df_socio_count_proj = df.groupby(['N_Proj', "Nif"])["Nif"].count().reset_index(name='socio_count_proj')
    socio_difs_projs = df_socio_count_proj.groupby(["Nif"])["Nif"].count().reset_index(name='socio_difs_projs')
    socio_difs_projs["pares_socios_dif_projs"] = \
        np.where((socio_difs_projs["socio_difs_projs"] > 1), 1, 0)

    df_final = df.merge(socio_difs_projs[["Nif", "pares_socios_dif_projs"]], how="left", on=["Nif"])

    df_socios = df.groupby(['N_Proj'])['N_Proj'].count().reset_index(name='socios')
    df_final = df_final.merge(df_socios, how="left", on=["N_Proj"])

    df_final_cleaned = df_final[['N_Proj', "pares_socios_dif_projs", 'socios']].drop_duplicates()
    df_final_cleaned = df_final_cleaned.groupby(["N_Proj", "socios", 'pares_socios_dif_projs'])[
        'pares_socios_dif_projs'].first().unstack(fill_value=np.nan).reset_index().rename_axis(None, axis=1)
    df_final_cleaned = df_final_cleaned.rename(columns={1: "pares_socios_dif_projs"})
    df_final_cleaned = df_final_cleaned[['N_Proj', "pares_socios_dif_projs", 'socios']]
    df_final_cleaned["pares_socios_dif_projs"] = fill_na(df_final_cleaned["pares_socios_dif_projs"], 0)

    df_final_cleaned["apresenta_socios"] = 1
    df = df_final_cleaned
    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_vendas_ext(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas vendas externas")

    #select and save
    file = "VendasExt_candidaturas.csv"
    select_list = ["N_Proj", "Vendas_Pre"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    df = df.groupby(["N_Proj"])["Vendas_Pre"].sum().reset_index(name='vendas_pre_total')
    df["vendas_merc_ext_sup_0_pre_proj"] = np.where((df["vendas_pre_total"] > 0), 1, 0)
    df = df[["N_Proj", "vendas_merc_ext_sup_0_pre_proj"]]

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_tipologia(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):

    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas tipologia")

    #select and save
    file = "Tipologia_candidaturas.csv"
    select_list = ["N_Proj", "Tipologia",'Select']

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index = False, encoding='utf-8-sig')

    temp_df = df
    temp = temp_df.groupby(["N_Proj", "Tipologia"])['Select'].first().unstack(
        fill_value=np.nan).reset_index().rename_axis(None, axis=1)
    value_to_binary(temp, temp.iloc[:, 1:])
    rename_dummies_names(temp, temp.columns[1:], "tipologia")
    df = temp

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


def transform_dominios_prioritarios(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas domínios prioritários")

    # select and save
    file = "DominiosPrioritarios_candidaturas.csv"
    select_list = ["N_Proj", "Dominio"]

    full_df = pd.read_csv(folder + file)
    df = full_df[select_list]
    df.to_excel(selected_cols_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')

    temp_df = df
    temp = temp_df.groupby(["N_Proj", "Dominio"])["Dominio"].first().unstack(
        fill_value=np.nan).reset_index().rename_axis(None, axis=1)
    value_to_binary(temp, temp.iloc[:, 1:])
    rename_dummies_names(temp, temp.columns[1:], "dominio")
    df = temp

    df.to_excel(transformed_not_derived_path + file.replace(".csv", ".xlsx"), index=False, encoding='utf-8-sig')
    return df


#includes NUTS and CAE mapping
def transform_cand(agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path,
                   transformed_not_derived_merged_path):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Transforming candidaturas")
    if agency == params.iapmei:
        folder = params.iapmei_cand
    else:
        folder = params.aicep_cand

    dfs = []

    dfs.append(transform_geral(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs.append(transform_financ(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs.append(transform_proj_cae(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs.append(transform_socios(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs.append(transform_vendas_ext(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs.append(transform_tipologia(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))
    dfs.append(transform_dominios_prioritarios(folder, agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path))

    previous_df = dfs[0]
    if len(dfs) > 1:
        print(dt.datetime.now().strftime("%H:%M:%S"), "Merging candidaturas")

        for df in dfs[1:]:
            previous_df = previous_df.merge(df, on=["N_Proj"], how="left")

            
        previous_df.drop_duplicates(keep=False, inplace=True)
        previous_df.to_excel("merge_data.xlsx", index = False)
        if previous_df['N_Proj'].duplicated().any():
            raise Exception('Found duplicates in merge!')

    print(dt.datetime.now().strftime("%H:%M:%S"), "Saving all candidaturas")
    previous_df.to_excel(transformed_not_derived_merged_path + "N_Proj_candidaturas.xlsx", index=False, encoding='utf-8-sig')




