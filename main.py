import pandas as pd
import datetime as dt
from distutils.dir_util import copy_tree, remove_tree
from shutil import move
from IPython.display import display
import params
import sys
from params import *
from transform_cand import *
from transform_faci import *
from transform_ppi_appi_facie import *
from transform_ppi import *
from transform_ies import *
from utils import *
from corrections import *
from transform_derived import *
from impute_unknown_values import *
from drop_unwanted import *
from data_dictionary import *


#create folder and parents if they doen't exist
def create_folder(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def create_save_folders(save_root_folder, save_folders, agency, type):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Creating needed folders")
    for save_folder in save_folders:
        save_folder_path = save_root_folder + save_folder + "\\" + type + "\\" + agency
        create_folder(save_folder_path)


def backup_data(from_folder, to_folder):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Backing up data to", to_folder)
    copy_tree(from_folder, to_folder, verbose=1)
    remove_tree(from_folder)


def main(agency, type, test_new, transform, impute_unknown, final_touches, create_dict_sources, create_dict_others):
    print(dt.datetime.now().strftime("%H:%M:%S"), "Starting")

    create_folder(params.save_root_folder)
    backup_timestamp = dt.date.today().strftime('%m-%d') + " " + \
                      dt.datetime.now().strftime("%H-%M-%S")
    backup_folder = params.backup_root_folder + "backup_" + backup_timestamp + "\\"
    # if transform:
    #     backup_data(params.save_root_folder, backup_folder)

    create_save_folders(params.save_root_folder, params.save_folders, agency, type)

    null_stats_path = params.save_root_folder + agency + "_" + type + "_"
    selected_cols_path = params.save_root_folder + params.save_folders[0] + "\\" + type + "\\" + agency + "\\"
    cleaned_path = params.save_root_folder + params.save_folders[1] + "\\" + type + "\\" + agency + "\\"
    transformed_not_derived_path = params.save_root_folder + params.save_folders[2] + "\\" + type + "\\" + agency + "\\"
    transformed_not_derived_merged_path = params.save_root_folder + params.save_folders[3] + "\\" + type + "\\" + agency + "\\"
    transformed_not_derived_merged_all_path = params.save_root_folder + params.save_folders[4] + "\\" + type + "\\" + agency + "\\merged.xlsx"
    cleaned_all_path = params.save_root_folder + params.save_folders[5] + "\\" + type + "\\" + agency + "\\cleaned_all.xlsx"
    transformed_derived_path = params.save_root_folder + params.save_folders[6] + "\\" + type + "\\" + agency + "\\derived.xlsx"
    imputed_known_values_path = params.save_root_folder + params.save_folders[7] + "\\" + type + "\\" + agency + "\\imputed_known.xlsx"
    transformed_ies_path = params.save_root_folder + params.save_folders[8] + "\\" + type + "\\" + agency + "\\transformed_ies.xlsx"
    merged_with_ies = params.save_root_folder + params.save_folders[9] + "\\" + type + "\\" + agency + "\\merged_with_ies.xlsx"
    merged_with_text_mining = params.save_root_folder + params.save_folders[10] + "\\" + type + "\\" + agency + "\\" + agency + "_" + type + ".xlsx"
    imputed_unknown_values_path = params.save_root_folder + params.save_folders[11] + "\\" + type + "\\" + agency
    with_drops = params.save_root_folder + params.save_folders[12] + "\\" + type + "\\" + agency
    dummied = params.save_root_folder + params.save_folders[13] + "\\" + type + "\\" + agency
    normalized = params.save_root_folder + params.save_folders[14] + "\\" + type + "\\" + agency

    df = None
    ies = None

    #transform
    if type == params.inelg:
        if transform:
            transform_cand(agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path, transformed_not_derived_merged_path)
            transform_faci(agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path, transformed_not_derived_merged_path)
            transform_ppi(agency, type, selected_cols_path, cleaned_path, transformed_not_derived_path, transformed_not_derived_merged_path)
            transform_ppi_appi_facie(agency, type, test_new, selected_cols_path, cleaned_path, transformed_not_derived_path, transformed_not_derived_merged_path)
            df = merge_transformed(transformed_not_derived_merged_path, transformed_not_derived_merged_all_path)
            df = correct(agency, type, df, cleaned_all_path)
            df = transform_derived(agency, type, df, transformed_derived_path)
            generate_null_stats(df, null_stats_path + "nulls_before_impute_known_values.txt")
            df = impute_known_values(agency, type, df, imputed_known_values_path)
            generate_null_stats(df, null_stats_path + "nulls_after_impute_known_values.txt")
    else:
        #ler o df das anulacoes
        if agency == params.iapmei:
            path = params.iapmei_anulacoes_features
        else:
            path = params.aicep_anulacoes_features
        df = pd.read_excel(path, engine='openpyxl')

    # transform ies and merge
    if transform:
        # transformed_not_derived_merged_path to get ano_candidatura to anulacoes
        ies = transform_ies(agency, type, df, transformed_not_derived_merged_path, transformed_ies_path)
        df = merge_with_ies(type, df, ies, transformed_ies_path, merged_with_ies)
        df = merge_with_text_mining(agency, type, df, merged_with_text_mining)

    #todo
    df = pd.read_excel(merged_with_text_mining, engine='openpyxl')
    create_data_dictionary(agency, type, merged_with_text_mining, create_dict_sources, create_dict_others)

    #impute_unknown_values
    mean_path = imputed_unknown_values_path + "\\" + "mean_impute.xlsx"
    zero_path = imputed_unknown_values_path + "\\" + "zero_impute.xlsx"
    if impute_unknown:
        if not transform:
            print(dt.datetime.now().strftime("%H:%M:%S"), "Reading data")
            df = pd.read_excel(merged_with_text_mining, engine='openpyxl')
        generate_null_stats(df, null_stats_path + "nulls_before_impute_unknown_values.txt")
        df_mean, df_zero = impute_unknown_values(agency, type, df, transformed_not_derived_merged_path, mean_path, zero_path)
        generate_null_stats(df_mean, null_stats_path + "nulls_after_impute_unknown_values_mean.txt")
        generate_null_stats(df_zero, null_stats_path + "nulls_after_impute_unknown_values_zero.txt")
    else:
        if final_touches:
            print(dt.datetime.now().strftime("%H:%M:%S"), "Reading data")
            df_mean = pd.read_excel(mean_path, engine='openpyxl')
            df_zero = pd.read_excel(zero_path, engine='openpyxl')

    #final touches
    if final_touches:
        df_mean = drop_unwanted(agency, type, df_mean, "mean", transformed_not_derived_merged_path, with_drops + "\\mean_with_drops.xlsx")
        df_zero = drop_unwanted(agency, type, df_zero, "zero", transformed_not_derived_merged_path, with_drops + "\\zero_with_drops.xlsx")
        df_mean = create_dummies(agency, type, df_mean, dummied + "\\mean_dummied.xlsx")
        df_zero = create_dummies(agency, type, df_zero, dummied + "\\zero_dummied.xlsx")
        df_mean = normalize(agency, type, df_mean, normalized + "\\mean_normalized.xlsx")
        df_zero = normalize(agency, type, df_zero, normalized + "\\zero_normalized.xlsx")

    print(dt.datetime.now().strftime("%H:%M:%S"), "The end")


if __name__ == "__main__":
    agency = params.iapmei
    type = params.inelg
    test_new = False
    transform = True
    impute_unknown = False
    final_touches = False
    create_dict_sources = True
    create_dict_others = False
    main(agency, type, test_new, transform, impute_unknown, final_touches, create_dict_sources, create_dict_others)