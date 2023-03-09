from pathlib import Path


#master folder
folder = "E:/IA SI DADOS/pipeline/"

metadata = "E:/IA SI DADOS/pipeline/metadata/Mapeamento.xlsx"

#data sources folder: put all source files here

iapmei_sources = "iapmei sources/"
iapmei_cand = iapmei_sources + "candidaturas/"
iapmei_faci = iapmei_sources + "faci/"
iapmei_ppi = iapmei_sources + "ppi/"
iapmei_appi = iapmei_sources + "appi/"
iapmei_facie = iapmei_sources + "facie/"
iapmei_anulacoes = folder + "iapmei sources/iapmei outras fontes/Anulações.xlsx"
iapmei_ies = folder + "iapmei sources/racios ies"
iapmei_anulacoes_features = folder + "features/iapmei_anulacoes.xlsx"
iapmei_text_mining_ineleg_features = folder + "features/Var_from_TM_iapmei_inl.csv"
iapmei_text_mining_anul_features = folder + "features/Var_from_TM_iapmei_anl.csv"

aicep_sources = folder + "aicep sources/aicep csvs"
aicep_cand = aicep_sources + "candidaturas_dados_aicep/csv_dados"
aicep_faci = aicep_sources + "faci_dados_aicep/csv_dados"
aicep_ppi = aicep_sources + "ppi_dados_aicep/csv_dados"
aicep_appi = aicep_sources + "appi_dados_aicep/csv_dados"
aicep_facie = aicep_sources + "facie_dados_aicep/csv_dados"
aicep_ies = folder + "aicep sources/ies"

aicep_em_encerramento = folder + "aicep sources/aicep outras fontes/lista_com_facie.xlsx"
aicep_anulacoes = folder + "aicep sources/aicep outras fontes/anulacoes_dados.xlsx"
aicep_terminados = folder + "aicep sources/aicep outras fontes/projetos_terminados.xlsx"

aicep_anulacoes_features = folder + "features/aicep_anulacoes.xlsx"
aicep_text_mining_ineleg_features = folder + "features/Var_from_TM_aicep_inl.csv"
aicep_text_mining_anul_features = folder + "features/Var_from_TM_aicep_anl.csv"

CAE = folder + "our sources/mapeamento das CAE.xlsx"
NUTS = folder + "our sources/NUTS.xlsx"


#other params
inelg = "inelegibilidades"
anul = "anulacoes"
ies = "indicadores financeiros"
text_mining = "text mining"

iapmei = "iapmei"
aicep = "aicep"

#backup folders
backup_root_folder = folder + "backups/"

#save folders
save_root_folder = folder + "saves/"

save_folders = ["selected columns",
                "cleaned",
                "transformed not derived",
                "transformed not derived merged",
                "transformed not derived merged all",
                "merged cleaned",
                "transformed derived",
                "imputed known values",
                "transformed ies",
                "merged with ies",
                "merged with text mining",
                "imputed unknown values",
                "with drops",
                "dummied",
                "normalized"]

