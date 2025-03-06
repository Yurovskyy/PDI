import pandas as pd
import numpy as np
import os

# Caminhos
caminho_base = os.path.dirname(__file__)

path_economatica = r'dados_economatica_3T2023'
path_dados_cadastrais = r'00_dados_cadastrais.xlsx'
path_patrimonio_liquido = r'05_patrimonio_liquido.xlsx'

path_dataset = r'dados_CGVN'
path_CNVG = r'dataset_CGVN.xlsx'

output_path = os.path.join(caminho_base, "dados.xlsx")

# Leitura das planilhas
dados_cadastrais = pd.read_excel(os.path.join(caminho_base, path_economatica, path_dados_cadastrais), header=3)
dados_CGVN = pd.read_excel(os.path.join(caminho_base, path_dataset, path_CNVG), header=0)
dados_patrimonio = pd.read_excel(os.path.join(caminho_base, path_economatica, path_patrimonio_liquido), header=3)

## Dados cadastrais

# Seleciona as colunas de interesse (10 é CNPJ e a 6 é Ticker(Código))
dados_cadastrais = dados_cadastrais.iloc[:, [10, 6]].copy()
dados_cadastrais.columns = ['CNPJ', 'Ticker']

# Limpeza dos campos para garantir o merge
dados_cadastrais['CNPJ'] = dados_cadastrais['CNPJ'].astype(str).str.strip()
dados_cadastrais['Ticker'] = dados_cadastrais['Ticker'].astype(str).str.strip().str.upper()

## Dados CGVN

# Pivot para transformar os indicadores em colunas
dados_CGVN = dados_CGVN.pivot(index=["CNPJ_Companhia", "Data_Referencia"], 
                              columns="ID_Item", 
                              values="Pratica_Adotada").reset_index()

# Renomear a coluna de CNPJ e extrair o ano da data
dados_CGVN.rename(columns={"CNPJ_Companhia": "CNPJ"}, inplace=True)
dados_CGVN['Ano'] = dados_CGVN['Data_Referencia'].astype(str).str.extract(r'(\d{4})').astype(int)
dados_CGVN['CNPJ'] = dados_CGVN['CNPJ'].astype(str).str.replace(r'[./-]', '', regex=True)
dados_CGVN.drop(columns=["Data_Referencia"], inplace=True)

## Dados patrimônio

# Ajustar os nomes das colunas: manter 'Data' e extrair o ticker da última linha do cabeçalho
dados_patrimonio.columns = [
    col if col.strip() == "Data" 
    else col.split('\n')[-1].strip() 
    for col in dados_patrimonio.columns
]

# Extrair o ano da coluna 'Data' (formato "1T1999", "2T1999", etc.)
dados_patrimonio['Ano'] = dados_patrimonio['Data'].str.extract(r'T(\d{4})').astype(int)

# Converter de wide para long: teremos as colunas Data, Ano, Ticker e Patrimonio
dados_patrimonio = dados_patrimonio.melt(id_vars=['Data', 'Ano'], 
                                         var_name='Ticker', 
                                         value_name='Patrimonio')

# Substituir valores não numéricos por NaN e preencher com o valor anterior
dados_patrimonio.replace(['----', '-', ' '], np.nan, inplace=True)
dados_patrimonio.sort_values(by=["Ano", "Data"], inplace=True)
dados_patrimonio = dados_patrimonio.ffill()

# Calcular a média anual para cada Ticker
dados_patrimonio = (
    dados_patrimonio.groupby(['Ano', 'Ticker'], as_index=False)['Patrimonio']
    .mean()
)

# Garantir que o Ticker esteja padronizado para o merge com patrimônio
dados_patrimonio['Ticker'] = dados_patrimonio['Ticker'].astype(str).str.strip().str.upper()

## Merge

# 1. Merge entre indicadores e dados cadastrais (chave: CNPJ)
df_merged = pd.merge(dados_CGVN, dados_cadastrais, on="CNPJ", how="inner")

# 2. Merge com patrimônio líquido (chaves: Ano e Ticker)
df_final = pd.merge(df_merged, dados_patrimonio, on=["Ano", "Ticker"], how="left")

# Reordenar as colunas: CNPJ, Data, Ticker, Patrimonio, 52 indicadores (as demais colunas)
fixed_cols = ['CNPJ', 'Ano', 'Ticker', 'Patrimonio']
other_cols = [col for col in df_final.columns if col not in fixed_cols]
df_final = df_final[fixed_cols + other_cols]

# Exportando
df_final.to_excel(output_path, index=False)
