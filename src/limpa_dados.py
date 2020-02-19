from utils import query_yes_no, PERGUNTA
import glob
import pandas as pd
import numpy as np
from datetime import datetime
import os
import re
from tqdm import tqdm

DEBUG = False

def executa_limpeza():
    APAGA = query_yes_no(PERGUNTA)
    if DEBUG:
        PATH = "C:/Users/Neylson/Documents/RAIS"
        arquivos = glob.glob(PATH + "/data/*.csv")
    else:
        arquivos = glob.glob("../data/*.csv")
    
    print("Aplicando regras de limpeza...")
    limpos = []
    for arq in tqdm(arquivos):
        bd = pd.read_csv(arq, sep='\t', decimal=',')
        limpos.append(limpa_dados(bd, arq))
    
    # Exporta os dados
    print("Exportando os dados para a pasta data/processed/ ...")
    pd.concat(limpos, sort=False)[[
        'Ano', 'CNAE 2.0 Classe', 
       'Escolaridade após 2005', 'Qtd Hora Contr', 'Idade',
       'Ind Simples',
       'Mun Trab', 'Município', 'Nacionalidade','Brasileiro', 'Natureza Jurídica',
       'Ind Portador Defic', 'Qtd Dias Afastamento', 'Raça Cor',
       'Regiões Adm DF', 'Vl Remun Dezembro Nom', 'Vl Remun Dezembro (SM)',
       'Vl Remun Média Nom', 'Vl Remun Média (SM)', 'CNAE 2.0 Subclasse',
       'Sexo Trabalhador', 'Tamanho Estabelecimento', 'Tempo Emprego',
       'Tipo Admissão', 'Tipo Defic', 'Tipo Salário',
       'Tipo Vínculo']].to_csv("../data/processed/rais_2008_2018.csv", sep='\t', index=False)

    if APAGA:
        [os.remove(i) for i in arquivos]



def limpa_dados(dados, arquivo): 

    if 'Tipo Salário' in dados.columns:
        # Filtra apenas salário mensal
        dados['Tipo Salário'] = pd.to_numeric(dados['Tipo Salário'], errors='coerce')
        dados = dados.loc[dados["Tipo Salário"] == 1, :]
    
    if 'Ind Trab Parcial' in dados.columns:
        # Filçtra apenas trabalhos não parciais
        dados['Ind Trab Parcial'] = pd.to_numeric(dados['Ind Trab Parcial'], errors='coerce')
        dados = dados.loc[dados["Ind Trab Parcial"] == 0, :]
    
    # Corrige grau de escolaridade para obter valor 0
    dados['Escolaridade após 2005'] = pd.to_numeric(dados['Escolaridade após 2005'], errors="coerce")
    dados['Escolaridade após 2005'] -= 1
    # Coloca label em sexo
    dados['Sexo Trabalhador'] = pd.to_numeric(dados['Sexo Trabalhador'],errors="coerce")
    dados.loc[dados['Sexo Trabalhador'] == 1, 'Sexo Trabalhador'] = "Masculino"
    dados.loc[dados['Sexo Trabalhador'] == 2, 'Sexo Trabalhador'] = "Feminino"
    # Filtra apenas quem Raça Cor não é 99
    dados['Raça Cor'] = pd.to_numeric(dados['Raça Cor'], errors="coerce")
    dados = dados.loc[dados["Raça Cor"] != 99,:]
    # Coloca label em raça
    dados.loc[dados["Raça Cor"] == 1, 'Raça Cor'] = "Indígena"
    dados.loc[dados["Raça Cor"] == 2, 'Raça Cor'] = "Branca"
    dados.loc[dados["Raça Cor"] == 4, 'Raça Cor'] = "Preta"
    dados.loc[dados["Raça Cor"] == 6, 'Raça Cor'] = "Amarela"
    dados.loc[dados["Raça Cor"] == 8, 'Raça Cor'] = "Parda"
    dados.loc[dados["Raça Cor"] == 9, 'Raça Cor'] = "Não identificada"
    # Filtra apenas quem tem salário maior que 0 e idade de 18 a 80 anos
    dados['Idade'] = pd.to_numeric(dados['Idade'], errors="coerce")
    dados = dados.loc[(dados['Vl Remun Média Nom'] > 0) & 
                        (dados['Idade'] > 17) & 
                        (dados['Idade'] < 81), :]

    # Cria variável binária Brasileiro
    dados['Nacionalidade'] = pd.to_numeric(dados['Nacionalidade'], errors="coerce")
    dados['Brasileiro'] = dados['Nacionalidade'].apply(lambda x: 1 if (x == 10) else 0)

    ano = 2018
    if re.search('VINC', arquivo) is not None:
        pass
    else:
        ano = int(arquivo[-8:-4])
    dados['Ano'] = np.array([ano] * dados.shape[0])

    return dados



if __name__ == "__main__":
    executa_limpeza()

    