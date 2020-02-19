from utils import query_yes_no, PERGUNTA
import glob
import pandas as pd
from datetime import datetime
import os

def limpa_dados():
    APAGA = query_yes_no(PERGUNTA)
    arquivos = glob.glob("../data/*.csv")
    
    print("Lendo e concatenando os dados...")
    dados = pd.concat([pd.read_csv(arq, sep='\t', decimal=',') for arq in arquivos])
    
    print("Aplicando regras de limpeza...")
    # Filtra apenas salário mensal
    dados = dados.loc[dados["Tipo Salário"] == 1, :]
    # Filçtra apenas trabalhos não parciais
    dados = dados.loc[dados["Ind Trab Parcial"] == 0, :]
    # Corrige grau de escolaridade para obter valor 0
    dados['Escolaridade após 2005'] -= 1
    # Coloca label em sexo
    dados.loc[dados['Sexo Trabalhador'] == 1, 'Sexo Trabalhador'] = "Masculino"
    dados.loc[dados['Sexo Trabalhador'] == 2, 'Sexo Trabalhador'] = "Feminino"
    # Filtra apenas quem Raça Cor não é 99
    dados = dados.loc[dados["Raça Cor"] != 99,:]
    # Coloca label em raça
    dados.loc[dados["Raça Cor"] == 1, 'Raça Cor'] = "Indígena"
    dados.loc[dados["Raça Cor"] == 2, 'Raça Cor'] = "Branca"
    dados.loc[dados["Raça Cor"] == 4, 'Raça Cor'] = "Preta"
    dados.loc[dados["Raça Cor"] == 6, 'Raça Cor'] = "Amarela"
    dados.loc[dados["Raça Cor"] == 8, 'Raça Cor'] = "Parda"
    dados.loc[dados["Raça Cor"] == 9, 'Raça Cor'] = "Não identificada"
    # Filtra apenas quem tem salário maior que 0 e idade de 18 a 80 anos
    dados = dados.loc[(dados['Vl Remun Média Nom'] > 0) & 
                        (dados['Idade'] > 17) & 
                        (dados['Idade'] < 81), :]

    # Cria variável binária Brasileiro
    dados['Brasileiro'] = dados['Nacionalidade'].apply(lambda x: 1 if (x == 10) else 0)

    # Exporta os dados
    print("Exportando os dados para a pasta data/processed/ ...")
    dados.to_csv("../data/processed/rais_2008_2018.csv", sep='\t', index=False)

    if APAGA:
        [os.remove(i) for i in arquivos]


if __name__ == "__main__":
    limpa_dados()

    