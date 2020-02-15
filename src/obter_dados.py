import wget
import py7zr
from tqdm import tqdm
import dask.dataframe as dd
import pandas as pd
import os

urls = [
    #2018
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2018/RAIS_VINC_PUB_SP.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2018/RAIS_VINC_PUB_MG_ES_RJ.7z",
    #2017
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2017/SP2017.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2017/MG2017.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2017/ES2017.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2017/RJ2017.7z",
    #2016
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2016/SP2016.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2016/MG2016.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2016/ES2016.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2016/RJ2016.7z",
    #2015
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2015/SP2015.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2015/MG2015.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2015/ES2015.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2015/RJ2015.7z",
    #2014
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2014/SP2014.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2014/MG2014.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2014/ES2014.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2014/RJ2014.7z",
    #2013
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2013/SP2013.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2013/MG2013.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2013/ES2013.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2013/RJ2013.7z",
    #2012
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2012/SP2012.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2012/MG2012.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2012/ES2012.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2012/RJ2012.7z",
    #2011
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2011/SP2011.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2011/MG2011.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2011/ES2011.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2011/RJ2011.7z",
    #2010
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2010/SP2010.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2010/MG2010.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2010/ES2010.7z",
    #"ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2010/RJ2010.7z",
    #2009
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2009/SP2009.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2009/MG2009.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2009/ES2009.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2009/RJ2009.7z",
    #2008
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2008/SP2008.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2008/MG2008.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2008/ES2008.7z",
    "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2008/RJ2008.7z"    
]

def importa_filtra(url):
    filename = wget.download(url,out="../data/raw/")
    archive = py7zr.SevenZipFile(filename)
    archive.extractall(path="../data/raw/")
    archive.close()
    os.remove(filename)
    file = filename[:-2] + "txt"
    if url == "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/2008/SP2008.7z":
        df = dd.read_csv(file, sep=';', encoding="latin1", assume_missing=True,
                        dtype={'CBO Ocupação 2002': 'object',
                                'Faixa Remun Dezem (SM)': 'object',
                                'Faixa Tempo Emprego': 'object',
                                'Faixa Etária': 'object',
                                'Bairros SP': 'object',
                                'Distritos SP': 'object',
                                'Bairros RJ': 'object',
                                'Raça Cor': 'object',
                                'Natureza Jurídica': 'object',
                                'CNAE 2.0 Subclasse': 'object',
                                'Tipo Defic': 'object',
                                'CNAE 2.0 Classe': 'object',
                                'CNAE 95 Classe': 'object'})
        df['CNAE 2.0 Classe'] = df['CNAE 2.0 Classe'].apply(pd.to_numeric, errors='coerce')
    else:
        df = dd.read_csv(file, sep=';', encoding="latin1", assume_missing=True,
                        dtype={'CBO Ocupação 2002': 'object',
                                'Faixa Remun Dezem (SM)': 'object',
                                'Faixa Tempo Emprego': 'object',
                                'Faixa Etária': 'object',
                                'Bairros SP': 'object',
                                'Distritos SP': 'object',
                                'Bairros RJ': 'object',
                                'Raça Cor': 'object',
                                'Natureza Jurídica': 'object',
                                'CNAE 2.0 Subclasse': 'object',
                                'Tipo Defic': 'object'})
    df = df.loc[(df["CNAE 2.0 Classe"] >= 62000) & (df["CNAE 2.0 Classe"] < 62050) ,:]
    computed_df = df.compute()
    computed_df.to_csv("../data/" + file[13:-3] + "csv", sep="\t", index=False)
    os.remove(file)


def obter_dados():
    for url in tqdm(urls):
        importa_filtra(url)

# Lembrar de setar '00000-1' como missing em CNAE 2.0 Subclasse

if __name__ == "__main__":
    print("Obtendo dados da RAIS...")
    obter_dados()
