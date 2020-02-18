from utils import query_yes_no, PERGUNTA
import glob
import pandas as pd

APAGA = query_yes_no(PERGUNTA)

def limpa_dados():
    arquivos = glob.glob("../data/*.csv")
    pd.read_csv(arquivos[0], sep='\t')



if __name__ == "__main__":
    limpa_dados()

    