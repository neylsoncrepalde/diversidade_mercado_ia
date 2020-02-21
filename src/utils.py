import sys
import requests
import json
import pandas as pd
import numpy as np
from io import StringIO

PERGUNTA = "Você gostaria de deletar os dados não processados da RAIS do seu computador?"

def query_yes_no(question, default="no"):
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

            
def deflate(nominal_values, nominal_dates, real_date, index='ipca'):
    nominal_values = np.array(nominal_values)
    if index == 'ipca':
        res = requests.get("http://ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='PRECOS12_IPCA12')")
        indice = pd.DataFrame.from_dict(json.load(StringIO(res.text))['value'])
        indice['VALDATA'] = pd.to_datetime(indice['VALDATA'], utc=True).dt.date.astype(str)
        
    # Calculate changes in prices
    indice['indx'] = indice.VALVALOR[indice.VALDATA == real_date].values / indice.VALVALOR.values
    
    return (indice.loc[indice.VALDATA.isin(nominal_dates),'indx'] * nominal_values).values