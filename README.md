# Um Panorama da Diversidade no Mercado de Inteligência Artificial no Brasil

## Prof. Dr. Neylson Crepalde

Estudo sobre a desigualdade de gênero no Brasil no mercado relacionado à Inteligência Artificial.

Para elaborar o estudo, utilizamos como fontes de dados a RAIS dos estados SP, MG, ES e RJ de 2008 a 2018.

# Como reproduzir o estudo

Primeiro, clone este repositório git em sua máquina:

```bash
git clone https://github.com/neylsoncrepalde/diversidade_mercado_ia.git
```


## 0 - Crie um ambiente virtual e instale as dependências

Vamos criar um ambiente `conda` que é simples de manusear. Se preferir, criar um ambiente usando `pip`, você pode usar [este tutorial.](https://uoa-eresearch.github.io/eresearch-cookbook/recipe/2014/11/26/python-virtual-env/). Para mais informações sobre `conda envs`, consulte [aqui.](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)

```bash
conda create -n diversidade python=3.7
conda activate diversidade
cd diversidade_mercado_ia
pip install -r requirements.txt
```

## 1 - Crie as pastas necessárias para armazenar os dados

```bash
mkdir data
mkdir data/raw
mkdir data/processed
```

## 2 - Obtendo os dados

```bash
cd src
python obter_dados.py
```

## 3 - Limpando e organizando os dados

```bash
python limpa_dados.py
```


## 4 - Análises

Abra no jupyter notebook ou o jupyter lab e comece as análises.

```bash
cd ..
jupyter lab
```
