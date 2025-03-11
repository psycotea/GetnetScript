import pandas as pd
import os
from datetime import datetime
import unicodedata

def consolidar_vendas_csv(pasta_vendas, arquivo_mapeamento):
    dados_consolidados = []
    mapeamento_df = pd.read_csv(arquivo_mapeamento, sep=';', encoding='utf-8')
    mapeamento_df = mapeamento_df.rename(columns={'código da maquininha': 'Número do Terminal'})
    mapeamento_df['Número do Terminal'] = mapeamento_df['Número do Terminal'].astype(str).str.strip()

    for nome_arquivo in os.listdir(pasta_vendas):
        if nome_arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(pasta_vendas, nome_arquivo)
            df = pd.read_csv(caminho_arquivo, sep=';', encoding='utf-8', skiprows=7)
            df = df.rename(columns={
                df.columns[3]: 'Data/Hora da Venda',
                df.columns[9]: 'Número do Terminal',
                df.columns[15]: 'Valor Bruto',
                df.columns[19]: 'Valor Líquido',
                df.columns[11]: 'Descrição do Lançamento',
                df.columns[14]: 'Total de Parcelas',
                df.columns[5]: 'Número do Cartão'
            })
            df['Número do Terminal'] = df['Número do Terminal'].astype(str).str.strip()
            df_filtrado = df[['Data/Hora da Venda', 'Número do Terminal', 'Valor Bruto', 'Valor Líquido', 'Descrição do Lançamento', 'Total de Parcelas','Número do Cartão']].copy()

            df_filtrado = pd.merge(df_filtrado, mapeamento_df, on='Número do Terminal', how='left')

            df_filtrado['Valor Bruto'] = df_filtrado['Valor Bruto'].str.replace('R$ ', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
            df_filtrado['Data/Hora da Venda'] = pd.to_datetime(df_filtrado['Data/Hora da Venda'], dayfirst=True)
            df_filtrado['Valor Líquido'] = df_filtrado['Valor Líquido'].str.replace('R$ ', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

            def calcular_liquido(row):
                lancamento = row['Descrição do Lançamento'].upper().strip()
                valor_bruto = row['Valor Bruto']
                if lancamento.startswith('PARCELADO EMISSOR'):
                    taxa = row['crédito']
                    return valor_bruto - (valor_bruto * taxa)
                elif 'PARCELADO LOJISTA 2X' == lancamento:
                    taxa = row['taxa_credito_parcelado_2']
                elif 'PARCELADO LOJISTA 3X' == lancamento:
                    taxa = row['taxa_credito_parcelado_3']
                elif 'PARCELADO LOJISTA 4X' == lancamento:
                    taxa = row['taxa_credito_parcelado_4']
                elif 'PARCELADO LOJISTA 5X' == lancamento:
                    taxa = row['taxa_credito_parcelado_5']
                elif 'PARCELADO LOJISTA 6X' == lancamento:
                    taxa = row['taxa_credito_parcelado_6']
                elif 'PARCELADO LOJISTA 7X' == lancamento:
                    taxa = row['taxa_credito_parcelado_7']
                elif 'PARCELADO LOJISTA 8X' == lancamento:
                    taxa = row['taxa_credito_parcelado_8']
                elif 'PARCELADO LOJISTA 9X' == lancamento:
                    taxa = row['taxa_credito_parcelado_9']
                elif 'PARCELADO LOJISTA 10X' == lancamento:
                    taxa = row['taxa_credito_parcelado_10']
                elif 'PARCELADO LOJISTA 11X' == lancamento:
                    taxa = row['taxa_credito_parcelado_11']
                elif 'PARCELADO LOJISTA 12X' == lancamento:
                    taxa = row['taxa_credito_parcelado_12']
                elif 'VENDA CREDITO A VISTA' == lancamento:
                    taxa = row['crédito']
                elif 'VENDA DEBITO A VISTA' == lancamento:
                    taxa = row['débito']
                elif 'CANCELAMENTO CREDITO A VISTA' == lancamento:
                    taxa = row['crédito']
                    return valor_bruto - (valor_bruto * taxa)
                elif 'CANCELAMENTO DEBITO A VISTA' == lancamento:
                    taxa = row['débito']
                    return valor_bruto - (valor_bruto * taxa)
                elif 'CANCELAMENTO PARCELADO LOJISTA' == lancamento:
                    parcelas = row['Total de Parcelas']
                    if parcelas == 2:
                        taxa = row['taxa_credito_parcelado_2']
                    elif parcelas == 3:
                        taxa = row['taxa_credito_parcelado_3']
                    elif parcelas == 4:
                        taxa = row['taxa_credito_parcelado_4']
                    elif parcelas == 5:
                        taxa = row['taxa_credito_parcelado_5']
                    elif parcelas == 6:
                        taxa = row['taxa_credito_parcelado_6']
                    elif parcelas == 7:
                        taxa = row['taxa_credito_parcelado_7']
                    elif parcelas == 8:
                        taxa = row['taxa_credito_parcelado_8']
                    elif parcelas == 9:
                        taxa = row['taxa_credito_parcelado_9']
                    elif parcelas == 10:
                        taxa = row['taxa_credito_parcelado_10']
                    elif parcelas == 11:
                        taxa = row['taxa_credito_parcelado_11']
                    elif parcelas == 12:
                        taxa = row['taxa_credito_parcelado_12']
                    return valor_bruto - (valor_bruto * taxa)
                else:
                    return valor_bruto
                return valor_bruto - (valor_bruto * taxa)

            df_filtrado['Valor líquido calculado'] = df_filtrado.apply(calcular_liquido, axis=1)
            df_filtrado['Hora da Venda'] = df_filtrado['Data/Hora da Venda'].dt.strftime('%H:%M:%S') # Adicionando a coluna de hora
            df_filtrado['Data/Hora da Venda'] = df_filtrado['Data/Hora da Venda'].dt.strftime('%d/%m/%Y')
            df_filtrado = df_filtrado.drop_duplicates()
            dados_consolidados.append(df_filtrado)

    df_final = pd.concat(dados_consolidados, ignore_index=True)
    df_final = df_final.drop_duplicates()

    # Formatar os valores para string com vírgula como separador decimal
    df_final['Valor Bruto'] = df_final['Valor Bruto'].apply(lambda x: '{:,.2f}'.format(x).replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.'))
    df_final['Valor líquido calculado'] = df_final['Valor líquido calculado'].apply(lambda x: '{:,.2f}'.format(x).replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.'))
    df_final['Valor Líquido'] = df_final['Valor Líquido'].apply(lambda x: '{:,.2f}'.format(x).replace(',', 'TEMP').replace('.', ',').replace('TEMP', '.'))

    # Selecionar e ordenar as colunas desejadas
    df_final = df_final[['Data/Hora da Venda', 'Hora da Venda','Número do Terminal', 'nome_da_maquininha', 'grupo', 'Valor Bruto', 'Valor Líquido', 'Valor líquido calculado', 'Descrição do Lançamento', 'Total de Parcelas','Número do Cartão']]

    return df_final

pasta_vendas = 'pasta_vendas'
arquivo_mapeamento = 'mapeamento_maquininhas.csv'
df_vendas = consolidar_vendas_csv(pasta_vendas, arquivo_mapeamento)

now = datetime.now()
data_hora_atual = now.strftime("%Y%m%d_%H%M%S")
nome_arquivo_saida = f'vendas_consolidadas_getnet_{data_hora_atual}.csv'

# Função para remover acentos
def remover_acentos(texto):
    if isinstance(texto, str):
        texto_sem_acentos = unicodedata.normalize('NFD', texto)
        texto_sem_acentos = ''.join([c for c in texto_sem_acentos if not unicodedata.combining(c)])
        return texto_sem_acentos
    return texto

for coluna in df_vendas.columns:
    if df_vendas[coluna].dtype == 'object':
        df_vendas[coluna] = df_vendas[coluna].apply(remover_acentos)

# Modificar o texto na coluna 'Descrição do Lançamento'
def simplificar_lancamento(texto):
    if isinstance(texto, str):
        texto = texto.upper().strip()
        if 'VENDA CREDITO A VISTA' in texto:
            return 'CREDITO A VISTA'
        elif 'VENDA DEBITO A VISTA' in texto:
            return 'DEBITO A VISTA'
        elif 'PARCELADO EMISSOR' in texto:
            # Extrair o número de parcelas
            partes = texto.split()
            if len(partes) >= 3 and 'X' in partes[-1]:
                return f'EMISSOR {partes[-1]}'
            return 'EMISSOR'
        elif 'PARCELADO LOJISTA' in texto:
            # Extrair o número de parcelas
            partes = texto.split()
            if len(partes) >= 3 and 'X' in partes[-1]:
                return f'PARCELADO {partes[-1]}'
            return 'PARCELADO'
        return texto
    return texto

# Aplicar a função de simplificação na coluna de lançamentos
df_vendas['Descrição do Lançamento'] = df_vendas['Descrição do Lançamento'].apply(simplificar_lancamento)

df_vendas = df_vendas.rename(columns={
    'Data/Hora da Venda': 'Data',
    'Hora da Venda': 'Hora',
    'Número do Terminal': 'Numero Terminal',
    'Valor Líquido': 'Valor liquido',
    'Valor líquido calculado': 'valor liquido calculado',
    'Descrição do Lançamento': 'lancamento',
    'Total de Parcelas': 'parcelas',
    'Número do Cartão': 'numero cartao'
})

df_vendas.to_csv(nome_arquivo_saida, index=False, sep=';')
print(f'Dados consolidados e salvos em {nome_arquivo_saida}')