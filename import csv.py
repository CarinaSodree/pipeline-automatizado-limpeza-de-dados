import csv
import os

def carregar_dados(caminho_arquivo):
    """Carrega os dados a partir de um arquivo CSV e retorna uma lista de listas."""
    if not os.path.exists(caminho_arquivo):
        print(f"Arquivo '{caminho_arquivo}' não encontrado. Criando arquivo fictício...")
        with open(caminho_arquivo, mode='w', newline='', encoding='utf-8') as f:
            escritor = csv.writer(f)
            escritor.writerow(["Nome", "Idade", "Cidade"])
            escritor.writerows([
                ["Ana", "25", "São Paulo"],
                ["Bruno", "30", "Rio de Janeiro"],
                ["Carlos", "", "Belo Horizonte"],  # Teste para valores ausentes
                ["Ana", "25", "São Paulo"]  # Teste para duplicatas
            ])
    
    with open(caminho_arquivo, newline='', encoding='utf-8') as f:
        return [row for row in csv.reader(f)]

def tratar_valores_ausentes(dados):
    """Substitui valores ausentes (células vazias) por 'N/A'."""
    for linha in dados[1:]:  # Ignora o cabeçalho
        for i, valor in enumerate(linha):
            if valor.strip() == "":
                linha[i] = "N/A"
    return dados

def remover_duplicatas(dados):
    """Remove linhas duplicadas do conjunto de dados."""
    nova_lista = []
    for linha in dados:
        if linha not in nova_lista:
            nova_lista.append(linha)
    return nova_lista

def padronizar_texto(dados, colunas_texto):
    """Converte os textos das colunas especificadas para minúsculas e remove espaços extras."""
    for linha in dados[1:]:  # Ignora o cabeçalho
        for coluna in colunas_texto:
            if coluna < len(linha):  # Garante que o índice existe
                linha[coluna] = linha[coluna].strip().lower()
    return dados

def salvar_dados(dados, caminho_saida):
    """Salva os dados processados em um novo arquivo CSV."""
    with open(caminho_saida, mode='w', newline='', encoding='utf-8') as f:
        escritor = csv.writer(f)
        escritor.writerows(dados)

def pipeline_limpeza(caminho_entrada, caminho_saida, colunas_texto):
    """Executa todas as etapas do pipeline de limpeza de dados de forma organizada."""
    
    print("1. Carregando dados...")
    dados = carregar_dados(caminho_entrada)
    
    print("2. Tratando valores ausentes...")
    dados = tratar_valores_ausentes(dados)
    
    print("3. Removendo duplicatas...")
    dados = remover_duplicatas(dados)
    
    print("4. Padronizando texto...")
    dados = padronizar_texto(dados, colunas_texto)
    
    print("5. Salvando dados processados...")
    salvar_dados(dados, caminho_saida)
    
    print(f"Dados limpos salvos em {caminho_saida}")

# Exemplo de uso
if __name__ == "__main__":
    caminho_entrada = "dados_sujos.csv"  # Arquivo de entrada
    caminho_saida = "dados_limpos.csv"  # Arquivo de saída
    colunas_texto = [0, 2]  # Índices das colunas de texto a serem padronizadas
    
    pipeline_limpeza(caminho_entrada, caminho_saida, colunas_texto)
