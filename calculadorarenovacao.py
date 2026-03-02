import pandas as pd
import numpy as np
import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta # Importação não utilizada, mas mantida do original

# === CONFIGURAÇÕES DE DIRETÓRIO E ARQUIVOS ===
# NOVO DIRETÓRIO BASE: C:\Users\guilherme.otilio\Desktop\Renovacao
# Todos os arquivos de saída (Selic e resultado final) serão salvos aqui.
base_dir = r"C:\Users\guilherme.otilio\Desktop\Renovacao"
os.makedirs(base_dir, exist_ok=True)

# Arquivos de saída
arquivo_selic_saida = os.path.join(base_dir, "taxa_selic_apurada.csv")
arquivo_saida = os.path.join(base_dir, "renovacao_atualizada.xlsx")

# SOLICITAÇÃO 1: Arquivo de entrada (agora dentro do novo base_dir)
arquivo_periodos = os.path.join(base_dir, "renovacao.xlsx")

# SOLICITAÇÃO 3: ARQUIVO DE REFERÊNCIA (Mantido como caminho externo)
arquivo_apolices = r"C:\Users\guilherme.otilio\desktop\apolicesemitidas.xlsx"

print("🔍 Buscando dados da Taxa Selic Diária via API do Banco Central...")

# Código da série temporal SELIC no SGS
CODIGO_SERIE = 11

# Obtém a data corrente
data_corrente = pd.to_datetime(datetime.now().date()) 

# Intervalo de datas: De 01/01/2021 até HOJE
data_inicial_str = "01/01/2021"
data_final_str = data_corrente.strftime("%d/%m/%Y") 

# URL da API do Banco Central
url_api = (
    f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{CODIGO_SERIE}/dados"
    f"?formato=csv&dataInicial={data_inicial_str}&dataFinal={data_final_str}"
)

# === 1. BAIXA OS DADOS SELIC ===
try:
    response = requests.get(url_api, timeout=15)
    response.raise_for_status()
    with open(arquivo_selic_saida, "wb") as f:
        f.write(response.content)
except requests.exceptions.RequestException as e:
    print(f"❌ Erro ao baixar a taxa SELIC: {e}")
    exit()

# === 2. ENTRADA DO FATOR SELIC ===
while True:
    fator_input = input("Por favor, insira o Fator Selic (ex: 1.15): ")
    try:
        fator_selic_input = float(fator_input)
        break
    except ValueError:
        print("Entrada inválida. Por favor, insira um número decimal.")
        
# === 3. LÊ E TRATA PLANILHA SELIC ===
df_selic = pd.read_csv(arquivo_selic_saida, sep=";", decimal=",", skiprows=1, encoding="utf-8")
df_selic.columns = [c.strip() for c in df_selic.columns]
df_selic = df_selic.rename(columns={
    df_selic.columns[0]: "Data",
    df_selic.columns[1]: "Taxa_diaria"
})
df_selic = df_selic.dropna(subset=["Data", "Taxa_diaria"])

df_selic["Data"] = pd.to_datetime(df_selic["Data"].astype(str).str.strip(), dayfirst=True, errors="coerce")
df_selic["Taxa_diaria"] = pd.to_numeric(df_selic["Taxa_diaria"], errors="coerce")
df_selic = df_selic.dropna(subset=["Data", "Taxa_diaria"]).sort_values("Data").reset_index(drop=True)

# Cálculo Fator Diário
df_selic["Fator_diario"] = 1 + (df_selic["Taxa_diaria"] / 100)

# === 4. LÊ PLANILHA DE CORREÇÃO MONETÁRIA (RENOVACAO) ===
# Adicionando tratamento de erro específico para FileNotFoundError
try:
    print(f"\n📑 Lendo planilha de períodos em: {arquivo_periodos}")
    df_periodos = pd.read_excel(arquivo_periodos)
except FileNotFoundError:
     print(f"❌ ERRO: Arquivo não encontrado! Verifique se o caminho abaixo está correto:")
     print(f"-> {arquivo_periodos}")
     print("O script será encerrado.")
     exit()
except Exception as e:
     print(f"❌ Erro ao ler a planilha de períodos: {e}")
     print("O script será encerrado.")
     exit()


# Colunas principais
col_apolice = "NumeroApolice"
col_inicio = "DataInicioVigencia"
col_fim = "DataFimVigencia"
col_valor_garantia = "ValorLimiteGarantia"
col_cobertura = 'Cobertura' # Coluna Cobertura é esperada aqui (em df_periodos)

# Converte as colunas de datas e valor
df_periodos[col_inicio] = pd.to_datetime(df_periodos[col_inicio], errors="coerce")
df_periodos[col_fim] = pd.to_datetime(df_periodos[col_fim], errors="coerce")
df_periodos[col_valor_garantia] = pd.to_numeric(df_periodos[col_valor_garantia], errors='coerce')


# ===============================================
# === 5. CÁLCULO: ANIVERSÁRIO, DIAS ATÉ E FATOR ACUMULADO ===
# ===============================================

# SOLICITAÇÃO 2: AniversarioApolice = DataFimVigencia
df_periodos["AniversarioApolice"] = df_periodos[col_fim] 

# Diferença de dias entre a Data de Aniversário (DataFimVigencia) e a data corrente
df_periodos["Diasate"] = (df_periodos["AniversarioApolice"] - data_corrente).dt.days

# Fator Acumulado
fatores = []
data_min_selic = df_selic["Data"].min()

for _, row in df_periodos.iterrows():
    data_ini = row[col_inicio]
    data_fim_filtro = data_corrente 

    if pd.isna(data_ini) or data_ini > data_fim_filtro:
        fatores.append(np.nan)
        continue
    
    if data_ini < data_min_selic:
        # Aviso mantido, mas não bloqueia
        pass 

    mask = (df_selic["Data"] >= data_ini) & (df_selic["Data"] <= data_fim_filtro)
    fator_acumulado = df_selic.loc[mask, "Fator_diario"].prod()
    
    if fator_acumulado is None or fator_acumulado == 0 or np.isnan(fator_acumulado):
        fatores.append(np.nan)
    else:
        fatores.append(fator_acumulado)

df_periodos["FatorAcumulado"] = fatores

# ===============================================
# === 6. CÁLCULO: FATOR2, FATORFINAL, ISAjustado E DiferencaIS ===
# ===============================================

expoente = df_periodos["Diasate"] / 365
df_periodos["Fator2"] = fator_selic_input ** expoente
df_periodos["FatorFinal"] = df_periodos["FatorAcumulado"] * df_periodos["Fator2"]

# 6.1. Cálculo Padrão de ISAjustado
df_periodos["ISAjustado"] = df_periodos[col_valor_garantia] * df_periodos["FatorFinal"]
print("✅ Cálculo Padrão de ISAjustado aplicado (ValorLimiteGarantia * FatorFinal).")

# 6.2. Aplica REGRA CONDICIONAL: Cobertura Aduaneiro - Admissão Temporária (ISAjustado)
if col_cobertura in df_periodos.columns:
    condicao_aduanas = df_periodos[col_cobertura] == "Aduaneiro - Admissão Temporária"
    
    if condicao_aduanas.any():
        # O valor ISAjustado será = ValorLimiteGarantia (sem correção monetária)
        df_periodos.loc[condicao_aduanas, "ISAjustado"] = df_periodos[col_valor_garantia]
        print("✅ REGRA CONDICIONAL aplicada: ISAjustado = ValorLimiteGarantia para 'Aduaneiro - Admissão Temporária'.")
    else:
        print("✅ REGRA CONDICIONAL checada. Nenhuma apólice 'Aduaneiro - Admissão Temporária' encontrada.")
else:
    print(f"⚠️ Aviso: Coluna '{col_cobertura}' não encontrada. A regra condicional Aduaneiro (ISAjustado) não pôde ser aplicada.")

# 6.3. Recálculo da DiferencaIS (Importante)
df_periodos["DiferencaIS"] = df_periodos["ISAjustado"] - df_periodos[col_valor_garantia]


# ===============================================
# === 7. ADIÇÃO DE COLUNAS DE PRÊMIO E PRAZO ===
# A coluna 'Cobertura' é assumida estar no arquivo de Renovação (df_periodos).
# ===============================================

col_chave_apolice_ref = 'N° da Apólice'
col_valor_taxa = 'Taxa Aplicada'
col_data_ini_apolice = 'Data de Começo de Vigência' 
col_data_fim_apolice = 'Data de Final de Vigência' 

# Colunas estritamente necessárias para o cálculo de PrazoApolice e Taxa
REQUIRED_CORE_COLS = [col_chave_apolice_ref, col_valor_taxa, col_data_ini_apolice, col_data_fim_apolice]


try:
    print(f"\n🔗 Lendo planilha de apólices e buscando dados de 'Taxa Aplicada' e Prazo em: {arquivo_apolices}")
    
    df_apolices = pd.read_excel(arquivo_apolices)
    
    # 7.1. Validação de colunas essenciais na planilha de apólices
    missing_required_cols = [c for c in REQUIRED_CORE_COLS if c not in df_apolices.columns]
    
    if missing_required_cols:
         print(f"❌ Erro: Colunas ESSENCIAIS para cálculo não encontradas em {arquivo_apolices}: {', '.join(missing_required_cols)}. Colunas dependentes serão NaN.")
         df_periodos["Taxa"] = np.nan
         df_periodos["PrazoApolice"] = np.nan
    else:
        # Se as colunas essenciais estiverem ok, prosseguir com merge e cálculos
        cols_to_merge = REQUIRED_CORE_COLS[:]

        # Realiza o MERGE (VLOOKUP) com as colunas necessárias de df_apolices
        df_periodos = pd.merge(
            df_periodos, 
            df_apolices[cols_to_merge],
            left_on=col_apolice, 
            right_on=col_chave_apolice_ref, 
            how='left'
        )
        
        # Converte datas após o merge
        df_periodos[col_data_ini_apolice] = pd.to_datetime(df_periodos[col_data_ini_apolice], errors="coerce")
        df_periodos[col_data_fim_apolice] = pd.to_datetime(df_periodos[col_data_fim_apolice], errors="coerce")
        
        # Renomeia coluna Taxa
        df_periodos = df_periodos.rename(columns={
            col_valor_taxa: "Taxa",
        })
        
        # Remove a coluna duplicada que serviu de chave
        df_periodos = df_periodos.drop(columns=[col_chave_apolice_ref], errors='ignore')
        
        # SOLICITAÇÃO 4: CALCULA O PRAZO DA APÓLICE (PrazoApolice) - CÁLCULO GERAL (BASEADO NO ARQUIVO DE APÓLICES)
        prazo_calculado = (df_periodos[col_data_fim_apolice] - df_periodos[col_data_ini_apolice]).dt.days
        df_periodos["PrazoApolice"] = prazo_calculado
        print("✅ Colunas 'Taxa' e 'PrazoApolice' (cálculo geral) adicionadas/calculadas com sucesso.")

        # === REGRAS CONDICIONAIS DE PRAZO (USANDO COBERTURA DO ARQUIVO RENOVACAO) ===
        if col_cobertura in df_periodos.columns:
            
            # NOVO: Regra Aduaneiro - Admissão Temporária
            condicao_aduanas_prazo = df_periodos[col_cobertura] == "Aduaneiro - Admissão Temporária"
            if condicao_aduanas_prazo.any():
                # O PrazoApolice será DataFimVigencia - DataInicioVigencia (colunas da planilha 'renovacao')
                prazo_aduanas = (df_periodos[col_fim] - df_periodos[col_inicio]).dt.days
                df_periodos.loc[condicao_aduanas_prazo, "PrazoApolice"] = prazo_aduanas
                print("✅ REGRA CONDICIONAL aplicada: PrazoApolice recalculado usando DataFimVigencia - DataInicioVigencia para 'Aduaneiro - Admissão Temporária'.")
            
            # SOLICITAÇÃO 6: Regra Execução Fiscal
            condicao_execucao_fiscal = df_periodos[col_cobertura] == "Judicial para Execução Fiscal"
            if condicao_execucao_fiscal.any():
                df_periodos.loc[condicao_execucao_fiscal, "PrazoApolice"] = 1825
                print("✅ REGRA CONDICIONAL aplicada: PrazoApolice definido como 1825 dias para 'Judicial para Execução Fiscal'.")
            
        else:
             print(f"⚠️ Aviso: Coluna '{col_cobertura}' não encontrada na planilha de renovação. As regras condicionais de prazo foram ignoradas.")
             
except FileNotFoundError:
    print(f"❌ Erro: Arquivo de apólices não encontrado em: {arquivo_apolices}. Colunas dependentes serão NaN.")
    df_periodos["Taxa"] = np.nan
    df_periodos["PrazoApolice"] = np.nan
except Exception as e:
    print(f"❌ Erro inesperado ao processar a planilha de apólices: {e}. Colunas dependentes serão NaN.")
    df_periodos["Taxa"] = np.nan
    df_periodos["PrazoApolice"] = np.nan


# SOLICITAÇÃO 5: AniversarioFim será = DataFimVigencia + prazoapolice (O resultado irá gerar uma data)
# Calcula a data de aniversário final adicionando o PrazoApolice (em dias) à DataFimVigencia.
df_periodos["AniversarioFim"] = df_periodos[col_fim] + pd.to_timedelta(df_periodos["PrazoApolice"], unit='D')
print("✅ Coluna 'AniversarioFim' definida como a data final de aniversário (DataFimVigencia + PrazoApolice).")


# SOLICITAÇÃO 7: CÁLCULO PREMIOFINAL COM NOVA FÓRMULA
# Nova fórmula (conforme correção): PremioFinal = ISAjustado * prazoapolice * Taxa / 365.
df_periodos["PremioFinal"] = (
    df_periodos["ISAjustado"] * df_periodos["PrazoApolice"] * df_periodos["Taxa"] / 365
)

print("✅ Coluna 'PremioFinal' calculada com a nova fórmula: ISAjustado * PrazoApolice * Taxa / 365.")


# ===============================================
# === 8. SALVA RESULTADO FINAL ===
# ===============================================

# Remove as colunas temporárias de datas de referência da apólice
cols_to_drop = [col_data_ini_apolice, col_data_fim_apolice]
df_periodos = df_periodos.drop(columns=cols_to_drop, errors='ignore')

df_periodos.to_excel(arquivo_saida, index=False)
print(f"\n✅ Processamento Finalizado! Planilha gerada em:\n{arquivo_saida}")
 