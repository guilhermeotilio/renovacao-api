import pandas as pd
import numpy as np
import io
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ─── HEALTH CHECK ───────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


# ─── ENDPOINT PRINCIPAL ─────────────────────────────────────
@app.route("/processar", methods=["POST"])
def processar():

    # ✏️ MUDANÇA 1: fator_selic vem do formulário web (antes era input())
    fator_input = request.form.get("fator_selic", "")
    try:
        fator_selic_input = float(fator_input)
    except ValueError:
        return jsonify({"erro": "Fator Selic inválido. Envie um número decimal, ex: 1.15"}), 400

    # ✏️ MUDANÇA 2: arquivos vem do upload web (antes eram caminhos fixos no HD)
    if "arquivo_renovacao" not in request.files or "arquivo_apolices" not in request.files:
        return jsonify({"erro": "Envie os arquivos: arquivo_renovacao e arquivo_apolices"}), 400

    arquivo_renovacao = request.files["arquivo_renovacao"]
    arquivo_apolices  = request.files["arquivo_apolices"]

    # ============================================================
    # A PARTIR DAQUI: SEU CÓDIGO ORIGINAL, SEM NENHUMA ALTERAÇÃO
    # ============================================================

    print("🔍 Buscando dados da Taxa Selic Diária via API do Banco Central...")

    CODIGO_SERIE = 11
    data_corrente = pd.to_datetime(datetime.now().date())
    data_inicial_str = "01/01/2021"
    data_final_str = data_corrente.strftime("%d/%m/%Y")

    url_api = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{CODIGO_SERIE}/dados"
        f"?formato=csv&dataInicial={data_inicial_str}&dataFinal={data_final_str}"
    )

    # === 1. BAIXA OS DADOS SELIC ===
    try:
        response = requests.get(url_api, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return jsonify({"erro": f"Erro ao baixar a taxa SELIC: {e}"}), 502

    # === 3. LÊ E TRATA PLANILHA SELIC ===
    df_selic = pd.read_csv(io.StringIO(response.content.decode("utf-8")), sep=";", decimal=",", skiprows=1)
    df_selic.columns = [c.strip() for c in df_selic.columns]
    df_selic = df_selic.rename(columns={
        df_selic.columns[0]: "Data",
        df_selic.columns[1]: "Taxa_diaria"
    })
    df_selic = df_selic.dropna(subset=["Data", "Taxa_diaria"])
    df_selic["Data"] = pd.to_datetime(df_selic["Data"].astype(str).str.strip(), dayfirst=True, errors="coerce")
    df_selic["Taxa_diaria"] = pd.to_numeric(df_selic["Taxa_diaria"], errors="coerce")
    df_selic = df_selic.dropna(subset=["Data", "Taxa_diaria"]).sort_values("Data").reset_index(drop=True)
    df_selic["Fator_diario"] = 1 + (df_selic["Taxa_diaria"] / 100)

    # === 4. LÊ PLANILHA DE CORREÇÃO MONETÁRIA (RENOVACAO) ===
    try:
        df_periodos = pd.read_excel(arquivo_renovacao)
    except Exception as e:
        return jsonify({"erro": f"Erro ao ler renovacao.xlsx: {e}"}), 400

    col_apolice        = "NumeroApolice"
    col_inicio         = "DataInicioVigencia"
    col_fim            = "DataFimVigencia"
    col_valor_garantia = "ValorLimiteGarantia"
    col_cobertura      = 'Cobertura'

    df_periodos[col_inicio]         = pd.to_datetime(df_periodos[col_inicio], errors="coerce")
    df_periodos[col_fim]            = pd.to_datetime(df_periodos[col_fim], errors="coerce")
    df_periodos[col_valor_garantia] = pd.to_numeric(df_periodos[col_valor_garantia], errors='coerce')

    # === 5. CÁLCULO: ANIVERSÁRIO, DIAS ATÉ E FATOR ACUMULADO ===
    df_periodos["AniversarioApolice"] = df_periodos[col_fim]
    df_periodos["Diasate"] = (df_periodos["AniversarioApolice"] - data_corrente).dt.days

    fatores = []
    data_min_selic = df_selic["Data"].min()

    for _, row in df_periodos.iterrows():
        data_ini = row[col_inicio]
        data_fim_filtro = data_corrente

        if pd.isna(data_ini) or data_ini > data_fim_filtro:
            fatores.append(np.nan)
            continue

        if data_ini < data_min_selic:
            pass

        mask = (df_selic["Data"] >= data_ini) & (df_selic["Data"] <= data_fim_filtro)
        fator_acumulado = df_selic.loc[mask, "Fator_diario"].prod()

        if fator_acumulado is None or fator_acumulado == 0 or np.isnan(fator_acumulado):
            fatores.append(np.nan)
        else:
            fatores.append(fator_acumulado)

    df_periodos["FatorAcumulado"] = fatores

    # === 6. CÁLCULO: FATOR2, FATORFINAL, ISAjustado E DiferencaIS ===
    expoente = df_periodos["Diasate"] / 365
    df_periodos["Fator2"]     = fator_selic_input ** expoente
    df_periodos["FatorFinal"] = df_periodos["FatorAcumulado"] * df_periodos["Fator2"]

    df_periodos["ISAjustado"] = df_periodos[col_valor_garantia] * df_periodos["FatorFinal"]
    print("✅ Cálculo Padrão de ISAjustado aplicado (ValorLimiteGarantia * FatorFinal).")

    if col_cobertura in df_periodos.columns:
        condicao_aduanas = df_periodos[col_cobertura] == "Aduaneiro - Admissão Temporária"
        if condicao_aduanas.any():
            df_periodos.loc[condicao_aduanas, "ISAjustado"] = df_periodos[col_valor_garantia]
            print("✅ REGRA CONDICIONAL aplicada: ISAjustado = ValorLimiteGarantia para 'Aduaneiro - Admissão Temporária'.")
        else:
            print("✅ REGRA CONDICIONAL checada. Nenhuma apólice 'Aduaneiro - Admissão Temporária' encontrada.")
    else:
        print(f"⚠️ Aviso: Coluna '{col_cobertura}' não encontrada. A regra condicional Aduaneiro (ISAjustado) não pôde ser aplicada.")

    df_periodos["DiferencaIS"] = df_periodos["ISAjustado"] - df_periodos[col_valor_garantia]

    # === 7. ADIÇÃO DE COLUNAS DE PRÊMIO E PRAZO ===
    col_chave_apolice_ref = 'N° da Apólice'
    col_valor_taxa        = 'Taxa Aplicada'
    col_data_ini_apolice  = 'Data de Começo de Vigência'
    col_data_fim_apolice  = 'Data de Final de Vigência'

    REQUIRED_CORE_COLS = [col_chave_apolice_ref, col_valor_taxa, col_data_ini_apolice, col_data_fim_apolice]

    try:
        print(f"\n🔗 Lendo planilha de apólices...")
        df_apolices = pd.read_excel(arquivo_apolices)

        missing_required_cols = [c for c in REQUIRED_CORE_COLS if c not in df_apolices.columns]

        if missing_required_cols:
            print(f"❌ Colunas ESSENCIAIS não encontradas: {', '.join(missing_required_cols)}.")
            df_periodos["Taxa"]         = np.nan
            df_periodos["PrazoApolice"] = np.nan
        else:
            cols_to_merge = REQUIRED_CORE_COLS[:]

            df_periodos = pd.merge(
                df_periodos,
                df_apolices[cols_to_merge],
                left_on=col_apolice,
                right_on=col_chave_apolice_ref,
                how='left'
            )

            df_periodos[col_data_ini_apolice] = pd.to_datetime(df_periodos[col_data_ini_apolice], errors="coerce")
            df_periodos[col_data_fim_apolice] = pd.to_datetime(df_periodos[col_data_fim_apolice], errors="coerce")

            df_periodos = df_periodos.rename(columns={col_valor_taxa: "Taxa"})
            df_periodos = df_periodos.drop(columns=[col_chave_apolice_ref], errors='ignore')

            prazo_calculado = (df_periodos[col_data_fim_apolice] - df_periodos[col_data_ini_apolice]).dt.days
            df_periodos["PrazoApolice"] = prazo_calculado
            print("✅ Colunas 'Taxa' e 'PrazoApolice' adicionadas/calculadas com sucesso.")

            if col_cobertura in df_periodos.columns:
                condicao_aduanas_prazo = df_periodos[col_cobertura] == "Aduaneiro - Admissão Temporária"
                if condicao_aduanas_prazo.any():
                    prazo_aduanas = (df_periodos[col_fim] - df_periodos[col_inicio]).dt.days
                    df_periodos.loc[condicao_aduanas_prazo, "PrazoApolice"] = prazo_aduanas
                    print("✅ REGRA CONDICIONAL aplicada: PrazoApolice recalculado para 'Aduaneiro - Admissão Temporária'.")

                condicao_execucao_fiscal = df_periodos[col_cobertura] == "Judicial para Execução Fiscal"
                if condicao_execucao_fiscal.any():
                    df_periodos.loc[condicao_execucao_fiscal, "PrazoApolice"] = 1825
                    print("✅ REGRA CONDICIONAL aplicada: PrazoApolice = 1825 para 'Judicial para Execução Fiscal'.")
            else:
                print(f"⚠️ Aviso: Coluna '{col_cobertura}' não encontrada. Regras condicionais de prazo ignoradas.")

    except Exception as e:
        print(f"❌ Erro inesperado ao processar apólices: {e}.")
        df_periodos["Taxa"]         = np.nan
        df_periodos["PrazoApolice"] = np.nan

    df_periodos["AniversarioFim"] = df_periodos[col_fim] + pd.to_timedelta(df_periodos["PrazoApolice"], unit='D')
    print("✅ Coluna 'AniversarioFim' definida como DataFimVigencia + PrazoApolice.")

    df_periodos["PremioFinal"] = (
        df_periodos["ISAjustado"] * df_periodos["PrazoApolice"] * df_periodos["Taxa"] / 365
    )
    print("✅ Coluna 'PremioFinal' calculada: ISAjustado * PrazoApolice * Taxa / 365.")

    # Remove colunas temporárias
    cols_to_drop = [col_data_ini_apolice, col_data_fim_apolice]
    df_periodos = df_periodos.drop(columns=cols_to_drop, errors='ignore')

    # ✏️ MUDANÇA 2 (continuação): retorna o arquivo para download em vez de salvar no HD
    output = io.BytesIO()
    df_periodos.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)

    return send_file(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name="renovacao_atualizada.xlsx"
    )


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=False, host="0.0.0.0", port=port)
