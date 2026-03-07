
import sqlite3
from pathlib import Path
from datetime import datetime, date

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# =========================================================
# CONFIGURAÇÃO
# =========================================================
st.set_page_config(
    page_title="Controle de Reclamações de Cliente - V3",
    page_icon="📋",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "controle_reclamacoes_v3.db"
UPLOAD_DIR = BASE_DIR / "uploads_v3"
UPLOAD_DIR.mkdir(exist_ok=True)

ETAPAS = [
    "Contenção imediata",
    "Análise de causa",
    "Plano de ação",
    "Envio de evidências",
]

PRAZOS_DIAS = {
    "Contenção imediata": 1,
    "Análise de causa": 5,
    "Plano de ação": 10,
    "Envio de evidências": 15,
}

STATUS_GERAIS = [
    "Aberta",
    "Aguardando contenção",
    "Aguardando análise de causa",
    "Aguardando plano de ação",
    "Aguardando evidências",
    "Concluída",
]

HEADERS_IMPORTACAO_PADRAO = [
    "Código",
    "Título",
    "Data de emissão",
    "Responsável",
    "Cliente",
    "Descrição",
    "Quantidade não conforme",
]

# =========================================================
# BANCO DE DADOS
# =========================================================
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def column_exists(table_name: str, column_name: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]
    conn.close()
    return column_name in cols

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ocorrencias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE NOT NULL,
            data_abertura TEXT NOT NULL,
            cliente TEXT NOT NULL,
            titulo TEXT NOT NULL,
            descricao TEXT,
            quantidade REAL,
            responsavel_interno TEXT,
            status_geral TEXT DEFAULT 'Aberta',
            created_at TEXT,
            updated_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS retornos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ocorrencia_id INTEGER NOT NULL,
            etapa TEXT NOT NULL,
            data_envio TEXT,
            hora_envio TEXT,
            titulo_email TEXT,
            contato_cliente TEXT,
            email_cliente TEXT,
            responsavel_envio TEXT,
            descricao_retorno TEXT,
            comprovacao_envio TEXT,
            anexo_path TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY (ocorrencia_id) REFERENCES ocorrencias(id)
        )
    """)

    conn.commit()
    conn.close()

    if not column_exists("ocorrencias", "responsavel_interno"):
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("ALTER TABLE ocorrencias ADD COLUMN responsavel_interno TEXT")
        conn.commit()
        conn.close()

def run_select(sql, params=None):
    conn = get_conn()
    df = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return df

def run_exec(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    conn.commit()
    last_id = cur.lastrowid
    conn.close()
    return last_id

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

init_db()

# =========================================================
# APOIO
# =========================================================
def salvar_arquivo(uploaded_file, codigo, etapa):
    if uploaded_file is None:
        return None

    nome_codigo = str(codigo).replace("/", "_").replace("\\", "_").replace(" ", "_")
    nome_etapa = str(etapa).replace("/", "_").replace("\\", "_").replace(" ", "_")
    pasta = UPLOAD_DIR / nome_codigo / nome_etapa
    pasta.mkdir(parents=True, exist_ok=True)

    caminho = pasta / uploaded_file.name
    with open(caminho, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return str(caminho)

def listar_ocorrencias():
    return run_select("""
        SELECT *
        FROM ocorrencias
        ORDER BY date(data_abertura) DESC, codigo DESC
    """)

def buscar_ocorrencia_por_codigo(codigo):
    df = run_select("""
        SELECT *
        FROM ocorrencias
        WHERE codigo = ?
        LIMIT 1
    """, [codigo])
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def buscar_retorno(ocorrencia_id, etapa):
    df = run_select("""
        SELECT *
        FROM retornos
        WHERE ocorrencia_id = ? AND etapa = ?
        ORDER BY id DESC
        LIMIT 1
    """, [ocorrencia_id, etapa])
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def listar_retornos_por_ocorrencia(ocorrencia_id):
    return run_select("""
        SELECT *
        FROM retornos
        WHERE ocorrencia_id = ?
        ORDER BY id ASC
    """, [ocorrencia_id])

def atualizar_status_geral(ocorrencia_id):
    cont = buscar_retorno(ocorrencia_id, "Contenção imediata")
    analise = buscar_retorno(ocorrencia_id, "Análise de causa")
    plano = buscar_retorno(ocorrencia_id, "Plano de ação")
    evidencia = buscar_retorno(ocorrencia_id, "Envio de evidências")

    if evidencia:
        status = "Concluída"
    elif plano:
        status = "Aguardando evidências"
    elif analise:
        status = "Aguardando plano de ação"
    elif cont:
        status = "Aguardando análise de causa"
    else:
        status = "Aguardando contenção"

    run_exec("""
        UPDATE ocorrencias
        SET status_geral = ?, updated_at = ?
        WHERE id = ?
    """, [status, now_str(), ocorrencia_id])

def calcular_prazo_etapa(ocorrencia, etapa):
    data_abertura = pd.to_datetime(ocorrencia["data_abertura"], errors="coerce")
    if pd.isna(data_abertura):
        return None
    return (data_abertura + pd.Timedelta(days=PRAZOS_DIAS[etapa])).date()

def status_semaforo(ocorrencia, etapa):
    retorno = buscar_retorno(int(ocorrencia["id"]), etapa)
    prazo = calcular_prazo_etapa(ocorrencia, etapa)
    hoje = date.today()

    if retorno:
        data_envio = pd.to_datetime(retorno.get("data_envio"), errors="coerce")
        if pd.isna(data_envio):
            return "🟢 Enviado"
        return "🟢 No prazo" if data_envio.date() <= prazo else "🔴 Enviado em atraso"

    if prazo is None:
        return "⚪ Sem data"

    dias_restantes = (prazo - hoje).days
    if dias_restantes < 0:
        return "🔴 Atrasado"
    if dias_restantes <= 2:
        return "🟡 Próximo do prazo"
    return "⚪ Pendente"

def gerar_resumo_semaforo(ocorrencia):
    dados = []
    for etapa in ETAPAS:
        prazo = calcular_prazo_etapa(ocorrencia, etapa)
        retorno = buscar_retorno(int(ocorrencia["id"]), etapa)
        dados.append({
            "Etapa": etapa,
            "Prazo": "" if prazo is None else prazo.strftime("%d/%m/%Y"),
            "Status": status_semaforo(ocorrencia, etapa),
            "Enviado em": "" if not retorno else str(retorno.get("data_envio") or "")
        })
    return pd.DataFrame(dados)

def indicadores():
    ocorr = listar_ocorrencias()
    total = len(ocorr)
    if total == 0:
        return {
            "total": 0,
            "abertas": 0,
            "contencao_pendente": 0,
            "analise_pendente": 0,
            "plano_pendente": 0,
            "evidencia_pendente": 0,
            "concluidas": 0,
            "atrasadas": 0,
        }

    abertas = len(ocorr[ocorr["status_geral"].isin(["Aberta", "Aguardando contenção"])])
    concluidas = len(ocorr[ocorr["status_geral"] == "Concluída"])
    cont_pend = 0
    ana_pend = 0
    plano_pend = 0
    evid_pend = 0
    atrasadas = 0

    for _, row in ocorr.iterrows():
        oid = int(row["id"])
        if not buscar_retorno(oid, "Contenção imediata"):
            cont_pend += 1
        if not buscar_retorno(oid, "Análise de causa"):
            ana_pend += 1
        if not buscar_retorno(oid, "Plano de ação"):
            plano_pend += 1
        if not buscar_retorno(oid, "Envio de evidências"):
            evid_pend += 1
        if any("🔴" in status_semaforo(row.to_dict(), etapa) for etapa in ETAPAS):
            atrasadas += 1

    return {
        "total": total,
        "abertas": abertas,
        "contencao_pendente": cont_pend,
        "analise_pendente": ana_pend,
        "plano_pendente": plano_pend,
        "evidencia_pendente": evid_pend,
        "concluidas": concluidas,
        "atrasadas": atrasadas,
    }

def exportar_bases():
    ocorr_df = listar_ocorrencias()
    ret_df = run_select("""
        SELECT
            o.codigo,
            o.data_abertura,
            o.cliente,
            o.titulo,
            o.descricao,
            o.quantidade,
            o.responsavel_interno,
            o.status_geral,
            r.etapa,
            r.data_envio,
            r.hora_envio,
            r.titulo_email,
            r.contato_cliente,
            r.email_cliente,
            r.responsavel_envio,
            r.descricao_retorno,
            r.comprovacao_envio,
            r.anexo_path
        FROM ocorrencias o
        LEFT JOIN retornos r ON o.id = r.ocorrencia_id
        ORDER BY o.codigo, r.id
    """)
    return ocorr_df, ret_df

def normalizar_quantidade(valor):
    if pd.isna(valor):
        return 0.0
    texto = str(valor).strip()
    if texto == "":
        return 0.0
    texto = texto.replace(" ", "")
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto and "." not in texto:
        texto = texto.replace(",", ".")
    else:
        partes = texto.split(".")
        if len(partes) > 2:
            texto = "".join(partes)
    try:
        return float(texto)
    except Exception:
        return 0.0

def importar_ocorrencias_excel(arquivo_excel):
    try:
        df = pd.read_excel(arquivo_excel)
    except Exception as e:
        return {"ok": False, "mensagem": f"Erro ao ler o arquivo Excel: {e}", "incluidas": 0, "ignoradas": 0}

    df.columns = [str(c).strip() for c in df.columns]
    colunas = set(df.columns)

    obrigatorias = set(HEADERS_IMPORTACAO_PADRAO)
    faltantes = [c for c in HEADERS_IMPORTACAO_PADRAO if c not in colunas]
    if faltantes:
        return {
            "ok": False,
            "mensagem": "A planilha precisa seguir exatamente o cabeçalho padrão: " + ", ".join(HEADERS_IMPORTACAO_PADRAO),
            "incluidas": 0,
            "ignoradas": 0,
        }

    incluidas = 0
    ignoradas = 0

    for _, row in df.iterrows():
        codigo = str(row.get("Código", "")).strip()
        titulo = str(row.get("Título", "")).strip()
        cliente = str(row.get("Cliente", "")).strip()

        if not codigo or not titulo or not cliente or codigo.lower() == "nan" or titulo.lower() == "nan" or cliente.lower() == "nan":
            ignoradas += 1
            continue

        if buscar_ocorrencia_por_codigo(codigo):
            ignoradas += 1
            continue

        data_abertura = pd.to_datetime(row.get("Data de emissão"), errors="coerce")
        if pd.isna(data_abertura):
            data_abertura = pd.to_datetime(date.today())

        responsavel = row.get("Responsável", "")
        responsavel = "" if pd.isna(responsavel) else str(responsavel)

        descricao = row.get("Descrição", "")
        descricao = "" if pd.isna(descricao) else str(descricao)

        quantidade = normalizar_quantidade(row.get("Quantidade não conforme", 0))

        run_exec("""
            INSERT INTO ocorrencias (
                codigo, data_abertura, cliente, titulo, descricao,
                quantidade, responsavel_interno, status_geral, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            codigo,
            data_abertura.date().isoformat(),
            cliente,
            titulo,
            descricao,
            quantidade,
            responsavel,
            "Aberta",
            now_str(),
            now_str()
        ])
        incluidas += 1

    return {
        "ok": True,
        "mensagem": "Importação concluída.",
        "incluidas": incluidas,
        "ignoradas": ignoradas,
    }

def excluir_ocorrencia_por_comando(codigo, comando):
    codigo = str(codigo).strip()
    comando_esperado = f"EXCLUIR {codigo}"
    if str(comando).strip() != comando_esperado:
        return False, f"Comando inválido. Digite exatamente: {comando_esperado}"

    ocorr = buscar_ocorrencia_por_codigo(codigo)
    if not ocorr:
        return False, "Ocorrência não encontrada."

    run_exec("DELETE FROM retornos WHERE ocorrencia_id = ?", [int(ocorr["id"])])
    run_exec("DELETE FROM ocorrencias WHERE id = ?", [int(ocorr["id"])])
    return True, "Ocorrência excluída com sucesso."

def montar_base_dashboard():
    ocorr = listar_ocorrencias()
    if ocorr.empty:
        return ocorr
    ocorr = ocorr.copy()
    ocorr["data_abertura"] = pd.to_datetime(ocorr["data_abertura"], errors="coerce")
    ocorr["mes_ref"] = ocorr["data_abertura"].dt.strftime("%m/%Y")
    ocorr["responsavel_interno"] = ocorr["responsavel_interno"].fillna("Não informado")
    return ocorr

# =========================================================
# ESTILO
# =========================================================
st.markdown("""
<style>
.block-container {
    padding-top: 1rem;
    padding-bottom: 1rem;
}
[data-testid="stMetricValue"] {
    font-size: 1.3rem;
}
.small-card {
    border: 1px solid #d9d9d9;
    border-radius: 12px;
    padding: 12px;
    background-color: #fafafa;
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# TOPO
# =========================================================
st.title("📋 Controle de Reclamações de Cliente - V3")
st.caption("Banco de dados próprio do app, importação padronizada por Excel, consulta por código, edição, exclusão por comando específico e dashboard.")

with st.sidebar:
    st.header("Filtros")
    termo = st.text_input("Buscar por código, cliente ou título")
    status_filtro = st.selectbox("Status geral", ["Todos"] + STATUS_GERAIS)
    responsavel_filtro = st.text_input("Filtrar responsável interno")
    st.markdown("---")
    st.write("**Fluxo sugerido**")
    st.write("1. Registrar ocorrência")
    st.write("2. Importar planilha padrão")
    st.write("3. Registrar retorno")
    st.write("4. Consultar e editar")
    st.write("5. Atualizar painel")

col_atualizar_1, col_atualizar_2 = st.columns([1, 5])
with col_atualizar_1:
    if st.button("🔄 Atualizar", use_container_width=True):
        st.rerun()
with col_atualizar_2:
    st.caption("Use este botão após incluir, editar, importar ou excluir uma ocorrência para recarregar os dados.")

abas = st.tabs([
    "PAINEL (GERAL)",
    "REGISTRAR OCORRÊNCIA",
    "EDITAR OCRRÊNCIA / STATUS",
    "CONSULTA OCORRÊNCIA",
    "DASHBORAD",
    "EXPORTAR"
])

# =========================================================
# ABA 1 - PAINEL
# =========================================================
with abas[0]:
    ind = indicadores()

    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    c1.metric("Total", ind["total"])
    c2.metric("Abertas", ind["abertas"])
    c3.metric("Pend. contenção", ind["contencao_pendente"])
    c4.metric("Pend. análise", ind["analise_pendente"])
    c5.metric("Pend. plano", ind["plano_pendente"])
    c6.metric("Concluídas", ind["concluidas"])
    c7.metric("Com atraso", ind["atrasadas"])

    st.subheader("Ocorrências registradas")

    with st.expander("Up load de planilha Excel", expanded=False):
        st.write("Importe ocorrências em lote para dentro do banco de dados do app.")
        st.write("O cabeçalho da planilha deve seguir exatamente esta estrutura:")
        st.code(" | ".join(HEADERS_IMPORTACAO_PADRAO))
        arquivo_excel = st.file_uploader(
            "Selecionar planilha Excel",
            type=["xlsx", "xls"],
            key="upload_excel_ocorrencias"
        )
        if arquivo_excel is not None and st.button("Importar planilha", key="btn_importar_planilha"):
            resultado = importar_ocorrencias_excel(arquivo_excel)
            if resultado["ok"]:
                st.success(
                    f'{resultado["mensagem"]} Incluídas: {resultado["incluidas"]}. Ignoradas: {resultado["ignoradas"]}.'
                )
                st.info("Os dados foram gravados no banco de dados do aplicativo.")
            else:
                st.error(resultado["mensagem"])

    df = listar_ocorrencias()

    if not df.empty:
        if termo:
            t = termo.lower()
            df = df[
                df["codigo"].astype(str).str.lower().str.contains(t, na=False) |
                df["cliente"].astype(str).str.lower().str.contains(t, na=False) |
                df["titulo"].astype(str).str.lower().str.contains(t, na=False)
            ]

        if status_filtro != "Todos":
            df = df[df["status_geral"] == status_filtro]

        if responsavel_filtro:
            rf = responsavel_filtro.lower()
            df = df[df["responsavel_interno"].astype(str).str.lower().str.contains(rf, na=False)]

        resumo_status = []
        for _, row in df.iterrows():
            resumo_status.append(status_semaforo(row.to_dict(), "Contenção imediata"))

        visao = df[[
            "codigo", "data_abertura", "cliente", "titulo", "quantidade",
            "responsavel_interno", "status_geral"
        ]].copy()
        visao["Semáforo contenção"] = resumo_status
        visao.columns = ["Código", "Data", "Cliente", "Título", "Quantidade", "Responsável", "Status geral", "Semáforo contenção"]
        st.dataframe(visao, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma ocorrência encontrada.")

# =========================================================
# ABA 2 - REGISTRAR OCORRÊNCIA
# =========================================================
with abas[1]:
    st.subheader("REGISTRAR OCORRÊNCIA")
    st.write("A ocorrência é criada diretamente no banco do app.")

    with st.form("form_ocorrencia", clear_on_submit=True):
        col1, col2 = st.columns(2)

        with col1:
            codigo = st.text_input("Código *", placeholder="Ex.: 25-0142")
            data_abertura = st.date_input("Data de emissão", value=date.today())
            cliente = st.text_input("Cliente *")
            responsavel_interno = st.text_input("Responsável")
        with col2:
            titulo = st.text_input("Título *", placeholder="Ex.: Vazamento em costura lateral")
            quantidade = st.number_input("Quantidade não conforme", min_value=0.0, step=1.0, value=0.0)

        descricao = st.text_area("Descrição", height=140)
        salvar_ocorrencia = st.form_submit_button("Salvar ocorrência")

        if salvar_ocorrencia:
            if not codigo or not cliente or not titulo:
                st.error("Preencha os campos obrigatórios: código, cliente e título.")
            else:
                try:
                    run_exec("""
                        INSERT INTO ocorrencias (
                            codigo, data_abertura, cliente, titulo, descricao,
                            quantidade, responsavel_interno, status_geral, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        codigo.strip(),
                        str(data_abertura),
                        cliente.strip(),
                        titulo.strip(),
                        descricao.strip(),
                        float(quantidade),
                        responsavel_interno.strip(),
                        "Aberta",
                        now_str(),
                        now_str()
                    ])
                    st.success("Ocorrência registrada com sucesso no banco de dados.")
                    st.info("Clique em Atualizar para refletir a inclusão no painel.")
                except sqlite3.IntegrityError:
                    st.error("Já existe uma ocorrência cadastrada com esse código.")

# =========================================================
# ABA 3 - EDITAR OCRRÊNCIA / STATUS
# =========================================================
with abas[2]:
    st.subheader("EDITAR OCRRÊNCIA / STATUS")
    st.write("Informe o código. O app busca os dados da ocorrência e grava o retorno na mesma base.")

    codigo_busca = st.text_input("Código da ocorrência", key="codigo_retorno")

    if codigo_busca:
        ocorr = buscar_ocorrencia_por_codigo(codigo_busca.strip())

        if not ocorr:
            st.warning("Código não encontrado no banco de dados.")
        else:
            caixa1, caixa2, caixa3 = st.columns(3)
            caixa1.markdown(f"**Código:** {ocorr['codigo']}")
            caixa1.markdown(f"**Cliente:** {ocorr['cliente']}")
            caixa1.markdown(f"**Responsável:** {ocorr.get('responsavel_interno') or ''}")
            caixa2.markdown(f"**Título:** {ocorr['titulo']}")
            caixa2.markdown(f"**Quantidade:** {ocorr['quantidade']}")
            caixa3.markdown(f"**Status geral:** {ocorr['status_geral']}")
            caixa3.markdown(f"**Data de emissão:** {ocorr['data_abertura']}")

            st.markdown('<div class="small-card">', unsafe_allow_html=True)
            st.write(f"**Descrição:** {ocorr['descricao']}")
            st.markdown('</div>', unsafe_allow_html=True)

            st.markdown("### Situação por etapa")
            st.dataframe(gerar_resumo_semaforo(ocorr), use_container_width=True, hide_index=True)

            etapa = st.selectbox("Etapa do retorno", ETAPAS)
            retorno_existente = buscar_retorno(int(ocorr["id"]), etapa)

            if retorno_existente:
                st.info("Já existe registro para esta etapa. Ao salvar novamente, os dados serão atualizados.")

            with st.form("form_retorno", clear_on_submit=False):
                a1, a2 = st.columns(2)

                with a1:
                    data_envio = st.date_input("Data do envio", value=date.today(), key=f"data_{etapa}")
                    hora_envio = st.text_input("Hora do envio", value=datetime.now().strftime("%H:%M"), key=f"hora_{etapa}")
                    responsavel_envio = st.text_input(
                        "Responsável pelo envio *",
                        value="" if not retorno_existente else str(retorno_existente.get("responsavel_envio") or ""),
                        key=f"resp_{etapa}"
                    )
                    contato_cliente = st.text_input(
                        "Pessoa responsável no cliente *",
                        value="" if not retorno_existente else str(retorno_existente.get("contato_cliente") or ""),
                        key=f"contato_{etapa}"
                    )

                with a2:
                    email_cliente = st.text_input(
                        "E-mail do cliente",
                        value="" if not retorno_existente else str(retorno_existente.get("email_cliente") or ""),
                        key=f"email_{etapa}"
                    )
                    titulo_email = st.text_input(
                        "Título do e-mail *",
                        value="" if not retorno_existente else str(retorno_existente.get("titulo_email") or ""),
                        key=f"titulo_{etapa}"
                    )
                    comprovacao_envio = st.text_input(
                        "Comprovação do envio",
                        value="" if not retorno_existente else str(retorno_existente.get("comprovacao_envio") or ""),
                        placeholder="Ex.: Print do e-mail enviado / protocolo / confirmação",
                        key=f"comp_{etapa}"
                    )

                descricao_retorno = st.text_area(
                    "Descrição do retorno / conteúdo enviado",
                    value="" if not retorno_existente else str(retorno_existente.get("descricao_retorno") or ""),
                    height=150,
                    key=f"desc_{etapa}"
                )

                arquivo = st.file_uploader("Anexar evidência", key=f"arquivo_{etapa}")
                salvar_retorno = st.form_submit_button("Salvar retorno")

                if salvar_retorno:
                    if not responsavel_envio or not contato_cliente or not titulo_email:
                        st.error("Preencha os campos obrigatórios: responsável pelo envio, pessoa responsável no cliente e título do e-mail.")
                    else:
                        anexo_path = None
                        if arquivo is not None:
                            anexo_path = salvar_arquivo(arquivo, ocorr["codigo"], etapa)
                        elif retorno_existente:
                            anexo_path = retorno_existente.get("anexo_path")

                        if retorno_existente:
                            run_exec("""
                                UPDATE retornos
                                SET data_envio = ?, hora_envio = ?, titulo_email = ?, contato_cliente = ?,
                                    email_cliente = ?, responsavel_envio = ?, descricao_retorno = ?,
                                    comprovacao_envio = ?, anexo_path = ?, updated_at = ?
                                WHERE id = ?
                            """, [
                                str(data_envio),
                                hora_envio,
                                titulo_email,
                                contato_cliente,
                                email_cliente,
                                responsavel_envio,
                                descricao_retorno,
                                comprovacao_envio,
                                anexo_path,
                                now_str(),
                                int(retorno_existente["id"])
                            ])
                        else:
                            run_exec("""
                                INSERT INTO retornos (
                                    ocorrencia_id, etapa, data_envio, hora_envio, titulo_email,
                                    contato_cliente, email_cliente, responsavel_envio,
                                    descricao_retorno, comprovacao_envio, anexo_path,
                                    created_at, updated_at
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, [
                                int(ocorr["id"]),
                                etapa,
                                str(data_envio),
                                hora_envio,
                                titulo_email,
                                contato_cliente,
                                email_cliente,
                                responsavel_envio,
                                descricao_retorno,
                                comprovacao_envio,
                                anexo_path,
                                now_str(),
                                now_str()
                            ])

                        atualizar_status_geral(int(ocorr["id"]))
                        st.success("Retorno registrado com sucesso no banco de dados da ocorrência.")
                        st.info("Clique em Atualizar para recarregar os dados.")

            st.markdown("---")
            st.markdown("### Exclusão controlada")
            st.write("A ocorrência só pode ser excluída digitando um comando específico. Isso evita perda acidental de dados.")
            comando = st.text_input(
                "Comando de exclusão",
                placeholder=f"EXCLUIR {ocorr['codigo']}",
                key="cmd_exclusao"
            )
            if st.button("Excluir ocorrência", key="btn_excluir_ocorrencia"):
                ok, msg = excluir_ocorrencia_por_comando(ocorr["codigo"], comando)
                if ok:
                    st.success(msg)
                    st.info("Clique em Atualizar para recarregar o painel.")
                else:
                    st.error(msg)

# =========================================================
# ABA 4 - CONSULTA OCORRÊNCIA
# =========================================================
with abas[3]:
    st.subheader("CONSULTA OCORRÊNCIA")
    st.write("Pesquise pelo código para visualizar a ocorrência e todo o histórico de retornos registrados.")

    codigo_consulta = st.text_input("Código para consulta", key="codigo_consulta")

    if codigo_consulta:
        ocorr = buscar_ocorrencia_por_codigo(codigo_consulta.strip())

        if not ocorr:
            st.warning("Ocorrência não encontrada.")
        else:
            i1, i2, i3 = st.columns(3)
            i1.write(f"**Código:** {ocorr['codigo']}")
            i1.write(f"**Data de emissão:** {ocorr['data_abertura']}")
            i2.write(f"**Cliente:** {ocorr['cliente']}")
            i2.write(f"**Quantidade não conforme:** {ocorr['quantidade']}")
            i3.write(f"**Título:** {ocorr['titulo']}")
            i3.write(f"**Status geral:** {ocorr['status_geral']}")
            st.write(f"**Responsável:** {ocorr.get('responsavel_interno') or ''}")
            st.write(f"**Descrição:** {ocorr['descricao']}")

            st.markdown("### Semáforo da ocorrência")
            st.dataframe(gerar_resumo_semaforo(ocorr), use_container_width=True, hide_index=True)

            st.markdown("### Histórico de retornos")
            retornos_df = listar_retornos_por_ocorrencia(int(ocorr["id"]))

            if retornos_df.empty:
                st.info("Nenhum retorno registrado para esta ocorrência.")
            else:
                for etapa in ETAPAS:
                    reg = retornos_df[retornos_df["etapa"] == etapa]
                    with st.expander(etapa, expanded=False):
                        if reg.empty:
                            st.warning("Etapa ainda não registrada.")
                        else:
                            linha = reg.iloc[-1]
                            r1, r2 = st.columns(2)
                            r1.write(f"**Data envio:** {linha['data_envio']}")
                            r1.write(f"**Hora envio:** {linha['hora_envio']}")
                            r1.write(f"**Responsável envio:** {linha['responsavel_envio']}")
                            r2.write(f"**Pessoa no cliente:** {linha['contato_cliente']}")
                            r2.write(f"**E-mail cliente:** {linha['email_cliente']}")
                            r2.write(f"**Título e-mail:** {linha['titulo_email']}")
                            st.write(f"**Comprovação do envio:** {linha['comprovacao_envio']}")
                            st.write(f"**Descrição do retorno:** {linha['descricao_retorno']}")
                            if linha["anexo_path"]:
                                st.write(f"**Arquivo anexo:** {linha['anexo_path']}")

# =========================================================
# ABA 5 - DASHBORAD
# =========================================================
with abas[4]:
    st.subheader("DASHBORAD")
    base = montar_base_dashboard()

    if base.empty:
        st.info("Não há dados para gerar dashboard.")
    else:
        d1, d2, d3 = st.columns(3)
        cliente_dash = d1.selectbox("Cliente", ["Todos"] + sorted(base["cliente"].dropna().astype(str).unique().tolist()))
        status_dash = d2.selectbox("Status", ["Todos"] + sorted(base["status_geral"].dropna().astype(str).unique().tolist()))
        responsavel_dash = d3.selectbox("Responsável", ["Todos"] + sorted(base["responsavel_interno"].dropna().astype(str).unique().tolist()))

        base_dash = base.copy()
        if cliente_dash != "Todos":
            base_dash = base_dash[base_dash["cliente"] == cliente_dash]
        if status_dash != "Todos":
            base_dash = base_dash[base_dash["status_geral"] == status_dash]
        if responsavel_dash != "Todos":
            base_dash = base_dash[base_dash["responsavel_interno"] == responsavel_dash]

        if base_dash.empty:
            st.warning("Nenhum registro para os filtros selecionados.")
        else:
            col_a, col_b = st.columns(2)

            with col_a:
                st.markdown("**Ocorrências por mês**")
                serie_mes = base_dash.groupby("mes_ref").size().sort_index()
                fig = plt.figure(figsize=(8, 4))
                ax = fig.add_subplot(111)
                ax.bar(serie_mes.index, serie_mes.values)
                ax.set_xlabel("Mês")
                ax.set_ylabel("Ocorrências")
                plt.xticks(rotation=45)
                plt.tight_layout()
                st.pyplot(fig)

            with col_b:
                st.markdown("**Ocorrências por cliente**")
                serie_cli = base_dash.groupby("cliente").size().sort_values(ascending=False).head(10)
                fig = plt.figure(figsize=(8, 4))
                ax = fig.add_subplot(111)
                ax.bar(serie_cli.index, serie_cli.values)
                ax.set_xlabel("Cliente")
                ax.set_ylabel("Ocorrências")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                st.pyplot(fig)

            col_c, col_d = st.columns(2)

            with col_c:
                st.markdown("**Ocorrências por status**")
                serie_status = base_dash.groupby("status_geral").size().sort_values(ascending=False)
                fig = plt.figure(figsize=(8, 4))
                ax = fig.add_subplot(111)
                ax.bar(serie_status.index, serie_status.values)
                ax.set_xlabel("Status")
                ax.set_ylabel("Quantidade")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                st.pyplot(fig)

            with col_d:
                st.markdown("**Ocorrências por responsável**")
                serie_resp = base_dash.groupby("responsavel_interno").size().sort_values(ascending=False).head(10)
                fig = plt.figure(figsize=(8, 4))
                ax = fig.add_subplot(111)
                ax.bar(serie_resp.index, serie_resp.values)
                ax.set_xlabel("Responsável")
                ax.set_ylabel("Quantidade")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                st.pyplot(fig)

# =========================================================
# ABA 6 - EXPORTAR
# =========================================================
with abas[5]:
    st.subheader("EXPORTAR")
    st.write("Exporta as ocorrências e os retornos registrados no app para um arquivo Excel.")

    if st.button("Gerar Excel de exportação"):
        ocorr_df, ret_df = exportar_bases()
        arquivo_saida = BASE_DIR / "exportacao_controle_reclamacoes_v3.xlsx"

        with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
            ocorr_df.to_excel(writer, sheet_name="Ocorrencias", index=False)
            ret_df.to_excel(writer, sheet_name="Retornos", index=False)

        with open(arquivo_saida, "rb") as f:
            st.download_button(
                label="Baixar Excel",
                data=f,
                file_name="exportacao_controle_reclamacoes_v3.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

st.caption("Versão 3: importação com cabeçalho padrão, gravação em banco de dados interno do app e exclusão apenas por comando específico.")
