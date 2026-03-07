
import sqlite3
from pathlib import Path
from datetime import datetime, date
import shutil

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# =========================================================
# CONFIGURAÇÃO
# =========================================================
st.set_page_config(
    page_title="Controle de Retorno de Reclamações de Cliente - CRRC ",
    page_icon="📋",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_app"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "controle_reclamacoes_v4.db"
UPLOAD_DIR = DATA_DIR / "uploads_v4"
BACKUP_DIR = DATA_DIR / "backups"
UPLOAD_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

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

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS log_exclusoes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_ocorrencia TEXT NOT NULL,
            titulo_ocorrencia TEXT,
            cliente TEXT,
            usuario_exclusao TEXT,
            motivo_exclusao TEXT,
            comando_digitado TEXT,
            deleted_at TEXT
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

init_db()

# =========================================================
# BACKUP
# =========================================================
def criar_backup_automatico(tag="manual"):
    if not DB_PATH.exists():
        return None
    nome = f"backup_{tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
    destino = BACKUP_DIR / nome
    shutil.copy2(DB_PATH, destino)
    return destino

def listar_backups():
    arquivos = sorted(BACKUP_DIR.glob("*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    dados = []
    for arq in arquivos:
        dados.append({
            "arquivo": arq.name,
            "caminho": str(arq),
            "data_modificacao": datetime.fromtimestamp(arq.stat().st_mtime).strftime("%d/%m/%Y %H:%M:%S"),
            "tamanho_kb": round(arq.stat().st_size / 1024, 2),
        })
    return pd.DataFrame(dados)

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
        return {"total": 0, "abertas": 0, "concluidas": 0, "atrasadas": 0}

    abertas = len(ocorr[ocorr["status_geral"].isin(["Aberta", "Aguardando contenção"])])
    concluidas = len(ocorr[ocorr["status_geral"] == "Concluída"])
    atrasadas = 0
    for _, row in ocorr.iterrows():
        if any("🔴" in status_semaforo(row.to_dict(), etapa) for etapa in ETAPAS):
            atrasadas += 1

    return {
        "total": total,
        "abertas": abertas,
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
    log_df = run_select("SELECT * FROM log_exclusoes ORDER BY id DESC")
    return ocorr_df, ret_df, log_df

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
    faltantes = [c for c in HEADERS_IMPORTACAO_PADRAO if c not in set(df.columns)]
    if faltantes:
        return {
            "ok": False,
            "mensagem": "A planilha precisa seguir exatamente o cabeçalho padrão: " + ", ".join(HEADERS_IMPORTACAO_PADRAO),
            "incluidas": 0,
            "ignoradas": 0,
        }

    incluidas = 0
    ignoradas = 0
    criar_backup_automatico("antes_importacao")

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

    criar_backup_automatico("apos_importacao")
    return {"ok": True, "mensagem": "Importação concluída.", "incluidas": incluidas, "ignoradas": ignoradas}

def excluir_ocorrencia_por_comando(codigo, comando, usuario, motivo):
    codigo = str(codigo).strip()
    comando_esperado = f"EXCLUIR {codigo}"
    if str(comando).strip() != comando_esperado:
        return False, f"Comando inválido. Digite exatamente: {comando_esperado}"

    ocorr = buscar_ocorrencia_por_codigo(codigo)
    if not ocorr:
        return False, "Ocorrência não encontrada."

    if not str(usuario).strip():
        return False, "Informe o usuário responsável pela exclusão."
    if not str(motivo).strip():
        return False, "Informe o motivo da exclusão."

    criar_backup_automatico(f"antes_exclusao_{codigo}")

    run_exec("""
        INSERT INTO log_exclusoes (
            codigo_ocorrencia, titulo_ocorrencia, cliente, usuario_exclusao,
            motivo_exclusao, comando_digitado, deleted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        ocorr["codigo"],
        ocorr["titulo"],
        ocorr["cliente"],
        str(usuario).strip(),
        str(motivo).strip(),
        str(comando).strip(),
        now_str(),
    ])

    run_exec("DELETE FROM retornos WHERE ocorrencia_id = ?", [int(ocorr["id"])])
    run_exec("DELETE FROM ocorrencias WHERE id = ?", [int(ocorr["id"])])
    criar_backup_automatico(f"apos_exclusao_{codigo}")
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
.block-container {padding-top: 1rem; padding-bottom: 1rem;}
[data-testid="stMetricValue"] {font-size: 1.3rem;}
.small-card {
    border: 1px solid #d9d9d9; border-radius: 12px; padding: 12px; background-color: #fafafa;
}
</style>
""", unsafe_allow_html=True)

# =========================================================
# TOPO
# =========================================================
st.title("📋 Controle de Reclamações de Cliente - V4")
st.caption("Banco de dados do app com backup automático, log de exclusão e opção de download dos backups.")

with st.sidebar:
    st.header("Filtros")
    termo = st.text_input("Buscar por código, cliente ou título")
    status_filtro = st.selectbox("Status geral", ["Todos"] + STATUS_GERAIS)
    responsavel_filtro = st.text_input("Filtrar responsável interno")
    st.markdown("---")
    st.write("**Cabeçalho padrão de importação**")
    st.code(" | ".join(HEADERS_IMPORTACAO_PADRAO))

col_atualizar_1, col_atualizar_2 = st.columns([1, 5])
with col_atualizar_1:
    if st.button("🔄 Atualizar", use_container_width=True):
        st.rerun()
with col_atualizar_2:
    st.caption("Use este botão após incluir, editar, importar, excluir ou gerar backup.")

abas = st.tabs([
    "PAINEL (GERAL)",
    "REGISTRAR OCORRÊNCIA",
    "EDITAR OCRRÊNCIA / STATUS",
    "CONSULTA OCORRÊNCIA",
    "DASHBORAD",
    "EXPORTAR / BACKUP"
])

# =========================================================
# ABA 1
# =========================================================
with abas[0]:
    ind = indicadores()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total", ind["total"])
    c2.metric("Abertas", ind["abertas"])
    c3.metric("Concluídas", ind["concluidas"])
    c4.metric("Com atraso", ind["atrasadas"])

    st.subheader("Ocorrências registradas")

    with st.expander("Up load de planilha Excel", expanded=False):
        st.write("Importe ocorrências em lote para dentro do banco de dados do app.")
        arquivo_excel = st.file_uploader("Selecionar planilha Excel", type=["xlsx", "xls"], key="upload_excel_ocorrencias")
        if arquivo_excel is not None and st.button("Importar planilha", key="btn_importar_planilha"):
            resultado = importar_ocorrencias_excel(arquivo_excel)
            if resultado["ok"]:
                st.success(f'{resultado["mensagem"]} Incluídas: {resultado["incluidas"]}. Ignoradas: {resultado["ignoradas"]}.')
                st.info("Os dados foram gravados no banco de dados do aplicativo e um backup foi gerado.")
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

        visao = df[["codigo", "data_abertura", "cliente", "titulo", "quantidade", "responsavel_interno", "status_geral"]].copy()
        visao.columns = ["Código", "Data", "Cliente", "Título", "Quantidade", "Responsável", "Status geral"]
        st.dataframe(visao, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma ocorrência encontrada.")

# =========================================================
# ABA 2
# =========================================================
with abas[1]:
    st.subheader("REGISTRAR OCORRÊNCIA")
    with st.form("form_ocorrencia", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            codigo = st.text_input("Código *")
            data_abertura = st.date_input("Data de emissão", value=date.today())
            cliente = st.text_input("Cliente *")
            responsavel_interno = st.text_input("Responsável")
        with col2:
            titulo = st.text_input("Título *")
            quantidade = st.number_input("Quantidade não conforme", min_value=0.0, step=1.0, value=0.0)
        descricao = st.text_area("Descrição", height=140)
        salvar_ocorrencia = st.form_submit_button("Salvar ocorrência")

        if salvar_ocorrencia:
            if not codigo or not cliente or not titulo:
                st.error("Preencha os campos obrigatórios: código, cliente e título.")
            else:
                try:
                    criar_backup_automatico("antes_cadastro")
                    run_exec("""
                        INSERT INTO ocorrencias (
                            codigo, data_abertura, cliente, titulo, descricao,
                            quantidade, responsavel_interno, status_geral, created_at, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        codigo.strip(), str(data_abertura), cliente.strip(), titulo.strip(),
                        descricao.strip(), float(quantidade), responsavel_interno.strip(),
                        "Aberta", now_str(), now_str()
                    ])
                    criar_backup_automatico("apos_cadastro")
                    st.success("Ocorrência registrada com sucesso no banco de dados.")
                except sqlite3.IntegrityError:
                    st.error("Já existe uma ocorrência cadastrada com esse código.")

# =========================================================
# ABA 3
# =========================================================
with abas[2]:
    st.subheader("EDITAR OCRRÊNCIA / STATUS")
    codigo_busca = st.text_input("Código da ocorrência", key="codigo_retorno")

    if codigo_busca:
        ocorr = buscar_ocorrencia_por_codigo(codigo_busca.strip())
        if not ocorr:
            st.warning("Código não encontrado no banco de dados.")
        else:
            st.dataframe(gerar_resumo_semaforo(ocorr), use_container_width=True, hide_index=True)

            etapa = st.selectbox("Etapa do retorno", ETAPAS)
            retorno_existente = buscar_retorno(int(ocorr["id"]), etapa)

            with st.form("form_retorno", clear_on_submit=False):
                a1, a2 = st.columns(2)
                with a1:
                    data_envio = st.date_input("Data do envio", value=date.today(), key=f"data_{etapa}")
                    hora_envio = st.text_input("Hora do envio", value=datetime.now().strftime("%H:%M"), key=f"hora_{etapa}")
                    responsavel_envio = st.text_input("Responsável pelo envio *", value="" if not retorno_existente else str(retorno_existente.get("responsavel_envio") or ""), key=f"resp_{etapa}")
                    contato_cliente = st.text_input("Pessoa responsável no cliente *", value="" if not retorno_existente else str(retorno_existente.get("contato_cliente") or ""), key=f"contato_{etapa}")
                with a2:
                    email_cliente = st.text_input("E-mail do cliente", value="" if not retorno_existente else str(retorno_existente.get("email_cliente") or ""), key=f"email_{etapa}")
                    titulo_email = st.text_input("Título do e-mail *", value="" if not retorno_existente else str(retorno_existente.get("titulo_email") or ""), key=f"titulo_{etapa}")
                    comprovacao_envio = st.text_input("Comprovação do envio", value="" if not retorno_existente else str(retorno_existente.get("comprovacao_envio") or ""), key=f"comp_{etapa}")
                descricao_retorno = st.text_area("Descrição do retorno / conteúdo enviado", value="" if not retorno_existente else str(retorno_existente.get("descricao_retorno") or ""), height=150, key=f"desc_{etapa}")
                arquivo = st.file_uploader("Anexar evidência", key=f"arquivo_{etapa}")
                salvar_retorno = st.form_submit_button("Salvar retorno")

                if salvar_retorno:
                    if not responsavel_envio or not contato_cliente or not titulo_email:
                        st.error("Preencha os campos obrigatórios: responsável pelo envio, pessoa responsável no cliente e título do e-mail.")
                    else:
                        criar_backup_automatico("antes_retorno")
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
                                str(data_envio), hora_envio, titulo_email, contato_cliente, email_cliente,
                                responsavel_envio, descricao_retorno, comprovacao_envio, anexo_path, now_str(),
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
                                int(ocorr["id"]), etapa, str(data_envio), hora_envio, titulo_email,
                                contato_cliente, email_cliente, responsavel_envio, descricao_retorno,
                                comprovacao_envio, anexo_path, now_str(), now_str()
                            ])
                        atualizar_status_geral(int(ocorr["id"]))
                        criar_backup_automatico("apos_retorno")
                        st.success("Retorno registrado com sucesso no banco de dados da ocorrência.")

            st.markdown("---")
            st.markdown("### Exclusão controlada")
            usuario_exclusao = st.text_input("Usuário responsável pela exclusão", key="usuario_exclusao")
            motivo_exclusao = st.text_area("Motivo da exclusão", key="motivo_exclusao", height=100)
            comando = st.text_input("Comando de exclusão", placeholder=f"EXCLUIR {ocorr['codigo']}", key="cmd_exclusao")
            if st.button("Excluir ocorrência", key="btn_excluir_ocorrencia"):
                ok, msg = excluir_ocorrencia_por_comando(ocorr["codigo"], comando, usuario_exclusao, motivo_exclusao)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

# =========================================================
# ABA 4
# =========================================================
with abas[3]:
    st.subheader("CONSULTA OCORRÊNCIA")
    codigo_consulta = st.text_input("Código para consulta", key="codigo_consulta")
    if codigo_consulta:
        ocorr = buscar_ocorrencia_por_codigo(codigo_consulta.strip())
        if not ocorr:
            st.warning("Ocorrência não encontrada.")
        else:
            st.write(f"**Código:** {ocorr['codigo']}")
            st.write(f"**Título:** {ocorr['titulo']}")
            st.write(f"**Cliente:** {ocorr['cliente']}")
            st.write(f"**Data de emissão:** {ocorr['data_abertura']}")
            st.write(f"**Responsável:** {ocorr.get('responsavel_interno') or ''}")
            st.write(f"**Descrição:** {ocorr['descricao']}")
            st.write(f"**Quantidade não conforme:** {ocorr['quantidade']}")
            st.write(f"**Status geral:** {ocorr['status_geral']}")
            st.dataframe(gerar_resumo_semaforo(ocorr), use_container_width=True, hide_index=True)

            retornos_df = listar_retornos_por_ocorrencia(int(ocorr["id"]))
            if retornos_df.empty:
                st.info("Nenhum retorno registrado para esta ocorrência.")
            else:
                st.dataframe(retornos_df, use_container_width=True, hide_index=True)

# =========================================================
# ABA 5
# =========================================================
with abas[4]:
    st.subheader("DASHBORAD")
    base = montar_base_dashboard()
    if base.empty:
        st.info("Não há dados para gerar dashboard.")
    else:
        col_a, col_b = st.columns(2)
        with col_a:
            serie_mes = base.groupby("mes_ref").size().sort_index()
            fig = plt.figure(figsize=(8, 4))
            ax = fig.add_subplot(111)
            ax.bar(serie_mes.index, serie_mes.values)
            ax.set_xlabel("Mês")
            ax.set_ylabel("Ocorrências")
            plt.xticks(rotation=45)
            plt.tight_layout()
            st.pyplot(fig)
        with col_b:
            serie_cli = base.groupby("cliente").size().sort_values(ascending=False).head(10)
            fig = plt.figure(figsize=(8, 4))
            ax = fig.add_subplot(111)
            ax.bar(serie_cli.index, serie_cli.values)
            ax.set_xlabel("Cliente")
            ax.set_ylabel("Ocorrências")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

# =========================================================
# ABA 6
# =========================================================
with abas[5]:
    st.subheader("EXPORTAR / BACKUP")

    e1, e2 = st.columns(2)
    with e1:
        if st.button("Gerar Excel de exportação"):
            ocorr_df, ret_df, log_df = exportar_bases()
            arquivo_saida = BASE_DIR / "exportacao_controle_reclamacoes_v4.xlsx"
            with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
                ocorr_df.to_excel(writer, sheet_name="Ocorrencias", index=False)
                ret_df.to_excel(writer, sheet_name="Retornos", index=False)
                log_df.to_excel(writer, sheet_name="Log_Exclusoes", index=False)
            with open(arquivo_saida, "rb") as f:
                st.download_button(
                    label="Baixar Excel",
                    data=f,
                    file_name="exportacao_controle_reclamacoes_v4.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    with e2:
        if st.button("Gerar backup manual do banco"):
            destino = criar_backup_automatico("manual")
            if destino:
                st.success(f"Backup criado: {destino.name}")

    st.markdown("### Backups disponíveis")
    backups_df = listar_backups()
    if backups_df.empty:
        st.info("Nenhum backup disponível.")
    else:
        st.dataframe(backups_df[["arquivo", "data_modificacao", "tamanho_kb"]], use_container_width=True, hide_index=True)
        arquivo_sel = st.selectbox("Selecionar backup para download", backups_df["arquivo"].tolist())
        caminho_sel = BACKUP_DIR / arquivo_sel
        with open(caminho_sel, "rb") as f:
            st.download_button(
                label="Baixar backup selecionado",
                data=f,
                file_name=arquivo_sel,
                mime="application/octet-stream"
            )

    st.markdown("### Log de exclusões")
    log_df = run_select("SELECT * FROM log_exclusoes ORDER BY id DESC")
    if log_df.empty:
        st.info("Nenhuma exclusão registrada.")
    else:
        st.dataframe(log_df, use_container_width=True, hide_index=True)

st.caption("Versão 4: gravação em banco do app com backup automático antes e depois de operações críticas, download de backups e log de exclusão.")
