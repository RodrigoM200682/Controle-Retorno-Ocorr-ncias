
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
    page_title="Retorno de Reclamações de Clientes - RRC-RS 2026_v_15",
    page_icon="📋",
    layout="wide",
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data_app"
DATA_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "controle_reclamacoes_v15.db"
UPLOAD_DIR = DATA_DIR / "uploads_v15"
BACKUP_DIR = DATA_DIR / "backups_v15"
UPLOAD_DIR.mkdir(exist_ok=True)
BACKUP_DIR.mkdir(exist_ok=True)

ETAPAS = [
    "Contenção imediata",
    "Análise de causa",
    "Plano de ação",
    "Envio de evidências",
    "Improcedência",
]

PRAZOS_DIAS = {
    "Contenção imediata": 1,
    "Análise de causa": 5,
    "Plano de ação": 10,
    "Envio de evidências": 15,
    "Improcedência": 5,
}

STATUS_GERAIS = [
    "Aberta",
    "Aguardando contenção",
    "Aguardando análise de causa",
    "Aguardando plano de ação",
    "Aguardando evidências",
    "Concluída",
]

MESES_ABREV = {
    1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun",
    7: "jul", 8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez"
}

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
# FUNÇÕES GERAIS
# =========================================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def formatar_data_br(valor):
    data = pd.to_datetime(valor, errors="coerce")
    if pd.isna(data):
        return ""
    return data.strftime("%d/%m/%Y")

def rotulo_mes_abrev(valor):
    data = pd.to_datetime(valor, errors="coerce")
    if pd.isna(data):
        return ""
    return f"{MESES_ABREV.get(data.month, '')}/{str(data.year)[-2:]}"

def limpar_responsavel(valor):
    texto = "" if pd.isna(valor) else str(valor).strip()
    return "Avaliação" if texto == "" else texto

def preparar_barras_com_rotulos(ax, serie, rotacao=0, alinhamento="center", cores=None):
    if cores is None:
        barras = ax.bar(serie.index, serie.values)
    else:
        barras = ax.bar(serie.index, serie.values, color=cores)
    ax.set_ylabel("")
    ax.set_yticks([])
    ax.spines["left"].set_visible(False)
    ax.tick_params(axis="y", left=False, labelleft=False)
    for barra in barras:
        altura = barra.get_height()
        ax.annotate(
            f"{int(altura)}",
            xy=(barra.get_x() + barra.get_width() / 2, altura),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
        )
    plt.xticks(rotation=rotacao, ha=alinhamento)
    return barras

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
            "data_modificacao": datetime.fromtimestamp(arq.stat().st_mtime).strftime("%d/%m/%Y %H:%M:%S"),
            "tamanho_kb": round(arq.stat().st_size / 1024, 2),
        })
    return pd.DataFrame(dados)

# =========================================================
# DADOS
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
    df = run_select("""
        SELECT *
        FROM ocorrencias
        ORDER BY date(data_abertura) DESC, codigo DESC
    """)
    if not df.empty:
        df["responsavel_interno"] = df["responsavel_interno"].apply(limpar_responsavel)
    return df

def buscar_ocorrencia_por_codigo(codigo):
    df = run_select("""
        SELECT *
        FROM ocorrencias
        WHERE codigo = ?
        LIMIT 1
    """, [codigo])
    if df.empty:
        return None
    registro = df.iloc[0].to_dict()
    registro["responsavel_interno"] = limpar_responsavel(registro.get("responsavel_interno"))
    return registro

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

def obter_lista_responsaveis():
    base = listar_ocorrencias()
    if base.empty:
        return []
    lista = sorted(list(set(base["responsavel_interno"].apply(limpar_responsavel).tolist())))
    return lista

def aplicar_filtro_responsavel(df, responsavel):
    if df.empty or responsavel == "Todos":
        return df
    if "responsavel_interno" in df.columns:
        return df[df["responsavel_interno"].apply(limpar_responsavel) == responsavel]
    return df

def atualizar_status_geral(ocorrencia_id):
    improcedencia = buscar_retorno(ocorrencia_id, "Improcedência")
    cont = buscar_retorno(ocorrencia_id, "Contenção imediata")
    analise = buscar_retorno(ocorrencia_id, "Análise de causa")
    plano = buscar_retorno(ocorrencia_id, "Plano de ação")
    evidencia = buscar_retorno(ocorrencia_id, "Envio de evidências")

    if improcedencia:
        status = "Concluída"
    elif evidencia:
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

def obter_etapa_atual(ocorrencia):
    resumo = gerar_resumo_semaforo(ocorrencia)
    for _, linha in resumo.iterrows():
        status = str(linha["Status"])
        if "⚪" in status or "🟡" in status or "🔴" in status:
            return str(linha["Etapa"])
    return "Concluída"

def gerar_resumo_semaforo(ocorrencia):
    ordem = {
        "Contenção imediata": 1,
        "Análise de causa": 2,
        "Plano de ação": 3,
        "Envio de evidências": 4,
        "Improcedência": 99,
    }

    retornos = {etapa: buscar_retorno(int(ocorrencia["id"]), etapa) for etapa in ETAPAS}
    improcedencia_retorno = retornos.get("Improcedência")
    improcedencia = improcedencia_retorno is not None

    maior_etapa_enviada = 0
    data_etapa_mais_avancada = ""
    for etapa, retorno in retornos.items():
        if retorno and etapa != "Improcedência":
            if ordem.get(etapa, 0) >= maior_etapa_enviada:
                maior_etapa_enviada = ordem.get(etapa, 0)
                data_etapa_mais_avancada = formatar_data_br(retorno.get("data_envio") or "")

    dados = []
    for etapa in ETAPAS:
        prazo = calcular_prazo_etapa(ocorrencia, etapa)
        retorno = retornos.get(etapa)

        if improcedencia:
            status = "🟢 Concluída"
            enviado_em = formatar_data_br(improcedencia_retorno.get("data_envio") or "")
        elif retorno:
            status = status_semaforo(ocorrencia, etapa)
            enviado_em = formatar_data_br(retorno.get("data_envio") or "")
        elif etapa != "Improcedência" and ordem.get(etapa, 0) < maior_etapa_enviada:
            status = "🟢 Concluída"
            enviado_em = data_etapa_mais_avancada
        else:
            status = status_semaforo(ocorrencia, etapa)
            enviado_em = ""

        dados.append({
            "Etapa": etapa,
            "Prazo": "" if prazo is None else prazo.strftime("%d/%m/%Y"),
            "Status": status,
            "Enviado em": enviado_em,
        })
    return pd.DataFrame(dados)

def indicadores(df_ocorr):
    total = len(df_ocorr)
    if total == 0:
        return {"total": 0, "abertas": 0, "concluidas": 0, "atrasadas": 0}

    abertas = len(df_ocorr[df_ocorr["status_geral"].isin(["Aberta", "Aguardando contenção"])])
    concluidas = len(df_ocorr[df_ocorr["status_geral"] == "Concluída"])
    atrasadas = 0
    for _, row in df_ocorr.iterrows():
        resumo = gerar_resumo_semaforo(row.to_dict())
        if resumo["Status"].astype(str).str.contains("🔴", na=False).any():
            atrasadas += 1

    return {"total": total, "abertas": abertas, "concluidas": concluidas, "atrasadas": atrasadas}

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
            COALESCE(NULLIF(o.responsavel_interno, ''), 'Avaliação') AS responsavel_interno,
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
    if not ret_df.empty:
        ret_df["data_abertura"] = ret_df["data_abertura"].apply(formatar_data_br)
        ret_df["data_envio"] = ret_df["data_envio"].apply(formatar_data_br)
    log_df = run_select("SELECT * FROM log_exclusoes ORDER BY id DESC")
    if not log_df.empty:
        log_df["deleted_at"] = log_df["deleted_at"].apply(formatar_data_br)
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
        return {
            "ok": False,
            "mensagem": f"Erro ao ler o arquivo Excel: {e}",
            "incluidas": 0,
            "ignoradas": 0,
            "ja_existentes": 0,
        }

    df.columns = [str(c).strip() for c in df.columns]
    faltantes = [c for c in HEADERS_IMPORTACAO_PADRAO if c not in set(df.columns)]
    if faltantes:
        return {
            "ok": False,
            "mensagem": "A planilha precisa seguir exatamente o cabeçalho padrão: " + ", ".join(HEADERS_IMPORTACAO_PADRAO),
            "incluidas": 0,
            "ignoradas": 0,
            "ja_existentes": 0,
        }

    incluidas = 0
    ignoradas = 0
    ja_existentes = 0

    base_atual = listar_ocorrencias()
    codigos_existentes = set()
    if not base_atual.empty and "codigo" in base_atual.columns:
        codigos_existentes = {
            str(c).strip()
            for c in base_atual["codigo"].fillna("").astype(str).tolist()
            if str(c).strip() != ""
        }

    codigos_processados_planilha = set()
    criar_backup_automatico("antes_importacao")

    for _, row in df.iterrows():
        codigo = str(row.get("Código", "")).strip()
        titulo = str(row.get("Título", "")).strip()
        cliente = str(row.get("Cliente", "")).strip()

        if not codigo or not titulo or not cliente or codigo.lower() == "nan" or titulo.lower() == "nan" or cliente.lower() == "nan":
            ignoradas += 1
            continue

        if codigo in codigos_processados_planilha:
            ignoradas += 1
            continue

        codigos_processados_planilha.add(codigo)

        if codigo in codigos_existentes:
            ja_existentes += 1
            continue

        data_abertura = pd.to_datetime(row.get("Data de emissão"), errors="coerce")
        if pd.isna(data_abertura):
            data_abertura = pd.to_datetime(date.today())

        responsavel = limpar_responsavel(row.get("Responsável", ""))
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
            codigo, data_abertura.date().isoformat(), cliente, titulo, descricao,
            quantidade, responsavel, "Aberta", now_str(), now_str()
        ])
        incluidas += 1
        codigos_existentes.add(codigo)

    criar_backup_automatico("apos_importacao")
    return {
        "ok": True,
        "mensagem": "Importação concluída com comparação por número da ocorrência.",
        "incluidas": incluidas,
        "ignoradas": ignoradas,
        "ja_existentes": ja_existentes,
    }

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
        ocorr["codigo"], ocorr["titulo"], ocorr["cliente"],
        str(usuario).strip(), str(motivo).strip(), str(comando).strip(), now_str()
    ])

    run_exec("DELETE FROM retornos WHERE ocorrencia_id = ?", [int(ocorr["id"])])
    run_exec("DELETE FROM ocorrencias WHERE id = ?", [int(ocorr["id"])])
    criar_backup_automatico(f"apos_exclusao_{codigo}")
    return True, "Ocorrência excluída com sucesso."

def montar_base_dashboard(df_filtrado):
    ocorr = df_filtrado.copy()
    if ocorr.empty:
        return ocorr
    ocorr["data_abertura"] = pd.to_datetime(ocorr["data_abertura"], errors="coerce")
    ocorr["mes_ref"] = ocorr["data_abertura"].dt.to_period("M").astype(str)
    ocorr["mes_label"] = ocorr["data_abertura"].apply(rotulo_mes_abrev)
    ocorr["responsavel_interno"] = ocorr["responsavel_interno"].apply(limpar_responsavel)
    return ocorr

# =========================================================
# ESTILO
# =========================================================
st.markdown("""
<style>
.block-container {padding-top: 1rem; padding-bottom: 1rem;}
[data-testid="stMetricValue"] {font-size: 1.3rem;}
.small-card {border: 1px solid #d9d9d9; border-radius: 12px; padding: 12px; background-color: #fafafa;}
</style>
""", unsafe_allow_html=True)

# =========================================================
# FILTRO GLOBAL
# =========================================================
base_global = listar_ocorrencias()

with st.sidebar:
    st.header("Filtros")
    termo = st.text_input("Buscar por código, cliente ou título")
    status_filtro = st.selectbox("Status geral", ["Todos"] + STATUS_GERAIS)
    responsavel_filtro = st.selectbox("Responsável", ["Todos"] + obter_lista_responsaveis())
    st.markdown("---")
    st.write("**Cabeçalho padrão de importação**")
    st.code(" | ".join(HEADERS_IMPORTACAO_PADRAO))

def aplicar_filtros_globais(df):
    df2 = df.copy()
    if df2.empty:
        return df2
    if termo:
        t = termo.lower()
        df2 = df2[
            df2["codigo"].astype(str).str.lower().str.contains(t, na=False) |
            df2["cliente"].astype(str).str.lower().str.contains(t, na=False) |
            df2["titulo"].astype(str).str.lower().str.contains(t, na=False)
        ]
    if status_filtro != "Todos":
        df2 = df2[df2["status_geral"] == status_filtro]
    df2 = aplicar_filtro_responsavel(df2, responsavel_filtro)
    return df2

base_filtrada = aplicar_filtros_globais(base_global)

if "codigo_selecionado_painel" not in st.session_state:
    st.session_state["codigo_selecionado_painel"] = ""

# =========================================================
# TOPO
# =========================================================
st.title("📋 Retorno de Reclamações de Clientes - RRC-RS 2026_v_15")
if responsavel_filtro == "Todos":
    st.caption("Visualização geral de todas as ocorrências.")
else:
    st.caption(f"Visualização filtrada pelo responsável: {responsavel_filtro}")

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
    "EXPORTAR / BACKUP",
])

# =========================================================
# ABA 1
# =========================================================
with abas[0]:
    ind = indicadores(base_filtrada)
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
                st.success(
                    f'{resultado["mensagem"]} '
                    f'Incluídas: {resultado["incluidas"]}. '
                    f'Já existentes no sistema: {resultado.get("ja_existentes", 0)}. '
                    f'Ignoradas: {resultado["ignoradas"]}.'
                )
                st.info("O sistema comparou o número da ocorrência da planilha com os códigos já cadastrados no app e incluiu somente as ocorrências que ainda não existiam.")
            else:
                st.error(resultado["mensagem"])

    if not base_filtrada.empty:
        visao = base_filtrada.copy()
        visao["Etapa do retorno"] = visao.apply(lambda row: obter_etapa_atual(row.to_dict()), axis=1)
        visao = visao[["codigo", "titulo", "status_geral", "Etapa do retorno"]].copy()
        visao.columns = ["Código", "Título", "Status", "Etapa do retorno"]

        st.markdown("### Lista de reclamações")
        cab1, cab2, cab3, cab4, cab5 = st.columns([1.1, 3.2, 1.5, 1.8, 0.8])
        cab1.markdown("**Código**")
        cab2.markdown("**Título**")
        cab3.markdown("**Status**")
        cab4.markdown("**Etapa do retorno**")
        cab5.markdown("**Ação**")
        st.markdown("---")
        for _, row in visao.iterrows():
            col_a, col_b, col_c, col_d, col_e = st.columns([1.1, 3.2, 1.5, 1.8, 0.8])
            col_a.write(row["Código"])
            col_b.write(row["Título"])
            col_c.write(row["Status"])
            col_d.write(row["Etapa do retorno"])
            if col_e.button("Abrir", key=f"abrir_{row['Código']}"):
                st.session_state["codigo_selecionado_painel"] = row["Código"]
                st.rerun()
            st.markdown("---")

        codigo_sel = st.session_state.get("codigo_selecionado_painel", "")
        if codigo_sel:
            ocorr_sel = buscar_ocorrencia_por_codigo(codigo_sel)
            if ocorr_sel:
                st.markdown("## Ocorrência selecionada")
                st.info(f"Painel detalhado da ocorrência {codigo_sel}.")
                d1, d2, d3 = st.columns(3)
                d1.write(f"**Código:** {ocorr_sel['codigo']}")
                d1.write(f"**Cliente:** {ocorr_sel['cliente']}")
                d2.write(f"**Título:** {ocorr_sel['titulo']}")
                d2.write(f"**Status geral:** {ocorr_sel['status_geral']}")
                d3.write(f"**Responsável:** {limpar_responsavel(ocorr_sel.get('responsavel_interno'))}")
                d3.write(f"**Data de emissão:** {formatar_data_br(ocorr_sel['data_abertura'])}")
                st.write(f"**Descrição:** {ocorr_sel['descricao']}")
                st.write(f"**Quantidade não conforme:** {ocorr_sel['quantidade']}")

                st.markdown("### Detalhe das etapas")
                st.dataframe(gerar_resumo_semaforo(ocorr_sel), use_container_width=True, hide_index=True)

                ac1, ac2 = st.columns(2)
                with ac1:
                    st.markdown("### Registrar retorno")
                    etapa_sel = st.selectbox("Etapa do retorno", ETAPAS, key="painel_etapa_retorno")
                    retorno_existente = buscar_retorno(int(ocorr_sel["id"]), etapa_sel)
                    with st.form("form_retorno_painel", clear_on_submit=False):
                        p1, p2 = st.columns(2)
                        with p1:
                            data_envio = st.date_input("Data do envio", value=date.today(), format="DD/MM/YYYY", key="painel_data_envio")
                            hora_envio = st.text_input("Hora do envio", value=datetime.now().strftime("%H:%M"), key="painel_hora_envio")
                            responsavel_envio = st.text_input("Responsável pelo envio *", value="" if not retorno_existente else str(retorno_existente.get("responsavel_envio") or ""), key="painel_resp_envio")
                            contato_cliente = st.text_input("Pessoa responsável no cliente *", value="" if not retorno_existente else str(retorno_existente.get("contato_cliente") or ""), key="painel_contato_cliente")
                        with p2:
                            email_cliente = st.text_input("E-mail do cliente", value="" if not retorno_existente else str(retorno_existente.get("email_cliente") or ""), key="painel_email_cliente")
                            titulo_email = st.text_input("Título do e-mail *", value="" if not retorno_existente else str(retorno_existente.get("titulo_email") or ""), key="painel_titulo_email")
                            comprovacao_envio = st.text_input("Comprovação do envio", value="" if not retorno_existente else str(retorno_existente.get("comprovacao_envio") or ""), key="painel_comprovacao")
                        descricao_retorno = st.text_area("Descrição do retorno", value="" if not retorno_existente else str(retorno_existente.get("descricao_retorno") or ""), height=120, key="painel_desc_retorno")
                        arquivo = st.file_uploader("Anexar evidência", key="painel_arquivo_retorno")
                        salvar_retorno_painel = st.form_submit_button("Salvar retorno da ocorrência")
                        if salvar_retorno_painel:
                            if not responsavel_envio or not contato_cliente or not titulo_email:
                                st.error("Preencha os campos obrigatórios: responsável pelo envio, pessoa responsável no cliente e título do e-mail.")
                            else:
                                criar_backup_automatico("antes_retorno_painel")
                                anexo_path = None
                                if arquivo is not None:
                                    anexo_path = salvar_arquivo(arquivo, ocorr_sel["codigo"], etapa_sel)
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
                                        int(ocorr_sel["id"]), etapa_sel, str(data_envio), hora_envio, titulo_email,
                                        contato_cliente, email_cliente, responsavel_envio, descricao_retorno,
                                        comprovacao_envio, anexo_path, now_str(), now_str()
                                    ])
                                atualizar_status_geral(int(ocorr_sel["id"]))
                                criar_backup_automatico("apos_retorno_painel")
                                st.success("Retorno registrado com sucesso para a ocorrência selecionada.")
                                st.rerun()

                with ac2:
                    st.markdown("### Detalhe do retorno")
                    retornos_df = listar_retornos_por_ocorrencia(int(ocorr_sel["id"]))
                    if retornos_df.empty:
                        st.info("Nenhum retorno registrado para esta ocorrência.")
                    else:
                        retornos_df["data_envio"] = retornos_df["data_envio"].apply(formatar_data_br)
                        st.dataframe(retornos_df, use_container_width=True, hide_index=True)

                    st.markdown("### Excluir ocorrência")
                    usuario_exclusao_painel = st.text_input("Usuário responsável pela exclusão", key="painel_usuario_exclusao")
                    motivo_exclusao_painel = st.text_area("Motivo da exclusão", key="painel_motivo_exclusao", height=100)
                    comando_exclusao_painel = st.text_input("Comando de exclusão", placeholder=f"EXCLUIR {ocorr_sel['codigo']}", key="painel_cmd_exclusao")
                    if st.button("Excluir ocorrência selecionada", key="painel_btn_excluir"):
                        ok, msg = excluir_ocorrencia_por_comando(ocorr_sel["codigo"], comando_exclusao_painel, usuario_exclusao_painel, motivo_exclusao_painel)
                        if ok:
                            st.success(msg)
                            st.session_state["codigo_selecionado_painel"] = ""
                            st.rerun()
                        else:
                            st.error(msg)

                if st.button("Limpar seleção da ocorrência", key="limpar_selecao_ocorrencia"):
                    st.session_state["codigo_selecionado_painel"] = ""
                    st.rerun()
    else:
        st.info("Nenhuma ocorrência encontrada para o filtro selecionado.")

# =========================================================
# ABA 2
# =========================================================
with abas[1]:
    st.subheader("REGISTRAR OCORRÊNCIA")
    with st.form("form_ocorrencia", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            codigo = st.text_input("Código *")
            data_abertura = st.date_input("Data de emissão", value=date.today(), format="DD/MM/YYYY")
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
                        descricao.strip(), float(quantidade), limpar_responsavel(responsavel_interno),
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
    opcoes_codigo = base_filtrada["codigo"].astype(str).tolist() if not base_filtrada.empty else []
    codigo_busca = st.selectbox("Código da ocorrência", [""] + opcoes_codigo, key="codigo_retorno")

    if codigo_busca:
        ocorr = buscar_ocorrencia_por_codigo(codigo_busca.strip())
        if not ocorr:
            st.warning("Código não encontrado no banco de dados.")
        else:
            st.write(f"**Responsável:** {limpar_responsavel(ocorr.get('responsavel_interno'))}")
            st.dataframe(gerar_resumo_semaforo(ocorr), use_container_width=True, hide_index=True)

            etapa = st.selectbox("Etapa do retorno", ETAPAS)
            retorno_existente = buscar_retorno(int(ocorr["id"]), etapa)

            with st.form("form_retorno", clear_on_submit=False):
                a1, a2 = st.columns(2)
                with a1:
                    data_envio = st.date_input("Data do envio", value=date.today(), format="DD/MM/YYYY", key=f"data_{etapa}")
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
    opcoes_codigo_consulta = base_filtrada["codigo"].astype(str).tolist() if not base_filtrada.empty else []
    codigo_consulta = st.selectbox("Código para consulta", [""] + opcoes_codigo_consulta, key="codigo_consulta")
    if codigo_consulta:
        ocorr = buscar_ocorrencia_por_codigo(codigo_consulta.strip())
        if not ocorr:
            st.warning("Ocorrência não encontrada.")
        else:
            st.write(f"**Código:** {ocorr['codigo']}")
            st.write(f"**Título:** {ocorr['titulo']}")
            st.write(f"**Cliente:** {ocorr['cliente']}")
            st.write(f"**Data de emissão:** {formatar_data_br(ocorr['data_abertura'])}")
            st.write(f"**Responsável:** {limpar_responsavel(ocorr.get('responsavel_interno'))}")
            st.write(f"**Descrição:** {ocorr['descricao']}")
            st.write(f"**Quantidade não conforme:** {ocorr['quantidade']}")
            st.write(f"**Status geral:** {ocorr['status_geral']}")
            st.dataframe(gerar_resumo_semaforo(ocorr), use_container_width=True, hide_index=True)

            retornos_df = listar_retornos_por_ocorrencia(int(ocorr["id"]))
            if retornos_df.empty:
                st.info("Nenhum retorno registrado para esta ocorrência.")
            else:
                retornos_df["data_envio"] = retornos_df["data_envio"].apply(formatar_data_br)
                st.dataframe(retornos_df, use_container_width=True, hide_index=True)

# =========================================================
# ABA 5
# =========================================================
with abas[4]:
    st.subheader("DASHBORAD")
    base_dash = montar_base_dashboard(base_filtrada)
    if base_dash.empty:
        st.info("Não há dados para gerar dashboard.")
    else:
        atrasadas_lista = []
        for _, row in base_dash.iterrows():
            resumo = gerar_resumo_semaforo(row.to_dict())
            atrasada = resumo["Status"].astype(str).str.contains("🔴", na=False).any()
            atrasadas_lista.append("Em atraso" if atrasada else "Concluído")
        base_dash["em_atraso"] = atrasadas_lista

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Ocorrências por mês**")
            serie_mes = base_dash.groupby(["mes_ref", "mes_label"]).size().reset_index(name="qtd").sort_values("mes_ref")
            serie_mes_plot = pd.Series(serie_mes["qtd"].values, index=serie_mes["mes_label"].values)
            fig = plt.figure(figsize=(8, 4))
            ax = fig.add_subplot(111)
            preparar_barras_com_rotulos(ax, serie_mes_plot, rotacao=45, alinhamento="right")
            ax.set_xlabel("Mês")
            plt.tight_layout()
            st.pyplot(fig)

        with col_b:
            st.markdown("**Ocorrências por cliente**")
            serie_cli = base_dash.groupby("cliente").size().sort_values(ascending=False).head(10)
            fig = plt.figure(figsize=(8, 4))
            ax = fig.add_subplot(111)
            preparar_barras_com_rotulos(ax, serie_cli, rotacao=45, alinhamento="right")
            ax.set_xlabel("Cliente")
            plt.tight_layout()
            st.pyplot(fig)

        col_c, col_d = st.columns(2)
        with col_c:
            st.markdown("**Ocorrências por responsável**")
            serie_resp = base_dash.groupby("responsavel_interno").size().sort_values(ascending=False).head(10)
            fig = plt.figure(figsize=(8, 4))
            ax = fig.add_subplot(111)
            preparar_barras_com_rotulos(ax, serie_resp, rotacao=45, alinhamento="right")
            ax.set_xlabel("Responsável")
            plt.tight_layout()
            st.pyplot(fig)

        with col_d:
            st.markdown("**Ocorrências em atraso**")
            serie_atraso = base_dash.groupby("em_atraso").size().reindex(["Em atraso", "Concluído"], fill_value=0)
            cores = ["red" if idx == "Em atraso" else "green" for idx in serie_atraso.index]
            fig = plt.figure(figsize=(8, 4))
            ax = fig.add_subplot(111)
            preparar_barras_com_rotulos(ax, serie_atraso, cores=cores)
            ax.set_xlabel("Situação")
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
            ocorr_df = aplicar_filtro_responsavel(ocorr_df, responsavel_filtro)
            if not ocorr_df.empty:
                codigos_filtrados = ocorr_df["codigo"].astype(str).unique().tolist()
                ret_df = ret_df[ret_df["codigo"].astype(str).isin(codigos_filtrados)] if not ret_df.empty else ret_df
                log_df = log_df[log_df["codigo_ocorrencia"].astype(str).isin(codigos_filtrados)] if not log_df.empty else log_df
                ocorr_df["data_abertura"] = ocorr_df["data_abertura"].apply(formatar_data_br)

            arquivo_saida = BASE_DIR / "exportacao_controle_reclamacoes_v15.xlsx"
            with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as writer:
                ocorr_df.to_excel(writer, sheet_name="Ocorrencias", index=False)
                ret_df.to_excel(writer, sheet_name="Retornos", index=False)
                log_df.to_excel(writer, sheet_name="Log_Exclusoes", index=False)

            with open(arquivo_saida, "rb") as f:
                st.download_button(
                    label="Baixar Excel",
                    data=f,
                    file_name="exportacao_controle_reclamacoes_v15.xlsx",
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
        st.dataframe(backups_df, use_container_width=True, hide_index=True)
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
    if not log_df.empty and responsavel_filtro != "Todos":
        codigos_filtrados = base_filtrada["codigo"].astype(str).tolist() if not base_filtrada.empty else []
        log_df = log_df[log_df["codigo_ocorrencia"].astype(str).isin(codigos_filtrados)]
    if log_df.empty:
        st.info("Nenhuma exclusão registrada para o filtro atual.")
    else:
        log_df["deleted_at"] = log_df["deleted_at"].apply(formatar_data_br)
        st.dataframe(log_df, use_container_width=True, hide_index=True)

st.caption("Versão 15: filtro global por responsável em todas as telas, datas no padrão brasileiro, gráficos com rótulos no topo e tratamento automático do responsável vazio como Avaliação.")
