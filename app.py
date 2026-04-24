import sqlite3
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import streamlit as st
import plotly.express as px

APP_TITLE = "SQDCP - Painel Industrial em Rede"
DB_PATH = Path("sqdcp_industrial.db")

CATEGORIAS = {
    "S - Segurança": ["Acidente", "Incidente", "Quase acidente", "Comportamento inseguro", "Condição insegura"],
    "Q - Qualidade": ["Reclamação de cliente", "RNC interna", "Retrabalho", "Refugo", "Bloqueio"],
    "D - Delivery": ["Atraso de entrega", "Atraso de programação", "Falta de material", "Parada logística"],
    "C - Custo": ["Perda de material", "Hora extra", "Consumo acima do padrão", "Ajuste de inventário"],
    "P - Processo": ["Eficiência", "Parada de máquina", "Setup", "Disponibilidade", "Performance"]
}

PROCESSOS_P = ["Prensas", "Litografia", "Montagem"]
TURNOS = ["1º", "2º", "3º"]
STATUS = ["Aberto", "Em análise", "Concluído"]


def conectar():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def criar_banco():
    conn = conectar()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS lancamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            semana INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            ano INTEGER NOT NULL,
            turno TEXT NOT NULL,
            categoria TEXT NOT NULL,
            indicador TEXT NOT NULL,
            processo TEXT,
            valor REAL NOT NULL,
            meta REAL,
            unidade TEXT,
            responsavel TEXT,
            status TEXT,
            descricao TEXT,
            acao TEXT,
            criado_em TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def inserir_lancamento(dados):
    conn = conectar()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO lancamentos (
            data, semana, mes, ano, turno, categoria, indicador, processo, valor,
            meta, unidade, responsavel, status, descricao, acao, criado_em
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        dados,
    )
    conn.commit()
    conn.close()


def carregar_dados():
    conn = conectar()
    df = pd.read_sql_query("SELECT * FROM lancamentos", conn)
    conn.close()
    if not df.empty:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["Mês/Ano"] = df["data"].dt.strftime("%m/%Y")
    return df


def excluir_lancamento(id_lancamento):
    conn = conectar()
    cur = conn.cursor()
    cur.execute("DELETE FROM lancamentos WHERE id = ?", (id_lancamento,))
    conn.commit()
    conn.close()


def aplicar_filtros(df):
    if df.empty:
        return df

    st.sidebar.header("Filtros")
    anos = sorted(df["ano"].dropna().unique().tolist())
    ano = st.sidebar.multiselect("Ano", anos, default=anos)

    meses = sorted(df["mes"].dropna().unique().tolist())
    mes = st.sidebar.multiselect("Mês", meses, default=meses)

    semanas = sorted(df["semana"].dropna().unique().tolist())
    semana = st.sidebar.multiselect("Semana", semanas, default=semanas)

    turnos = sorted(df["turno"].dropna().unique().tolist())
    turno = st.sidebar.multiselect("Turno", turnos, default=turnos)

    categorias = sorted(df["categoria"].dropna().unique().tolist())
    categoria = st.sidebar.multiselect("SQDCP", categorias, default=categorias)

    processos = sorted([x for x in df["processo"].dropna().unique().tolist() if x])
    processo = st.sidebar.multiselect("Processo", processos, default=processos) if processos else []

    filtrado = df[
        df["ano"].isin(ano)
        & df["mes"].isin(mes)
        & df["semana"].isin(semana)
        & df["turno"].isin(turno)
        & df["categoria"].isin(categoria)
    ]

    if processo:
        filtrado = filtrado[(filtrado["processo"].isin(processo)) | (filtrado["processo"].isna()) | (filtrado["processo"] == "")]

    return filtrado


def kpi_card(titulo, valor):
    st.metric(titulo, valor)


def pagina_dashboard(df):
    st.subheader("Painel Geral SQDCP")
    if df.empty:
        st.info("Ainda não existem lançamentos para exibir.")
        return

    df_f = aplicar_filtros(df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("Total de lançamentos", len(df_f))
    with col2:
        kpi_card("Soma dos valores", round(df_f["valor"].sum(), 2))
    with col3:
        kpi_card("Abertos", int((df_f["status"] == "Aberto").sum()))
    with col4:
        kpi_card("Concluídos", int((df_f["status"] == "Concluído").sum()))

    st.divider()

    c1, c2 = st.columns(2)

    with c1:
        base_cat = df_f.groupby("categoria", as_index=False)["valor"].sum()
        fig = px.bar(base_cat, x="categoria", y="valor", text="valor", title="Resultado por SQDCP")
        fig.update_traces(textposition="outside")
        fig.update_layout(yaxis_title="Valor", xaxis_title="SQDCP")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        base_turno = df_f.groupby("turno", as_index=False)["valor"].sum()
        fig = px.bar(base_turno, x="turno", y="valor", text="valor", title="Resultado por turno")
        fig.update_traces(textposition="outside")
        fig.update_layout(yaxis_title="Valor", xaxis_title="Turno")
        st.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        base_dia = df_f.groupby(df_f["data"].dt.date, as_index=False)["valor"].sum()
        base_dia.columns = ["data", "valor"]
        fig = px.line(base_dia, x="data", y="valor", markers=True, title="Evolução diária")
        fig.update_layout(yaxis_title="Valor", xaxis_title="Data")
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        base_semana = df_f.groupby("semana", as_index=False)["valor"].sum()
        fig = px.bar(base_semana, x="semana", y="valor", text="valor", title="Resultado por semana")
        fig.update_traces(textposition="outside")
        fig.update_layout(yaxis_title="Valor", xaxis_title="Semana")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Tabela de lançamentos")
    st.dataframe(df_f.sort_values("data", ascending=False), use_container_width=True, hide_index=True)

    excel = df_f.to_excel(index=False, engine="openpyxl") if False else None
    csv = df_f.to_csv(index=False, sep=";", encoding="utf-8-sig")
    st.download_button("Baixar dados filtrados em CSV", csv, "sqdcp_filtrado.csv", "text/csv")


def pagina_lancamento():
    st.subheader("Incluir lançamento")

    with st.form("form_lancamento", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            data_lanc = st.date_input("Data", value=date.today())
            turno = st.selectbox("Turno", TURNOS)
            categoria = st.selectbox("Indicador SQDCP", list(CATEGORIAS.keys()))
        with c2:
            indicador = st.selectbox("Tipo de lançamento", CATEGORIAS[categoria])
            processo = ""
            if categoria == "P - Processo":
                processo = st.selectbox("Processo", PROCESSOS_P)
            valor = st.number_input("Valor", min_value=0.0, step=1.0)
        with c3:
            meta = st.number_input("Meta / referência", min_value=0.0, step=1.0)
            unidade = st.text_input("Unidade / linha", placeholder="Ex.: VAK1, GL, LDL")
            responsavel = st.text_input("Responsável")
            status = st.selectbox("Status", STATUS)

        descricao = st.text_area("Descrição do fato")
        acao = st.text_area("Ação / encaminhamento")

        salvar = st.form_submit_button("Salvar lançamento")

        if salvar:
            semana = int(data_lanc.isocalendar().week)
            dados = (
                data_lanc.isoformat(),
                semana,
                data_lanc.month,
                data_lanc.year,
                turno,
                categoria,
                indicador,
                processo,
                float(valor),
                float(meta),
                unidade,
                responsavel,
                status,
                descricao,
                acao,
                datetime.now().isoformat(timespec="seconds"),
            )
            inserir_lancamento(dados)
            st.success("Lançamento salvo com sucesso.")
            st.rerun()


def pagina_edicao(df):
    st.subheader("Consultar e excluir lançamentos")
    if df.empty:
        st.info("Não existem lançamentos cadastrados.")
        return

    termo = st.text_input("Pesquisar por descrição, responsável, unidade, indicador ou categoria")
    df_view = df.copy()
    if termo:
        termo_low = termo.lower()
        cols = ["descricao", "responsavel", "unidade", "indicador", "categoria", "processo", "status"]
        mask = False
        for col in cols:
            mask = mask | df_view[col].fillna("").astype(str).str.lower().str.contains(termo_low, na=False)
        df_view = df_view[mask]

    st.dataframe(df_view.sort_values("data", ascending=False), use_container_width=True, hide_index=True)

    ids = df_view["id"].tolist()
    if ids:
        id_excluir = st.selectbox("Selecione o ID para excluir", ids)
        confirmar = st.checkbox("Confirmo que desejo excluir este lançamento")
        if st.button("Excluir lançamento selecionado"):
            if confirmar:
                excluir_lancamento(id_excluir)
                st.success("Lançamento excluído.")
                st.rerun()
            else:
                st.warning("Marque a confirmação antes de excluir.")


def pagina_importacao():
    st.subheader("Importar dados de Excel ou CSV")
    st.write("Use esta opção para carregar uma base inicial. A planilha deve conter colunas compatíveis com o modelo abaixo.")
    modelo = pd.DataFrame(columns=[
        "data", "turno", "categoria", "indicador", "processo", "valor", "meta", "unidade", "responsavel", "status", "descricao", "acao"
    ])
    st.dataframe(modelo, use_container_width=True)

    arquivo = st.file_uploader("Selecionar arquivo", type=["xlsx", "csv"])
    if arquivo:
        try:
            if arquivo.name.endswith(".csv"):
                df_imp = pd.read_csv(arquivo, sep=None, engine="python")
            else:
                df_imp = pd.read_excel(arquivo)

            st.dataframe(df_imp.head(50), use_container_width=True)
            if st.button("Importar para o banco"):
                obrigatorias = ["data", "turno", "categoria", "indicador", "valor"]
                faltantes = [c for c in obrigatorias if c not in df_imp.columns]
                if faltantes:
                    st.error(f"Colunas obrigatórias ausentes: {faltantes}")
                    return

                importados = 0
                for _, row in df_imp.iterrows():
                    data_lanc = pd.to_datetime(row["data"], errors="coerce")
                    if pd.isna(data_lanc):
                        continue
                    data_lanc = data_lanc.date()
                    dados = (
                        data_lanc.isoformat(),
                        int(data_lanc.isocalendar().week),
                        int(data_lanc.month),
                        int(data_lanc.year),
                        str(row.get("turno", "")),
                        str(row.get("categoria", "")),
                        str(row.get("indicador", "")),
                        str(row.get("processo", "")),
                        float(row.get("valor", 0) or 0),
                        float(row.get("meta", 0) or 0),
                        str(row.get("unidade", "")),
                        str(row.get("responsavel", "")),
                        str(row.get("status", "Aberto")),
                        str(row.get("descricao", "")),
                        str(row.get("acao", "")),
                        datetime.now().isoformat(timespec="seconds"),
                    )
                    inserir_lancamento(dados)
                    importados += 1
                st.success(f"Importação concluída. Registros importados: {importados}")
                st.rerun()
        except Exception as e:
            st.error(f"Erro na importação: {e}")


def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    criar_banco()

    st.title(APP_TITLE)
    st.caption("Aplicação Streamlit com banco SQLite para uso em rede local.")

    df = carregar_dados()

    menu = st.sidebar.radio(
        "Menu",
        ["Dashboard", "Incluir lançamento", "Consultar / excluir", "Importar base"],
    )

    if menu == "Dashboard":
        pagina_dashboard(df)
    elif menu == "Incluir lançamento":
        pagina_lancamento()
    elif menu == "Consultar / excluir":
        pagina_edicao(df)
    elif menu == "Importar base":
        pagina_importacao()


if __name__ == "__main__":
    main()
