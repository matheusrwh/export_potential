import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

from warnings import filterwarnings
filterwarnings("ignore")

######## Setting the directories ########
def get_project_root():
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / 'app' / 'app.py').exists():
            return parent
    return Path.cwd()

project_root = get_project_root()
app = project_root / 'app'
data_raw = project_root / 'data' / 'raw'
data_processed = project_root / 'data' / 'processed'
data_interim = project_root / 'data' / 'interim'
references = project_root / 'references'

######## Setting the style ########
with open(app / "style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.set_page_config(
    page_title="Potencial de exportações - Observatório FIESC",
    page_icon=app / "logo_dark_mini.png",
    layout="wide"
)


def format_contabil(value):
    if value >= 1e9:
        return f"{value/1e9:,.1f} bi".replace(",", "X").replace(".", ",").replace("X", ".")
    elif value >= 1e6:
        return f"{value/1e6:,.1f} mi".replace(",", "X").replace(".", ",").replace("X", ".")
    elif value >= 1e3:
        return f"{value/1e3:,.1f} mil".replace(",", "X").replace(".", ",").replace("X", ".")
    else:
        return f"{value:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")

######## Loading the data ########
### Munic and VP list ###
df_munic_vp = pl.read_excel(references / 'munic_vp.xlsx')

vp = df_munic_vp['vp'].unique().to_list()
vp.sort()
munic = df_munic_vp['munic'].unique().to_list()
munic.sort()

### EPI scores SH6 ###
df_epi_sh6 = pl.read_parquet(app / 'data' / 'epi_scores_sh6.parquet')
df_epi_sh6.head()

df_epi_sh6 = df_epi_sh6.with_columns(
    pl.col("epi_score_normalized").round(3)
)

### EPI scores countries ###
df_epi_countries = pl.read_parquet(app / 'data' / 'epi_scores_countries.parquet')
df_epi_countries.head()

df_epi_countries = df_epi_countries.with_columns(
    pl.col("epi_score_normalized").round(3)
)

@st.cache_data(ttl=1800, show_spinner=False)
def load_epi():
    return pl.read_parquet(app / 'data' / 'epi_scores.parquet')

df_epi = load_epi()

df_epi = df_epi.with_columns(
    pl.col("epi_score_normalized").round(3)
)

### EPI scores SC Competitiva ###
df_epi_sc_comp = pl.read_parquet(app / 'data' / 'epi_scores_sc_comp.parquet')
df_epi_sc_comp.head()

df_epi_sc_comp = df_epi_sc_comp.with_columns(
    pl.col("epi_score_normalized").round(3)
)

@st.cache_data(ttl=1800, show_spinner=False)
def load_markets():
    return pl.read_parquet(app / 'data' / 'app_dataset.parquet')

df_markets = load_markets()

df_markets.head()
#df_markets.shape

df_markets = df_markets.select([
    pl.col('importer'),
    pl.col('country_name').alias('importer_name'),
    pl.col('sh6'),
    pl.col('description').alias('product_description_br'),
    pl.col('value'),
    pl.col('market_share'),
    pl.col('cagr_5y'),
    pl.col('share_brazil'),
    pl.col('share_sc'),
    pl.col('dist')
])

df_markets = df_markets.with_columns(
    (pl.col('sh6') + " - " + pl.col('product_description_br')).alias('sh6_product')
)

df_markets = df_markets.with_columns(
    pl.col("value").map_elements(format_contabil).alias("value_contabil"),
    pl.col("dist").map_elements(format_contabil).alias("dist")
)

@st.cache_resource(ttl=1800, show_spinner=False)
def load_competitors():
    return pl.read_parquet(app / 'data' / 'df_competitors.parquet')

df_competitors = load_competitors()


def format_decimal(value, decimals=1):
    return f"{value:.{decimals}f}".replace(".", ",")

df_markets = df_markets.with_columns(
    pl.col("cagr_5y").map_elements(lambda x: format_decimal(x, 1)).alias("cagr_5y_adj"),
    pl.col('market_share').map_elements(lambda x: format_decimal(x, 1)).alias('market_share'),
    pl.col('share_sc').map_elements(lambda x: format_decimal(x, 1)).alias('share_sc'),
    pl.col('share_brazil').map_elements(lambda x: format_decimal(x, 1)).alias('share_brazil'),
)

################## APP ########################
#### SIDEBAR ####
with st.sidebar:
    st.markdown(
        """
        <div style="display: flex; flex-direction: column; justify-content: center; height: 25vh;">
        """,
        unsafe_allow_html=True
    )
    st.header("Filtros")
    st.selectbox("Selecione a vice-presidência:", options=["Todos"] + vp)
    st.selectbox("Selecione o município:", options=["Todos"] + munic)
    st.image(app / "logo_dark.png")
    st.markdown("</div>", unsafe_allow_html=True)

st.title("Potencial de exportações")

st.markdown(
    "Indicador de potencial de exportações dos produtos catarinenses construído pelo Observatório FIESC.<br>"
    "Metodologia adaptada do indicador EPI (Export Potential Index) do ITC (International Trade Centre).<br>" \
    "O indicador incorpora fatores de oferta, demanda e facilidade de comércio para identificar o potencial de exportações por produto, setor e mercados.",
    unsafe_allow_html=True
)

tab1, tab2, tab3, tab4, tab5 = st.tabs(['Visão geral', 'Produtos e mercados', 'Fornecedores', 'Mapa tarifário', 'Metodologia'])




#### TAB 1 - PRODUTOS ####
with tab1:
    ### FIRST SECTION
    col1, col2 = st.columns([2, 1])
    with col1:
        # Gerar um dicionário de cores para cada categoria de 'sc_comp'
        sc_comp_unique = df_epi_sh6['sc_comp'].unique().to_list()
        color_map = {row['sc_comp']: row['color'] for row in df_epi_sh6.select(['sc_comp', 'color']).unique().to_dicts()}

        fig = px.treemap(
            df_epi_sh6.to_pandas().head(200),
            title="Produtos (SH6):",
            path=["sh6"],
            values="epi_score_normalized",
            color="sc_comp",
            hover_data={
                "product_description_br": True,
                "sh6": True,
                "epi_score_normalized": True,
                "sc_comp": True,
                "categoria": False
            },
            color_discrete_map=color_map
        )

        fig.update_traces(marker=dict(cornerradius=5))

        fig.update_traces(
            hovertemplate="<br>".join([
                "SH6: %{label}",
                "Descrição: %{customdata[0]}",
                "Índice PE: %{customdata[2]}",
                "SC Competitiva: %{customdata[3]}"
            ])
        )

        st.plotly_chart(fig, config={"responsive": True})

    with col2:
        df_sector = df_epi_sc_comp.sort('epi_score_normalized', descending=True).head(10)
        #df_sector = df_sector.reverse()

        fig_sector = px.bar(
            df_sector.to_pandas(),
            title="Setores SC Competitiva:",
            x="epi_score_normalized",
            y="sc_comp",
            orientation="h",
            labels={"sc_comp": "", "epi_score_normalized": "Potencial de exportação"},
            color="sc_comp",
            color_discrete_map=color_map
        )

        fig_sector.update_layout(showlegend=False)

        st.plotly_chart(fig_sector, config={"responsive": True})

    ### SECOND SECTION
    col3, col4 = st.columns([2, 0.675])

    with col3:
        fig_geo = px.scatter_geo(
            df_epi_countries.to_pandas(),
            title="Potencial de mercados:",
            locations="importer",
            locationmode="ISO-3",
            color="categoria",
            hover_name="importer",
            size="epi_score_normalized",
            projection="natural earth",
            color_discrete_sequence=px.colors.qualitative.Plotly,
            size_max=50,
            hover_data={
                "importer_name": True,
                "epi_score_normalized": True,
                "categoria": False,
                "importer": False
            }
        )

        fig_geo.update_geos(
            showcountries=True,
            countrycolor="white",  
            showland=True,
            landcolor="#595959",   
            bgcolor="#0e1117",     
            showcoastlines=True,
            coastlinecolor="white", 
            countrywidth=0.1,      
            coastlinewidth=0.1
        )

        fig_geo.update_traces(
            hovertemplate="<br>".join([
            "País: %{customdata[0]}",
            "Índice PE: %{customdata[1]}"
            ])
        )

        fig_geo.update_layout(
            width=1200,
            height=600,
            legend=dict(
            title="Categoria",
            orientation="v",
            x=-0.02,
            y=1,
            bgcolor='rgba(0,0,0,0)'
            )
        )
        
        st.plotly_chart(fig_geo, config={"responsive": True})

    with col4:
        st.markdown("<div style='margin-top: 110px;'></div>", unsafe_allow_html=True)
        st.dataframe(
            df_epi_countries.select([
                pl.col('importer_name').alias('País'),
                pl.col('epi_score_normalized').alias('Índice PE'),
                pl.col('categoria').alias('Categoria')
            ]).to_pandas().head(50),
            width='stretch',
            hide_index=True
        )
    
    st.markdown("<hr style='margin-top: -50px; margin-bottom: 0;'>", unsafe_allow_html=True)
    st.markdown("<div style='margin-top: -55px;'></div><span style='font-size:14px;'><b>Fonte:</b> CEPII (2023) e Observatório FIESC (2025).</span>", unsafe_allow_html=True)











#### TAB 2 - PRODUTOS E MERCADOS ####
with tab2:
    sh6_options = sorted([opt for opt in df_epi["sh6_product"].unique().to_list() if opt is not None])
    selected_sh6 = st.selectbox("**Selecione o código SH6:**", sh6_options, key="sh6_selectbox_tab2")

    ### Columns for layout
    col1, col2 = st.columns([0.8, 1])
    
    with col1:
        df_selected = df_epi.filter(pl.col("sh6_product") == selected_sh6).sort("epi_score_normalized", descending=True)
        df_selected_pd = df_selected.to_pandas().head(25).sort_values("epi_score_normalized", ascending=True)
        df_selected_pd_map = df_selected.to_pandas().sort_values("epi_score_normalized", ascending=False)

        df_selected_markets = df_markets.filter(pl.col("sh6_product") == selected_sh6).sort("value", descending=True)

        fig = go.Figure()

        # Bar for EPI index (primary x-axis)
        fig.add_trace(
            go.Bar(
            x=df_selected_pd["epi_score_normalized"],
            y=df_selected_pd["importer_name"],
            orientation="h",
            name="Índice PE",
            marker_color=px.colors.qualitative.Plotly[0],
            hovertemplate="País: %{y}<br>Índice PE: %{x}<extra></extra>",
            xaxis="x",
            )
        )

        # Scatter for bilateral exports (secondary x-axis)
        fig.add_trace(
            go.Scatter(
            x=df_selected_pd["bilateral_exports_sc_sh6"],
            y=df_selected_pd["importer_name"],
            mode="markers+lines",
            name="Exportações de SC",
            marker=dict(size=10, color=px.colors.qualitative.Plotly[1], symbol="circle"),
            hovertemplate="País: %{y}<br>Exportações SC: %{x}<extra></extra>",
            xaxis="x2"
            )
        )

        fig.update_layout(
            title="Índice PE e importações dos produtos catarinenses:",
            xaxis=dict(
            title="Índice PE",
            side="bottom",
            showgrid=False
            ),
            xaxis2=dict(
            title="Montante importado de Santa Catarina (US$ FOB)",
            overlaying="x",
            side="top",
            showgrid=False,
            position=0.98
            ),
            legend=dict(
            orientation="h",
            yanchor="bottom",
            y=0,
            xanchor="center",
            x=0.5,
            bgcolor='rgba(0,0,0,0)'
            ),
            height=800,
            margin=dict(l=0, r=0, t=140, b=0)  # Increased top margin to 140 for more spacing below the title
        )

        st.plotly_chart(fig, config={"responsive": True})
    with col2:
        st.markdown("<div style='margin-top: 180px;'></div>", unsafe_allow_html=True)
        total_imports = df_selected_markets['value'].sum()
        st.markdown(f"**Mercado mundial do produto (2023):**<br><span style='font-size:24px; font-weight:bold;'>US$ {format_contabil(total_imports)}</span>", unsafe_allow_html=True)
        
        # Adiciona coluna de posição relativa (ranking)
        df_selected_markets = df_selected_markets.with_columns(
            (pl.arange(1, df_selected_markets.height + 1)).alias("Posição")
        )

        st.dataframe(
            df_selected_markets.select([
            pl.col('Posição'),
            pl.col('importer_name').alias("País"),
            pl.col('value_contabil').alias("Montante US$"),
            pl.col('market_share').alias("Market Share (%)"),
            pl.col('cagr_5y_adj').alias("CAGR 5 anos (%)"),
            pl.col('share_brazil').alias("Share Brasil (%)"),
            pl.col('share_sc').alias("Share SC (%)"),
            pl.col('dist').alias("Distância (km)")
            ]),
            width='stretch',
            hide_index=True
        )

        st.markdown(
            "<div style='margin-top: -15px;'></div>"
            "<span style='font-size:14px;'><b>Nota:</b> CAGR 5 anos (%) refere-se ao crescimento anual composto das importações nos últimos 5 anos.</span>",
            unsafe_allow_html=True
        )
    
    #################### MAPA ####################
    st.markdown("<div style='margin-top: 5px; margin-bottom: 10px;'></div>", unsafe_allow_html=True)
    fig_geo_prod = px.scatter_geo(
        df_selected_pd_map,
        locations="importer",
        locationmode="ISO-3",
        hover_name="importer",
        color='categoria',
        size="epi_score_normalized",
        projection="natural earth",
        color_discrete_sequence=px.colors.qualitative.Plotly,
        size_max=65,
        hover_data={
            "importer_name": True,
            "epi_score_normalized": True,
            "importer": False
        }
    )

    fig_geo_prod.update_geos(
        showcountries=True,
        countrycolor="white",  
        showland=True,
        landcolor="#595959",   
        bgcolor="#0e1117",     
        showcoastlines=True,
        coastlinecolor="white", 
        countrywidth=0.1,      
        coastlinewidth=0.1
    )

    fig_geo_prod.update_traces(
        hovertemplate="<br>".join([
        "País: %{customdata[0]}",
        "Índice PE: %{customdata[1]}"
        ])
    )

    fig_geo_prod.update_layout(
        width=1200,
        height=600,
        title=f"Distribuição geográfica do Índice PE:",
        legend=dict(
        title=None,
        orientation="h",
        x=0.33,
        y=0,
        bgcolor='rgba(0,0,0,0)'
        ),
        margin=dict(t=40)  # Reduce top margin
    )

    st.plotly_chart(fig_geo_prod, config={"responsive": True})

    st.markdown("<hr style='margin-top: -50px; margin-bottom: 0;'>", unsafe_allow_html=True)
    st.markdown("<div style='margin-top: -55px;'></div><span style='font-size:14px;'><b>Fonte:</b> CEPII (2023) e Observatório FIESC (2025).</span>", unsafe_allow_html=True)







    
#### TAB 3 - FORNECEDORES ####
with tab3:
    # --- Cache unique values ---
    @st.cache_data(show_spinner=False)
    def get_unique_options(df: pl.DataFrame):
        """
        Return sorted unique lists for importer_name and sh6_product.
        This will only recompute when df_competitors changes.
        """
        countries = (
            df.select(pl.col("importer_name").drop_nulls().unique().sort())
            .to_series().to_list()
        )
        products = (
            df.select(pl.col("sh6_product").drop_nulls().unique().sort())
            .to_series().to_list()
        )
        return countries, products
    

    countries, products = get_unique_options(df_competitors)
    col1, col2 = st.columns([0.8, 1])

    with col1:
        sel_country = st.selectbox(
            "*Selecione o país:*",
            options=countries,
            key="country_selectbox_tab3"
        )

    with col2:
        sel_product = st.selectbox(
            "*Selecione o produto (SH6):*",
            options=products,
            key="product_selectbox_tab3"
        )

    df_competitors_filtered = (
        df_competitors
        .filter(
            (pl.col("importer_name") == sel_country) &
            (pl.col("sh6_product") == sel_product)
        )
        .sort("value", descending=True)
    )

    total_imports = df_competitors_filtered.select(pl.col("value").sum()).item()

    # ==== FIRST SECTION (fast + categorical-proof via graph_objects) ====
    import plotly.graph_objects as go
    from itertools import cycle
    import polars as pl

    col3, col4 = st.columns([2, 1.25])

    with col3:
        df_treemap_pl = (
            df_competitors_filtered
            .select([
                "exporter_name", "sh6", "value",
                "product_description_br", "value_contabil"
            ])
            .filter(pl.col("exporter_name").is_not_null() & pl.col("sh6").is_not_null())
            .head(200)
        )

        st.markdown("<div style='margin-top: 40px;'></div>", unsafe_allow_html=True)


        if df_treemap_pl.height == 0:
            st.info("Sem dados para este país/produto.")
        else:
            # Optional: join your preferred colors per exporter (fallback palette if missing)
            # df_epi_sh6 must have ['exporter_name','color'] if you want custom colors
            has_colors = "df_epi_sh6" in globals() and isinstance(df_epi_sh6, pl.DataFrame) \
                        and set(["exporter_name","color"]).issubset(df_epi_sh6.columns)

            if has_colors:
                df_plot = (
                    df_treemap_pl.join(
                        df_epi_sh6.select(["exporter_name","color"]).unique(),
                        on="exporter_name",
                        how="left"
                    )
                )
            else:
                df_plot = df_treemap_pl

            # Convert columns to Python lists (no pandas/categoricals involved)
            exporters  = df_plot.get_column("exporter_name").to_list()
            sh6_arr    = df_plot.get_column("sh6").to_list()
            values     = df_plot.get_column("value").to_list()
            descr      = df_plot.get_column("product_description_br").to_list()
            v_text     = df_plot.get_column("value_contabil").to_list()

            # Colors: use provided colors if present; otherwise generate a palette
            provided_colors = df_plot.get_column("color").to_list() if has_colors else [None] * len(exporters)
            if any(c is not None for c in provided_colors):
                node_colors = [c if c is not None else "#8FA5FF" for c in provided_colors]
            else:
                # deterministic palette by exporter name
                palette = (
                    ['#23CCA1', '#E24B5E', '#EAD97F', '#4FD1C5', '#8FA5FF',
                    '#B388EB', '#FFA07A', '#7FB77E', '#F6C85F', '#9FD3C7']
                )
                cyc = cycle(palette)
                # unique exporters -> color
                uniq = {}
                node_colors = []
                for name in exporters:
                    if name not in uniq:
                        uniq[name] = next(cyc)
                    node_colors.append(uniq[name])

            # Customdata for hover: [descr, sh6, value_contabil]
            import numpy as np
            customdata = np.column_stack([descr, sh6_arr, v_text]) if exporters else np.empty((0,3))

            fig = go.Figure(
                go.Treemap(
                    labels=exporters,
                    parents=[""] * len(exporters),  # remove parent label from nodes
                    values=values,
                    branchvalues="total",
                    marker=dict(
                        colors=node_colors,
                        line=dict(width=0.5, color="rgba(255,255,255,0.15)"),
                    ),
                    tiling=dict(pad=2),
                    textinfo="label+value",
                    texttemplate="%{label}<br>%{value:.2s}",
                    hovertemplate="<br>".join([
                        "Exportador: %{label}",
                        "SH6: %{customdata[1]}",
                        "Descrição: %{customdata[0]}",
                        "Valor importado: US$ %{customdata[2]}",
                        "<extra></extra>"
                    ]),
                    customdata=customdata,
                )
            )
            fig.update_layout(
                title="Países fornecedores (2023):",
                margin=dict(l=0, r=0, t=40, b=0),
            )

            st.plotly_chart(fig, config={"responsive": True})
    
    with col4:
        st.markdown("<div style='margin-top: 0px;'></div>", unsafe_allow_html=True)
        st.markdown(f"**Total importado (2023):**<br><span style='font-size:24px; font-weight:bold;'>US$ {format_contabil(total_imports)}</span>", unsafe_allow_html=True)
        
        # Adiciona coluna de posição relativa (ranking)
        df_competitors_filtered = df_competitors_filtered.with_columns(
            (pl.arange(1, df_competitors_filtered.height + 1)).alias("Posição")
        )

        st.dataframe(
            df_competitors_filtered.select([
            pl.col('Posição'),
            pl.col('exporter_name').alias('País fornecedor'),
            pl.col('value_contabil').alias('Montante US$'),
            pl.col('importer_sh6_share').alias('Share (%)'),
            pl.col('cagr_5y_adj').alias('CAGR 5 anos (%)')
            ]).to_pandas().head(50),
            width='stretch',
            hide_index=True
        )
        st.markdown(
            "<div style='margin-top: -15px;'></div>"
            "<span style='font-size:14px;'><b>Nota:</b> CAGR 5 anos (%) refere-se ao crescimento anual composto das importações nos últimos 5 anos.</span>",
            unsafe_allow_html=True
        )

    st.markdown("<div style='margin-top: 5px; margin-bottom: 10px;'></div>", unsafe_allow_html=True)
    
    # Mapa de distribuição das importações por país para os filtros feitos
    # Use config argument instead of deprecated keyword arguments in st.plotly_chart
    fig_geo_imports = px.scatter_geo(
        df_competitors_filtered.to_pandas(),
        locations="exporter",
        locationmode="ISO-3",
        hover_name="exporter_name",
        size="value",
        projection="natural earth",
        color_discrete_sequence=px.colors.qualitative.Plotly,
        size_max=50,
        hover_data={
            "exporter_name": True,
            "value_contabil": True,
            "exporter": False
        }
    )

    fig_geo_imports.update_geos(
        showcountries=True,
        countrycolor="white",
        showland=True,
        landcolor="#595959",
        bgcolor="#0e1117",
        showcoastlines=True,
        coastlinecolor="white",
        countrywidth=0.1,
        coastlinewidth=0.1
    )

    fig_geo_imports.update_traces(
        hovertemplate="<br>".join([
            "País fornecedor: %{customdata[0]}",
            "Valor importado: US$ %{customdata[1]}"
        ])
    )

    fig_geo_imports.update_layout(
        width=1200,
        height=600,
        showlegend=False
    )

    st.plotly_chart(fig_geo_imports, config={"responsive": True})

    st.markdown("<hr style='margin-top: -50px; margin-bottom: 0;'>", unsafe_allow_html=True)
    st.markdown("<div style='margin-top: -55px;'></div><span style='font-size:14px;'><b>Fonte:</b> CEPII (2023) e Observatório FIESC (2025).</span>", unsafe_allow_html=True)
