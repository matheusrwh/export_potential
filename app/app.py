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

### EPI scores ###
df_epi = pl.read_parquet(app / 'data' / 'epi_scores.parquet')
df_epi.head()

df_epi = df_epi.with_columns(
    pl.col("epi_score_normalized").round(3)
)


### Mercados mundiais ###
df_markets = pl.read_parquet(app / 'data' / 'app_dataset.parquet')

df_markets.head()

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
            color_discrete_sequence=px.colors.qualitative.Plotly
        )

        fig.update_traces(marker=dict(cornerradius=5))

        # Remove parent and id from hovertemplate
        fig.update_traces(
            hovertemplate="<br>".join([
                "SH6: %{label}",
                "Descrição: %{customdata[0]}",
                "Índice PE: %{customdata[2]}",
                "SC Competitiva: %{customdata[3]}"
            ])
        )

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        df_sectors = pd.DataFrame({
            "names": [
            "Madeira e móveis", "Alimentos e bebidas", "Máquinas e equipamentos", 
            "Produtos químicos", "Veículos automotores", "Têxtil e vestuário", 
            "Metalurgia", "Plásticos e borracha", "Construção civil", 
            "Papel e celulose", "Eletroeletrônicos", "Agropecuária"
            ],
            "values": [34, 16, 14, 10, 8, 12, 9, 7, 6, 11, 5, 13]
        })
        
        df_sectors_sorted = df_sectors.sort_values("values", ascending=True)
        
        second_color = px.colors.qualitative.Plotly[0]
        fig_sector = px.bar(
            df_sectors_sorted,
            title="Setores SC Competitiva:",
            x="values",
            y="names",
            orientation="h",
            labels={"names": "","values": "Potencial de exportação"},
            color_discrete_sequence=[second_color]
        )
        
        st.plotly_chart(fig_sector, use_container_width=True)

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
            landcolor="#363d49",   
            bgcolor="#0e1117",     
            showcoastlines=True,
            coastlinecolor="white", 
            countrywidth=0.4,      
            coastlinewidth=0.4
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
            x=0,
            y=1,
            bgcolor='rgba(0,0,0,0)'
            )
        )
        
        st.plotly_chart(fig_geo, use_container_width=True)

    with col4:
        st.markdown("<div style='margin-top: 110px;'></div>", unsafe_allow_html=True)
        st.dataframe(
            df_epi_countries.select([
                pl.col('importer_name').alias('País'),
                pl.col('epi_score_normalized').alias('Índice PE'),
                pl.col('categoria').alias('Categoria')
            ]).to_pandas().head(50),
            use_container_width=True,
            hide_index=True
        )
    
    st.markdown("<hr style='margin-top: -35px; margin-bottom: 10px;'>", unsafe_allow_html=True)













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
        second_color = px.colors.qualitative.Plotly[0]
        fig.add_trace(
            go.Bar(
            x=df_selected_pd["epi_score_normalized"],
            y=df_selected_pd["importer_name"],
            orientation="h",
            name="Índice PE",
            marker_color=second_color,
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

        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.markdown("<div style='margin-top: 200px;'></div>", unsafe_allow_html=True)
        st.markdown("**Mercado mundial do produto:**")
        st.dataframe(
            df_selected_markets.select([
                pl.col('importer_name').alias("País"),
                pl.col('value_contabil').alias("Montante US$"),
                pl.col('market_share').alias("Market Share (%)"),
                pl.col('cagr_5y_adj').alias("CAGR 5 anos (%)"),
                pl.col('share_brazil').alias("Share Brasil (%)"),
                pl.col('share_sc').alias("Share SC (%)"),
                pl.col('dist').alias("Distância (km)")
            ])
        )

        st.markdown(
            "<div style='margin-top: -15px;'></div>"
            "<span style='font-size:14px;'>Nota: CAGR 5 anos (%) refere-se ao crescimento anual composto das importações do país no produto nos últimos 5 anos.</span>",
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
        size_max=50,
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
        landcolor="#363d49",   
        bgcolor="#0e1117",     
        showcoastlines=True,
        coastlinecolor="white", 
        countrywidth=0.4,      
        coastlinewidth=0.4
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
    
    st.plotly_chart(fig_geo_prod, use_container_width=True)

    st.markdown("<hr style='margin-top: -40px; margin-bottom: 10px;'>", unsafe_allow_html=True)