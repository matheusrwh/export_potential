import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
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

df_epi_countries = pl.read_parquet(app / 'data' / 'epi_scores_countries.parquet')
df_epi_countries.head()

# Limitar epi_score_normalized a duas casas após a vírgula
df_epi_sh6 = df_epi_sh6.with_columns(
    pl.col("epi_score_normalized").round(2)
)
df_epi_countries = df_epi_countries.with_columns(
    pl.col("epi_score_normalized").round(2)
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
    "O indicador incorpora fatores de oferta, demanda e facilidade de comércio para identificar o potencial de exportações por produto, setor e mercados. Mais detalhes na aba Metodologia.",
    unsafe_allow_html=True
)

tab1, tab2, tab3, tab4 = st.tabs(['Produtos', 'Mercados', 'Fornecedores', 'Metodologia'])

#### TAB 1 - PRODUTOS ####
with tab1:
    ### FIRST SECTION
    col1, col2 = st.columns([2, 1])
    with col1:
        fig = px.treemap(
            df_epi_sh6.to_pandas().head(30),
            title="Produtos (SH6):",
            path=["sh6"],
            values="epi_score_normalized",
            color="categoria",
            color_discrete_sequence=px.colors.qualitative.Plotly)
        fig.update_traces(marker=dict(cornerradius=5))
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
    
    #st.markdown("<hr style='margin-top: 0px; margin-bottom: 10px;'>", unsafe_allow_html=True)
    
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
            size_max=50
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
            use_container_width=False,
            hide_index=True
        )
    
    st.markdown("<hr style='margin-top: -35px; margin-bottom: 10px;'>", unsafe_allow_html=True)
