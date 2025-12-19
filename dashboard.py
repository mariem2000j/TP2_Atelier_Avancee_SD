import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- PARTIE 3 : Construction du Cube OLAP ---
@st.cache_data
def load_and_merge_data():
    # Chargement des données
    try:
        dim_clients = pd.read_csv('Dim_Clients.csv')
        dim_produits = pd.read_csv('Dim_Produits.csv')
        dim_temps = pd.read_csv('Dim_Temps.csv')
        fact_ventes = pd.read_csv('Fact_Ventes.csv')
    except FileNotFoundError:
        st.error("Erreur: Les fichiers CSV de l'entrepôt de données sont manquants. Veuillez exécuter etl_script.py d'abord.")
        return None

    # Conversion des types pour la fusion
    fact_ventes['client_id'] = pd.to_numeric(fact_ventes['client_id'])
    fact_ventes['produit_id'] = pd.to_numeric(fact_ventes['produit_id'])
    
    dim_clients['client_id'] = pd.to_numeric(dim_clients['client_id'])
    dim_produits['produit_id'] = pd.to_numeric(dim_produits['produit_id'])
    
    # Assurer que les dates sont bien au format datetime
    fact_ventes['date'] = pd.to_datetime(fact_ventes['date'])
    dim_temps['date'] = pd.to_datetime(dim_temps['date'])

    # Fusion (Merge) pour créer le Cube
    # Fact -> Clients
    df_cube = pd.merge(fact_ventes, dim_clients, on='client_id', how='left')
    # -> Produits
    df_cube = pd.merge(df_cube, dim_produits, on='produit_id', how='left')
    # -> Temps
    df_cube = pd.merge(df_cube, dim_temps, on='date', how='left')
    
    return df_cube

# --- PARTIE 4 : Tableau de bord interactif ---
def main():
    st.set_page_config(page_title="Dashboard Ventes", layout="wide")
    
    st.title("📊 TP Business Intelligence - Analyse des Ventes")
    
    df = load_and_merge_data()
    
    if df is not None:
        # --- Sidebar : Filtres Interactifs ---
        st.sidebar.header("Filtres")
        
        # Filtre Année
        annees_dispo = sorted(df['annee'].dropna().unique())
        selected_year = st.sidebar.multiselect("Année", annees_dispo, default=annees_dispo)
        
        # Filtre Pays
        pays_dispo = sorted(df['pays'].dropna().astype(str).unique())
        selected_country = st.sidebar.multiselect("Pays", pays_dispo, default=pays_dispo)
        
        # Filtre Catégorie Produit
        # dropna() pour enlever les NaN (float) qui causent l'erreur avec sorted() sur des strings
        categories_dispo = sorted(df['categorie'].dropna().astype(str).unique())
        selected_category = st.sidebar.multiselect("Catégorie", categories_dispo, default=categories_dispo)
        
        # Application des filtres
        filtered_df = df[
            (df['annee'].isin(selected_year)) &
            (df['pays'].isin(selected_country)) &
            (df['categorie'].isin(selected_category))
        ]
        
        # --- KPIs ---
        total_ventes = filtered_df['montant'].sum()
        total_quantite = filtered_df['quantite'].sum()
        prix_moyen = filtered_df['prix'].mean()
        
        col1, col2, col3 = st.columns(3)
        col1.metric("💰 Chiffre d'Affaires Total", f"{total_ventes:,.2f} €")
        col2.metric("📦 Quantité Vendue", f"{total_quantite:,.0f}")
        col3.metric("🏷️ Prix Moyen Produit", f"{prix_moyen:,.2f} €")
        
        st.divider()

        # --- Visualisations ---
        
        # 1. Ventes par Région (Pays/Ville) et Période
        st.subheader("1. Analyse Géographique et Temporelle")
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            ventes_par_pays = filtered_df.groupby('pays')['montant'].sum().reset_index()
            fig_pays = px.bar(ventes_par_pays, x='pays', y='montant', title="Ventes par Pays", color='montant')
            st.plotly_chart(fig_pays, use_container_width=True)
            
        with col_chart2:
            # Ventes par mois
            ventes_par_mois = filtered_df.groupby('mois')['montant'].sum().reset_index()
            fig_temps = px.line(ventes_par_mois, x='mois', y='montant', title="Évolution des Ventes (par Mois)", markers=True)
            st.plotly_chart(fig_temps, use_container_width=True)

        # 2. Les produits les plus vendus
        st.subheader("2. Top Produits")
        top_produits = filtered_df.groupby('nom_produit')['quantite'].sum().reset_index().sort_values('quantite', ascending=False).head(10)
        fig_prod = px.bar(top_produits, x='quantite', y='nom_produit', orientation='h', title="Top 10 Produits (Quantité)", color='quantite')
        fig_prod.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_prod, use_container_width=True)

        col_chart3, col_chart4 = st.columns(2)

        # 3. Répartition des ventes par catégorie de client (segment)
        with col_chart3:
            st.subheader("3. Segmentation Client")
            ventes_par_segment = filtered_df.groupby('segment')['montant'].sum().reset_index()
            fig_seg = px.pie(ventes_par_segment, values='montant', names='segment', title="Répartition des Ventes par Segment")
            st.plotly_chart(fig_seg, use_container_width=True)

        # 4. Indicateur Top 5 Clients
        with col_chart4:
            st.subheader("4. Top 5 Clients Fidèles")
            top_clients = filtered_df.groupby('nom')['montant'].sum().reset_index().sort_values('montant', ascending=False).head(5)
            st.dataframe(top_clients.style.format({"montant": "{:.2f} €"}), use_container_width=True)

if __name__ == "__main__":
    main()
