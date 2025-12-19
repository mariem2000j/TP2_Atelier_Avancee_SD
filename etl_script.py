import pandas as pd
import os

def run_etl():
    print("Début du processus ETL...")
    
    # --- PARTIE 1 : Extraction et Transformation ---
    
    # 1. Importation des fichiers CSV
    print("Chargement des données...")
    import csv
    try:
        # Utilisation de utf-8-sig pour gérer le BOM
        # quoting=csv.QUOTE_NONE car chaque ligne semble être entièrement entre guillemets
        clients_df = pd.read_csv('Clients.csv', quoting=csv.QUOTE_NONE, encoding='utf-8-sig')
        produits_df = pd.read_csv('Produits.csv', quoting=csv.QUOTE_NONE, encoding='utf-8-sig')
        ventes_df = pd.read_csv('Ventes.csv', quoting=csv.QUOTE_NONE, encoding='utf-8-sig')
        
        # Fonction de nettoyage
        def clean_quotes(df):
            df.columns = df.columns.astype(str).str.replace('"', '').str.strip()
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].astype(str).str.replace('"', '').str.strip()
            return df

        clients_df = clean_quotes(clients_df)
        produits_df = clean_quotes(produits_df)
        ventes_df = clean_quotes(ventes_df)
        
        # Convertir les types numériques si nécessaire (car tout est peut-être string à cause du quoting)
        # Ventes: client_id, produit_id, quantite, montant -> numeric
        # Clients: client_id -> numeric
        # Produits: produit_id, prix -> numeric
        
        clients_df['client_id'] = pd.to_numeric(clients_df['client_id'])
        
        produits_df['produit_id'] = pd.to_numeric(produits_df['produit_id'])
        produits_df['prix'] = pd.to_numeric(produits_df['prix'])
        
        ventes_df['vente_id'] = pd.to_numeric(ventes_df['vente_id'])
        ventes_df['client_id'] = pd.to_numeric(ventes_df['client_id'])
        ventes_df['produit_id'] = pd.to_numeric(ventes_df['produit_id'])
        ventes_df['quantite'] = pd.to_numeric(ventes_df['quantite'])
        ventes_df['montant'] = pd.to_numeric(ventes_df['montant'])
        
        ventes_df['date'] = pd.to_datetime(ventes_df['date'], errors='coerce')
        ventes_df.dropna(subset=['date'], inplace=True)

    except Exception as e:
        print(f"Erreur lors du chargement ou du nettoyage initial : {e}")
        import traceback
        traceback.print_exc()
        return

    # 2. Nettoyage des données
    print("Nettoyage des données...")
    
    # Suppression des valeurs nulles
    clients_df.dropna(inplace=True)
    produits_df.dropna(inplace=True)
    ventes_df.dropna(inplace=True)
    
    # Standardisation des noms de pays
    # Exemple : "États-Unis", "United States" -> "USA"
    country_mapping = {
        "États-Unis": "USA",
        "United States": "USA",
        "US": "USA"
    }
    clients_df['pays'] = clients_df['pays'].replace(country_mapping)
    
    # Suppression des doublons
    clients_df.drop_duplicates(inplace=True)
    produits_df.drop_duplicates(inplace=True)
    ventes_df.drop_duplicates(inplace=True)

    print("Données nettoyées.")

    # --- PARTIE 2 : Construction de l'Entrepôt de Données (Schéma en Étoile) ---
    print("Construction du Data Warehouse...")

    # Table de dimension : Dim_Clients
    dim_clients = clients_df[['client_id', 'nom', 'ville', 'pays', 'segment']].copy()
    
    # Table de dimension : Dim_Produits
    dim_produits = produits_df[['produit_id', 'nom_produit', 'categorie', 'prix']].copy()
    
    # Table de dimension : Dim_Temps
    # Extraire les dates uniques des ventes pour créer la dimension temps
    dates_uniques = pd.to_datetime(ventes_df['date']).unique()
    dim_temps = pd.DataFrame({'date': dates_uniques})
    dim_temps['date'] = pd.to_datetime(dim_temps['date'])
    dim_temps['annee'] = dim_temps['date'].dt.year
    dim_temps['mois'] = dim_temps['date'].dt.month
    dim_temps['jour'] = dim_temps['date'].dt.day
    dim_temps['trimestre'] = dim_temps['date'].dt.quarter
    dim_temps.sort_values('date', inplace=True)
    
    # Table de fait : Fact_Ventes
    # On s'assure que 'date' est bien au format datetime pour la cohérence
    ventes_df['date'] = pd.to_datetime(ventes_df['date'])
    fact_ventes = ventes_df[['vente_id', 'client_id', 'produit_id', 'date', 'quantite', 'montant']].copy()

    # 3. Chargement (Exportation vers CSV pour simuler l'entrepôt)
    print("Exportation des tables du Data Warehouse...")
    dim_clients.to_csv('Dim_Clients.csv', index=False)
    dim_produits.to_csv('Dim_Produits.csv', index=False)
    dim_temps.to_csv('Dim_Temps.csv', index=False)
    fact_ventes.to_csv('Fact_Ventes.csv', index=False)
    
    print("ETL terminé avec succès. Les fichiers 'Dim_*.csv' et 'Fact_*.csv' ont été créés.")

if __name__ == "__main__":
    run_etl()
