import streamlit as st
import snowflake.connector
import plotly.graph_objects as go 


# Fonction pour lister les datawarehouses existants
def list_datawarehouses(conn):
    try:
        query = "SHOW WAREHOUSES"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        warehouse_names = [row[0] for row in results]  
        return warehouse_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des datawarehouses : {e}")
        return []

# Fonction pour créer un nouveau datawarehouse
def create_datawarehouse(conn, warehouse_name):
    try:
        query = f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  # Commit the transaction
        st.success(f"Datawarehouse '{warehouse_name}' créé avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création du datawarehouse '{warehouse_name}': {e}")

# Fonction pour lister les bases de données existantes
def list_databases(conn):
    try:
        query = "SHOW DATABASES"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        database_names = [row[1] for row in results]  # Assuming database name is at index 1
        return database_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des bases de données : {e}")
        return []

# Fonction pour créer une nouvelle base de données
def create_database(conn, database_name):
    try:
        query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  # Commit the transaction
        st.success(f"Base de données '{database_name}' créée avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création de la base de données '{database_name}': {e}")

# Fonction pour lister les schémas existants dans une base de données spécifique
def list_schemas(conn, database_name):
    try:
        query = f"SHOW SCHEMAS IN DATABASE {database_name}"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        schema_names = [row[1] for row in results]  # Assuming schema name is at index 1
        return schema_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des schémas : {e}")
        return []

# Fonction pour créer un nouveau schéma dans une base de données spécifique
def create_schema(conn, database_name, schema_name):
    try:
        query = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  # Commit the transaction
        st.success(f"Schéma '{schema_name}' créé avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création du schéma '{schema_name}': {e}")

# Fonction pour lister les tables existantes dans un schéma spécifique
def list_tables(conn, database_name, schema_name):
    try:
        query = f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        table_names = [row[1] for row in results]  # Assuming table name is at index 1
        return table_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des tables : {e}")
        return []

# Fonction pour créer une nouvelle table dans un schéma spécifique
# Fonction pour créer une nouvelle table dans un schéma spécifique
def create_table(conn, database_name, schema_name, table_name, columns):
    try:
        query = f"CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} ({columns})"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  # Commit the transaction
        st.success(f"Table '{table_name}' créée avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création de la table '{table_name}': {e}")

# Fonction pour lire les données d'une table spécifique
def read_table(conn, database_name, schema_name, table_name):
    try:
        query = f"SELECT * FROM {database_name}.{schema_name}.{table_name}"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        return results
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la lecture de la table '{table_name}': {e}")
        return []

# Fonction pour mettre à jour une table spécifique
def update_table(conn, database_name, schema_name, table_name, column_name, new_value, condition):
    try:
        query = f"UPDATE {database_name}.{schema_name}.{table_name} SET {column_name} = '{new_value}' WHERE {condition}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  # Commit the transaction
        st.success("Table mise à jour avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la mise à jour de la table '{table_name}': {e}")

# Fonction pour supprimer une table spécifique
def delete_table(conn, database_name, schema_name, table_name):
    try:
        query = f"DROP TABLE {database_name}.{schema_name}.{table_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  # Commit the transaction
        st.success(f"Table '{table_name}' supprimée avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la suppression de la table '{table_name}': {e}")
# Fonctions pour récupérer les données
def count_elements(conn, selected_database):
    warehouse_count = len(list_datawarehouses(conn))
    database_count = len(list_databases(conn))
    schema_count = len(list_schemas(conn, selected_database))
    return warehouse_count, database_count, schema_count


# Connexion à Snowflake
@st.cache(allow_output_mutation=True)
def connect_to_snowflake(user, password, account):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account
        )
        return conn
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur de connexion à Snowflake : {e}")

# Affichage dans Streamlit
def main():
    st.title("Gestion des Datawarehouses Snowflake")

    # Paramètres de connexion
    user = st.sidebar.text_input("Utilisateur")
    password = st.sidebar.text_input("Mot de passe", type="password")
    account = st.sidebar.text_input("Compte")

    if st.sidebar.button("Se connecter"):
        conn = connect_to_snowflake(user, password, account)
        st.session_state['conn'] = conn
        st.success("Connexion réussie")

    if 'conn' in st.session_state:
        conn = st.session_state['conn']

        # Liste des datawarehouses
        st.header("Liste des Datawarehouses")
        warehouses = list_datawarehouses(conn)
        st.write(warehouses)

        # Créer un nouveau datawarehouse
        st.header("Créer un Nouveau Datawarehouse")
        new_warehouse_name = st.text_input("Nom du nouveau datawarehouse")
        if st.button("Créer Datawarehouse"):
            create_datawarehouse(conn, new_warehouse_name)

        # Liste des bases de données
        st.header("Liste des Bases de Données")
        databases = list_databases(conn)
        st.write(databases)

        # Créer une nouvelle base de données
        st.header("Créer une Nouvelle Base de Données")
        new_database_name = st.text_input("Nom de la nouvelle base de données")
        if st.button("Créer Base de Données"):
            create_database(conn, new_database_name)

        # Liste des schémas dans une base de données spécifique
        st.header("Liste des Schémas dans une Base de Données")
        selected_database = st.selectbox("Sélectionnez une base de données", databases)
        schemas = list_schemas(conn, selected_database)
        st.write(schemas)

        # Créer un nouveau schéma dans une base de données spécifique
        st.header("Créer un Nouveau Schéma dans une Base de Données")
        new_schema_name = st.text_input("Nom du nouveau schéma")
        if st.button("Créer Schéma"):
            create_schema(conn, selected_database, new_schema_name)

        # Liste des tables dans un schéma spécifique
        st.header("Liste des Tables dans un Schéma")
        selected_schema = st.selectbox("Sélectionnez un schéma", schemas)
        tables = list_tables(conn, selected_database, selected_schema)
        st.write(tables)
        # Opérations CRUD
        operation = st.selectbox("Sélectionnez une opération", ["Créer", "Lire", "Mettre à jour", "Supprimer"])

        if operation == "Créer":
            # Afficher les champs pour saisir le nom et les colonnes de la nouvelle table
            st.header("Création d'une Nouvelle Table dans un Schéma")
            new_table_name = st.text_input("Nom de la nouvelle table")
            new_table_columns = st.text_area("Colonnes (format: nom_colonne TYPE, ...)")

            if st.button("Créer Table"):
                # Appeler la fonction de création de table
                create_table(conn, selected_database, selected_schema, new_table_name, new_table_columns)

        elif operation == "Lire":
            st.header("Lecture de la table sélectionnée")
            # Affichez ici les données de la table sélectionnée
            if tables:
                selected_table = st.selectbox("Sélectionnez une table", tables)
                if selected_table:
                    table_data = read_table(conn, selected_database, selected_schema, selected_table)
                    st.write(table_data)
            else:
                st.warning("Aucune table disponible pour la lecture.")

        elif operation == "Mettre à jour":
            st.header("Mise à jour de la table sélectionnée")
            # Ajoutez ici les champs pour saisir les détails de la mise à jour
            if tables:
                selected_table = st.selectbox("Sélectionnez une table", tables)
                if selected_table:
                    column_name = st.text_input("Nom de la colonne à mettre à jour")
                    new_value = st.text_input("Nouvelle valeur")
                    condition = st.text_input("Condition de mise à jour")
                    if st.button("Mettre à jour"):
                        update_table(conn, selected_database, selected_schema, selected_table, column_name, new_value, condition)
            else:
                st.warning("Aucune table disponible pour la mise à jour.")

        elif operation == "Supprimer":
            st.header("Suppression de la table sélectionnée")
            # Confirmez la suppression de la table sélectionnée
            if tables:
                selected_table = st.selectbox("Sélectionnez une table", tables)
                if selected_table:
                    if st.button("Confirmer la suppression"):
                        delete_table(conn, selected_database, selected_schema, selected_table)
            else:
                st.warning("Aucune table disponible pour la suppression.")
        st.header("Visualisation des Données")
        warehouse_count, database_count, schema_count = count_elements(conn, selected_database)

        fig = go.Figure([go.Bar(x=['Datawarehouses', 'Bases de Données', 'Schémas'], y=[warehouse_count, database_count, schema_count])])
        fig.update_layout(title_text='Nombre de Datawarehouses, Bases de Données et Schémas')
        st.plotly_chart(fig)
       

if __name__ == "__main__":
    main()
