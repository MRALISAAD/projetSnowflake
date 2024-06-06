import streamlit as st
import snowflake.connector
import plotly.graph_objects as go 

st.markdown(
    """
    <style>
    .sidebar .sidebar-content {
        background-color: #f0f2f6;
    }
    .sidebar .sidebar-content .block-container {
        padding: 1rem;
    }
    .main .block-container {
        padding: 2rem;
    }
    .stButton>button {
        background-color: #4CAF50;
        color: white;
        padding: 0.5rem 1rem;
        border: none;
        border-radius: 5px;
        cursor: pointer;
        transition: background-color 0.3s;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    </style>
    """,
    unsafe_allow_html=True
)

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

def create_datawarehouse(conn, warehouse_name):
    try:
        query = f"CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit() 
        st.success(f"Datawarehouse '{warehouse_name}' créé avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création du datawarehouse '{warehouse_name}': {e}")

def list_databases(conn):
    try:
        query = "SHOW DATABASES"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        database_names = [row[1] for row in results]  
        return database_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des bases de données : {e}")
        return []

def create_database(conn, database_name):
    try:
        query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  
        st.success(f"Base de données '{database_name}' créée avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création de la base de données '{database_name}': {e}")

def list_schemas(conn, database_name):
    try:
        query = f"SHOW SCHEMAS IN DATABASE {database_name}"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        schema_names = [row[1] for row in results]  
        return schema_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des schémas : {e}")
        return []

def create_schema(conn, database_name, schema_name):
    try:
        query = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création du schéma '{schema_name}': {e}")

def list_tables(conn, database_name, schema_name):
    try:
        query = f"SHOW TABLES IN SCHEMA {database_name}.{schema_name}"
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        table_names = [row[1] for row in results]  
        return table_names
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la liste des tables : {e}")
        return []


def create_table(conn, database_name, schema_name, table_name, columns):
    try:
        query = f"CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} ({columns})"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  #
        st.success(f"Table '{table_name}' créée avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la création de la table '{table_name}': {e}")

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

def update_table(conn, database_name, schema_name, table_name, column_name, new_value, condition):
    try:
        query = f"UPDATE {database_name}.{schema_name}.{table_name} SET {column_name} = '{new_value}' WHERE {condition}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  
        st.success("Table mise à jour avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la mise à jour de la table '{table_name}': {e}")

def delete_table(conn, database_name, schema_name, table_name):
    try:
        query = f"DROP TABLE {database_name}.{schema_name}.{table_name}"
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()  
        st.success(f"Table '{table_name}' supprimée avec succès.")
    except snowflake.connector.errors.DatabaseError as e:
        st.error(f"Erreur lors de la suppression de la table '{table_name}': {e}")
def count_elements(conn, selected_database):
    warehouse_count = len(list_datawarehouses(conn))
    database_count = len(list_databases(conn))
    schema_count = len(list_schemas(conn, selected_database))
    return warehouse_count, database_count, schema_count


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

def main():
    st.title("Gestion des Datawarehouses Snowflake")

    user = st.sidebar.text_input("Utilisateur")
    password = st.sidebar.text_input("Mot de passe", type="password")
    account = st.sidebar.text_input("Compte")

    if st.sidebar.button("Se connecter"):
        conn = connect_to_snowflake(user, password, account)
        st.session_state['conn'] = conn
        st.success("Connexion réussie")

    if 'conn' in st.session_state:
        conn = st.session_state['conn']

        st.header("Liste des Datawarehouses")
        warehouses = list_datawarehouses(conn)
        st.write(warehouses)

        st.header("Créer un Nouveau Datawarehouse")
        new_warehouse_name = st.text_input("Nom du nouveau datawarehouse")
        if st.button("Créer Datawarehouse"):
            create_datawarehouse(conn, new_warehouse_name)

        st.header("Liste des Bases de Données")
        databases = list_databases(conn)
        st.write(databases)

        st.header("Créer une Nouvelle Base de Données")
        new_database_name = st.text_input("Nom de la nouvelle base de données")
        if st.button("Créer Base de Données"):
            create_database(conn, new_database_name)

        st.header("Liste des Schémas dans une Base de Données")
        selected_database = st.selectbox("Sélectionnez une base de données", databases)
        schemas = list_schemas(conn, selected_database)
        st.write(schemas)

        st.header("Créer un Nouveau Schéma dans une Base de Données")
        new_schema_name = st.text_input("Nom du nouveau schéma")
        if st.button("Créer Schéma"):
            create_schema(conn, selected_database, new_schema_name)

        st.header("Liste des Tables dans un Schéma")
        selected_schema = st.selectbox("Sélectionnez un schéma", schemas)
        tables = list_tables(conn, selected_database, selected_schema)
        st.write(tables)
        operation = st.selectbox("Sélectionnez une opération", ["Créer", "Lire", "Mettre à jour", "Supprimer"])

        if operation == "Créer":
            st.header("Création d'une Nouvelle Table dans un Schéma")
            new_table_name = st.text_input("Nom de la nouvelle table")
            new_table_columns = st.text_area("Colonnes (format: nom_colonne TYPE, ...)")

            if st.button("Créer Table"):
                create_table(conn, selected_database, selected_schema, new_table_name, new_table_columns)

        elif operation == "Lire":
            st.header("Lecture de la table sélectionnée")
            if tables:
                selected_table = st.selectbox("Sélectionnez une table", tables)
                if selected_table:
                    table_data = read_table(conn, selected_database, selected_schema, selected_table)
                    st.write(table_data)
            else:
                st.warning("Aucune table disponible pour la lecture.")

        elif operation == "Mettre à jour":
            st.header("Mise à jour de la table sélectionnée")
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
