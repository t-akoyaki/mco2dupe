import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd
import datetime

# Database connection strings for each node
DB_SERVER0 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20272/mco2"
DB_SERVER1 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20282/mco2"
DB_SERVER2 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20292/mco2"

# Establish a connection to a database
def get_db_connection(db_url):
    engine = create_engine(db_url)
    return engine.connect()

# Fetch data from the database
def fetch_data(offset=0, limit=100):
    query = f"SELECT * FROM app_info LIMIT {limit} OFFSET {offset}"
    with get_db_connection(DB_SERVER0) as connection:
        df = pd.read_sql(query, connection)
        
    return df

# Fetch a single record by info_id
def fetch_record_by_info_id(info_id):
    query = text("SELECT * FROM app_info WHERE info_id = :info_id")
    with get_db_connection(DB_SERVER0) as connection:
        result = connection.execute(query, {'info_id': info_id})
        record = result.fetchone()
    return dict(record._mapping) if record else None

# Insert data into the database
def insert_data(data, db_url):
    query = text("""
        INSERT INTO app_info (info_id, name, release_date, price, discount_dlc_count, about, achievements, notes, developers, publishers, categories, genres, tags)
        VALUES (:info_id, :name, :release_date, :price, :discount_dlc_count, :about, :achievements, :notes, :developers, :publishers, :categories, :genres, :tags)
    """)
    with get_db_connection(db_url) as connection:
        trans = connection.begin()
        try:
            connection.execute(query, data)
            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e

# Update data in the database
def update_data(info_id, updated_data, db_url):
    query = text("""
        UPDATE app_info
        SET name = :name,
            release_date = :release_date,
            price = :price,
            discount_dlc_count = :discount_dlc_count,
            about = :about,
            achievements = :achievements,
            notes = :notes,
            developers = :developers,
            publishers = :publishers,
            categories = :categories,
            genres = :genres,
            tags = :tags
        WHERE info_id = :info_id
    """)
    with get_db_connection(db_url) as connection:
        trans = connection.begin()
        try:
            connection.execute(query, updated_data)
            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e

# Delete data from the database
def delete_data(info_id, db_url):
    query = text("DELETE FROM app_info WHERE info_id = :info_id")
    with get_db_connection(db_url) as connection:
        trans = connection.begin()
        try:
            connection.execute(query, {'info_id': info_id})
            trans.commit()
        except Exception as e:
            trans.rollback()
            raise e

# Check if info_id already exists in the database
def check_duplicate_info_id(info_id):
    query = text("SELECT COUNT(*) FROM app_info WHERE info_id = :info_id")
    with get_db_connection(DB_SERVER0) as connection:
        result = connection.execute(query, {'info_id': info_id}).scalar()
    return result > 0

# Streamlit application
st.sidebar.title("CRUD Operations")
page = st.sidebar.selectbox("Select a Page", ["View Data", "Add Record", "Update Record", "Delete Record", "Search Record"])

if page == "View Data":
    st.title("Steam Games Dataset Viewer - Server0")

    # Pagination logic
    if 'offset' not in st.session_state:
        st.session_state['offset'] = 0

    limit = 100
    data = fetch_data(offset=st.session_state['offset'], limit=limit)

    # Display the data as a table
    st.write(data)

    # Pagination buttons
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("Previous Page"):
            if st.session_state['offset'] >= limit:
                st.session_state['offset'] -= limit
    with col3:
        if st.button("Next Page"):
            st.session_state['offset'] += limit

elif page == "Add Record":
    st.title("Add a New Record")

    # Form to add a new record
    with st.form("add_record_form"):
        info_id = st.number_input("Info ID", min_value=1, step=1)
        name = st.text_input("Name")
        release_date = st.date_input("Release Date", value=datetime.date.today())
        price = st.number_input("Price", min_value=0.0, step=0.01)
        discount_dlc_count = st.number_input("Discount DLC Count", min_value=0, step=1)
        about = st.text_area("About")
        achievements = st.number_input("Achievements", min_value=0, step=1)
        notes = st.text_area("Notes")
        developers = st.text_input("Developers")
        publishers = st.text_input("Publishers")
        categories = st.text_input("Categories")
        genres = st.text_input("Genres")
        tags = st.text_input("Tags")
        submit = st.form_submit_button("Add Record")

    if submit:
        # Check for duplicate info_id
        if check_duplicate_info_id(info_id):
            st.error("A record with this Info ID already exists. Please use a unique Info ID.")
        else:
            # Prepare data for insertion
            new_record = {
                'info_id': info_id,
                'name': name,
                'release_date': release_date.strftime('%Y-%m-%d'),
                'price': price,
                'discount_dlc_count': discount_dlc_count,
                'about': about,
                'achievements': achievements,
                'notes': notes,
                'developers': developers,
                'publishers': publishers,
                'categories': categories,
                'genres': genres,
                'tags': tags
            }

            # Insert data into the central node (Server0)
            insert_data(new_record, DB_SERVER0)

            # Determine which node to insert based on release date
            release_year = release_date.year
            if release_year < 2010:
                insert_data(new_record, DB_SERVER1)
            else:
                insert_data(new_record, DB_SERVER2)

            st.success("Record added successfully!")

elif page == "Update Record":
    st.title("Update an Existing Record")

    # Form to update a record
    with st.form("update_record_form"):
        info_id = st.number_input("Info ID", min_value=1, step=1)
        name = st.text_input("Name")
        release_date = st.date_input("Release Date", value=datetime.date.today())
        price = st.number_input("Price", min_value=0.0, step=0.01)
        discount_dlc_count = st.number_input("Discount DLC Count", min_value=0, step=1)
        about = st.text_area("About")
        achievements = st.number_input("Achievements", min_value=0, step=1)
        notes = st.text_area("Notes")
        developers = st.text_input("Developers")
        publishers = st.text_input("Publishers")
        categories = st.text_input("Categories")
        genres = st.text_input("Genres")
        tags = st.text_input("Tags")
        submit = st.form_submit_button("Update Record")

    if submit:
        # Prepare updated data
        updated_data = {
            'info_id': info_id,
            'name': name,
            'release_date': release_date.strftime('%Y-%m-%d'),
            'price': price,
            'discount_dlc_count': discount_dlc_count,
            'about': about,
            'achievements': achievements,
            'notes': notes,
            'developers': developers,
            'publishers': publishers,
            'categories': categories,
            'genres': genres,
            'tags': tags
        }

        # Update the record in the central and current secondary node
        update_data(info_id, updated_data, DB_SERVER0)
        release_year = release_date.year
        if release_year < 2010:
            update_data(info_id, updated_data, DB_SERVER1)
        else:
            update_data(info_id, updated_data, DB_SERVER2)

        st.success("Record updated successfully!")

elif page == "Delete Record":
    st.title("Delete a Record")

    # Form to delete a record
    with st.form("delete_record_form"):
        info_id = st.number_input("Enter Info ID to delete", min_value=1, step=1)
        delete = st.form_submit_button("Delete Record")

    if delete:
        try:
            record = fetch_record_by_info_id(info_id)
            if record:
                delete_data(info_id, DB_SERVER0)
                release_year = datetime.datetime.strptime(record['release_date'], '%Y-%m-%d').year
                if release_year < 2010:
                    delete_data(info_id, DB_SERVER1)
                else:
                    delete_data(info_id, DB_SERVER2)
                st.success("Record deleted successfully!")
            else:
                st.error("No record found with this Info ID.")
        except Exception as e:
            st.error(f"Error deleting record: {e}")

elif page == "Search Record":
    st.title("Search for a Record")

    # Form to search for a record
    with st.form("search_record_form"):
        search_id = st.number_input("Enter Info ID to search", min_value=1, step=1)
        search = st.form_submit_button("Search Record")

    if search:
        record = fetch_record_by_info_id(search_id)
        if record:
            # Display the record information
            st.write(f"**Name:** {record['name']}")
            st.write(f"**Release Date:** {record['release_date']}")
            st.write(f"**Price:** {record['price']}")
            st.write(f"**Discount DLC Count:** {record['discount_dlc_count']}")
            st.write(f"**About:** {record['about']}")
            st.write(f"**Achievements:** {record['achievements']}")
            st.write(f"**Notes:** {record['notes']}")
            st.write(f"**Developers:** {record['developers']}")
            st.write(f"**Publishers:** {record['publishers']}")
            st.write(f"**Categories:** {record['categories']}")
            st.write(f"**Genres:** {record['genres']}")
            st.write(f"**Tags:** {record['tags']}")
        else:
            st.error("No record found with this Info ID.")