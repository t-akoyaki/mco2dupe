import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd
import datetime

# Database connection strings for each node with autocommit configuration
DB_SERVER0 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20272/mco2?autocommit=true"
DB_SERVER1 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20282/mco2?autocommit=true"
DB_SERVER2 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20292/mco2?autocommit=true"

# Establish a connection to a database
def get_db_connection(db_url):
    engine = create_engine(db_url, isolation_level="AUTOCOMMIT")
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
    with get_db_connection(db_url) as connection:
        data.to_sql('app_info', con=connection, if_exists='append', index=False)

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
        connection.execute(query, updated_data)

# Delete data from the database
def delete_data(info_id, db_url):
    query = text("DELETE FROM app_info WHERE info_id = :info_id")
    with get_db_connection(db_url) as connection:
        connection.execute(query, {'info_id': info_id})

# Check if info_id already exists in the database
def check_duplicate_info_id(info_id):
    query = text("SELECT COUNT(*) FROM app_info WHERE info_id = :info_id")
    with get_db_connection(DB_SERVER0) as connection:
        result = connection.execute(query, {'info_id': info_id}).scalar()
    return result > 0

# Streamlit application
st.sidebar.title("CRUD Operations")
page = st.sidebar.selectbox("Select a Page", ["View Data", "Add Record", "Update Record", "Delete Record"])

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
            # Create a DataFrame from the form input
            new_record = pd.DataFrame({
                'info_id': [info_id],
                'name': [name],
                'release_date': [release_date.strftime('%Y-%m-%d')],
                'price': [price],
                'discount_dlc_count': [discount_dlc_count],
                'about': [about],
                'achievements': [achievements],
                'notes': [notes],
                'developers': [developers],
                'publishers': [publishers],
                'categories': [categories],
                'genres': [genres],
                'tags': [tags]
            })

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

    # Search for the record by info_id
    search_id = st.number_input("Enter Info ID to search", min_value=1, step=1)
    search_button = st.button("Search")

    if search_button:
        record = fetch_record_by_info_id(search_id)
        if record:
            # Display the form with the existing data
            with st.form("update_record_form"):
                name = st.text_input("Name", value=record['name'])
                try:
                    release_date = st.date_input("Release Date", value=datetime.datetime.strptime(record['release_date'], '%Y-%m-%d').date())
                except ValueError:
                    release_date = st.date_input("Release Date", value=datetime.datetime.strptime(record['release_date'], '%b %d, %Y').date())
                price = st.number_input("Price", min_value=0.0, step=0.01, value=record['price'])
                discount_dlc_count = st.number_input("Discount DLC Count", min_value=0, step=1, value=record['discount_dlc_count'])
                about = st.text_area("About", value=record['about'])
                achievements = st.number_input("Achievements", min_value=0, step=1, value=record['achievements'])
                notes = st.text_area("Notes", value=record['notes'])
                developers = st.text_input("Developers", value=record['developers'])
                publishers = st.text_input("Publishers", value=record['publishers'])
                categories = st.text_input("Categories", value=record['categories'])
                genres = st.text_input("Genres", value=record['genres'])
                tags = st.text_input("Tags", value=record['tags'])
                submit = st.form_submit_button("Update Record")

            if submit:
                # Prepare updated data
                updated_data = {
                    'info_id': search_id,
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

                # Determine old release year and delete from the old secondary node if necessary
                old_release_year = datetime.datetime.strptime(record['release_date'], '%Y-%m-%d').year
                new_release_year = release_date.year
                if old_release_year < 2010 and new_release_year >= 2010:
                    delete_data(search_id, DB_SERVER1)
                    update_data(search_id, updated_data, DB_SERVER0)
                    insert_data(pd.DataFrame([updated_data]), DB_SERVER2)
                elif old_release_year >= 2010 and new_release_year < 2010:
                    delete_data(search_id, DB_SERVER2)
                    update_data(search_id, updated_data, DB_SERVER0)
                    insert_data(pd.DataFrame([updated_data]), DB_SERVER1)
                else:
                    # Update the record in the central and current secondary node
                    update_data(search_id, updated_data, DB_SERVER0)
                    if new_release_year < 2010:
                        update_data(search_id, updated_data, DB_SERVER1)
                    else:
                        update_data(search_id, updated_data, DB_SERVER2)

                st.success("Record updated successfully!")
        else:
            st.error("No record found with this Info ID.")

elif page == "Delete Record":
    st.title("Delete a Record")

    # Search for the record by info_id
    search_id = st.number_input("Enter Info ID to search", min_value=1, step=1)
    search_button = st.button("Search")

    if search_button:
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

            # Delete button
            delete_button = st.button("Delete Record")

            if delete_button:
                # Delete the record from all nodes
                try:
                    delete_data(search_id, DB_SERVER0)
                    release_year = datetime.datetime.strptime(record['release_date'], '%Y-%m-%d').year
                    if release_year < 2010:
                        delete_data(search_id, DB_SERVER1)
                    else:
                        delete_data(search_id, DB_SERVER2)
                    st.success("Record deleted successfully!")
                except Exception as e:
                    st.error(f"Error deleting record: {e}")
        else:
            st.error("No record found with this Info ID.")
