import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd
import datetime
import json
import time

# Database connection strings for each node
DB_SERVER0 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20272/mco2"
DB_SERVER1 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20282/mco2"
DB_SERVER2 = "mysql+pymysql://root:12345@ccscloud.dlsu.edu.ph:20292/mco2"

LOG_FILE = "transaction_log.txt"
RETRY_DELAY = 2  # Delay in seconds before retrying

# Establish a connection to a database
def get_db_connection(db_url):
    engine = create_engine(db_url)
    return engine.connect()

# Function to log transactions
def log_transaction(action, db_url, query, params):
    try:
        log_entry = {
            "action": action,
            "db_url": db_url,
            "query": str(query),
            "params": params,
            "timestamp": datetime.datetime.now().isoformat()
        }
        with open(LOG_FILE, "a") as log_file:
            log_file.write(json.dumps(log_entry) + "\n")
    except Exception as e:
        st.error(f"Error logging transaction: {e}")

# Function to check if all servers are online
def are_all_servers_online():
    db_urls = [DB_SERVER0, DB_SERVER1, DB_SERVER2]
    for db_url in db_urls:
        try:
            with get_db_connection(db_url):
                pass
        except Exception as e:
            st.warning(f"Could not connect to {db_url}: {e}")
            return False
    return True

# Function to recover transactions
def recover_transactions():
    if not are_all_servers_online():
        st.warning("Not all servers are online. Skipping recovery.")
        return

    try:
        with open(LOG_FILE, "r") as log_file:
            lines = log_file.readlines()

        if not lines:
            return

        updated_logs = []
        for line in lines:
            try:
                entry = json.loads(line.strip())
                action = entry["action"]
                db_url = entry["db_url"]
                params = entry["params"]

                attempt = 0
                while attempt < 3:
                    try:
                        if action == "INSERT":
                            insert_data(params, db_url)
                        elif action == "UPDATE":
                            update_data(params['info_id'], params, db_url)
                        elif action == "DELETE":
                            delete_data(params['info_id'], db_url)
                        break
                    except Exception as e:
                        attempt += 1
                        if attempt < 3:
                            time.sleep(RETRY_DELAY)
                        else:
                            updated_logs.append(line)
                            break
            except json.JSONDecodeError:
                continue

        with open(LOG_FILE, "w") as log_file:
            log_file.writelines(updated_logs)
    except Exception as e:
        st.error(f"Error during recovery: {e}")

# Fetch data from the database
def fetch_data(offset=0, limit=100):
    query = f"SELECT * FROM app_info LIMIT {limit} OFFSET {offset}"
    db_urls = [DB_SERVER0, DB_SERVER1, DB_SERVER2]
    
    for db_url in db_urls:
        try:
            with get_db_connection(db_url) as connection:
                df = pd.read_sql(query, connection)
                return df
        except Exception as e:
            st.warning(f"Could not connect to {db_url}: {e}")
            continue
    return pd.DataFrame()

# Fetch a single record by info_id
def fetch_record_by_info_id(info_id):
    query = text("SELECT * FROM app_info WHERE info_id = :info_id FOR SHARE")
    #lock_query = text("LOCK TABLE app_info IN SHARE MODE")
    db_urls = [DB_SERVER0, DB_SERVER1, DB_SERVER2]
    
    for db_url in db_urls:
        try:
            with get_db_connection(db_url) as connection:
                with connection.begin(): #begin transaction
                    connection.execute(query, {'info_id': info_id}) #for locking
                    st.write("(frbii) Delaying for 10 seconds...")
                    time.sleep(10) #wait to mimic concurrency
                    result = connection.execute(query, {'info_id': info_id}) #check if value changed
                    record = result.fetchone()
                    if record:
                        return dict(record._mapping)
        except Exception as e:
            st.warning(f"Could not connect to {db_url}: {e}")
            continue
    return None

# Insert data into the database
def insert_data(data, db_url):
    query = text("""
        INSERT INTO app_info (info_id, name, release_date, price, discount_dlc_count, about, achievements, notes, developers, publishers, categories, genres, tags)
        VALUES (:info_id, :name, :release_date, :price, :discount_dlc_count, :about, :achievements, :notes, :developers, :publishers, :categories, :genres, :tags)
    """)
    try:
        with get_db_connection(db_url) as connection:
            trans = connection.begin()
            try:
                connection.execute(query, data)
                trans.commit()
            except Exception as e:
                trans.rollback()
                log_transaction("INSERT", db_url, query, data)
                raise e
    except Exception as e:
        log_transaction("INSERT", db_url, query, data)
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
    try:
        with get_db_connection(db_url) as connection:
            trans = connection.begin()
            try:
                connection.execute(query, updated_data)
                st.write("(update) Delaying for 10 seconds...")
                time.sleep(10)
                trans.commit()
            except Exception as e: 
                trans.rollback()
                log_transaction("UPDATE", db_url, query, updated_data)
                raise e
    except Exception as e:
        log_transaction("UPDATE", db_url, query, updated_data)
        raise e

# Delete data from the database
def delete_data(info_id, db_url):
    query = text("DELETE FROM app_info WHERE info_id = :info_id")
    try:
        with get_db_connection(db_url) as connection:
            trans = connection.begin()
            try:
                connection.execute(query, {'info_id': info_id})
                trans.commit()
            except Exception as e:
                trans.rollback()
                log_transaction("DELETE", db_url, query, {'info_id': info_id})
                raise e
    except Exception as e:
        log_transaction("DELETE", db_url, query, {'info_id': info_id})
        raise e

# Check if info_id already exists in the database
def check_duplicate_info_id(info_id):
    query = text("SELECT COUNT(*) FROM app_info WHERE info_id = :info_id")
    db_urls = [DB_SERVER0, DB_SERVER1, DB_SERVER2]
    
    for db_url in db_urls:
        try:
            with get_db_connection(db_url) as connection:
                result = connection.execute(query, {'info_id': info_id}).scalar()
                if result > 0:
                    return True
        except Exception as e:
            st.warning(f"Could not connect to {db_url}: {e}")
            continue
    return False

# Streamlit application
st.sidebar.title("CRUD Operations")
page = st.sidebar.selectbox("Select a Page", ["View Data", "Add Record", "Update Record", "Delete Record", "Search Record"])

# Run recovery transactions on page load
recover_transactions()

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
            try:
                insert_data(new_record, DB_SERVER0)
            except Exception as e:
                st.error(f"Error inserting record into Server0: {e}")

            # Determine which node to insert based on release date
            release_year = release_date.year
            try:
                if release_year < 2010:
                    insert_data(new_record, DB_SERVER1)
                else:
                    insert_data(new_record, DB_SERVER2)
            except Exception as e:
                st.error(f"Error inserting record into secondary server: {e}")

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

        # Update the record in the central node (Server0)
        try:
            update_data(info_id, updated_data, DB_SERVER0)
        except Exception as e:
            st.error(f"Error updating record in Server0: {e}")

        # Update the record in the current secondary node
        release_year = release_date.year
        try:
            if release_year < 2010:
                update_data(info_id, updated_data, DB_SERVER1)
            else:
                update_data(info_id, updated_data, DB_SERVER2)
        except Exception as e:
            st.error(f"Error updating record in secondary server: {e}")

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
                try:
                    delete_data(info_id, DB_SERVER0)
                except Exception as e:
                    st.error(f"Error deleting record from Server0: {e}")

                release_year = datetime.datetime.strptime(record['release_date'], '%Y-%m-%d').year
                try:
                    if release_year < 2010:
                        delete_data(info_id, DB_SERVER1)
                    else:
                        delete_data(info_id, DB_SERVER2)
                except Exception as e:
                    st.error(f"Error deleting record from secondary server: {e}")

                st.success("Record deleted successfully!")
            else:
                st.error("No record found with this Info ID.")
        except Exception as e:
            st.error(f"Error fetching record: {e}")

elif page == "Search Record":
    st.title("Search for a Record")
    recover_transactions()
    # Form to search for a record
    with st.form("search_record_form"):
        search_id = st.number_input("Enter Info ID to search", min_value=1, step=1)
        search = st.form_submit_button("Search Record")

    if search:
        #st.write("Delaying for 10 seconds...")
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

if __name__ == "__main__":
    # Initial recovery on app start
    recover_transactions() 