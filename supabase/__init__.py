import psycopg2
import os
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()
# Get connection parameters from environment variables
USER = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__USERNAME")
PASSWORD = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PASSWORD")
HOST = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__HOST")
PORT = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__PORT")
DBNAME = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__DATABASE")
CONNECT_TIMEOUT = os.getenv("DESTINATION__POSTGRES__CREDENTIALS__CONNECT_TIMEOUT")

def add_triggers() -> None:
    """Function to add triggers to Supabase database"""

    # Connect to the database
    try:
        connection = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        print("Supabase Connection successful!")
        
        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        add_organizations_triggers(connection, cursor)
        add_persons_triggers(connection, cursor)      
        add_deals_triggers(connection, cursor)
  
        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("Connection closed.")

    except Exception as e:
        print(f"Failed to connect to Supabase: {e}")

def add_organizations_triggers(connection, cursor):
    print("Adding triggers for any changes on pipedrive's tables organizations.")
    cursor.execute("""
         -- For inserts
        drop trigger if exists organizations_insert_trigger on pipedrive_data.organizations;
        CREATE TRIGGER organizations_insert_trigger
        AFTER INSERT ON pipedrive_data.organizations
        FOR EACH ROW EXECUTE FUNCTION sync_organization_insert();

        -- For updates
        drop trigger if exists organizations_update_trigger on pipedrive_data.organizations;
        CREATE TRIGGER organizations_update_trigger
        AFTER UPDATE ON pipedrive_data.organizations
        FOR EACH ROW EXECUTE FUNCTION sync_organization_update();
    """)

    connection.commit()
    print("Organizations triggers added successfully.")

def add_persons_triggers(connection, cursor):
    print("Adding triggers for any changes on pipedrive's tables persons.")
    cursor.execute("""
        -- Insert
        drop trigger if exists contacts_persons_insert_trigger on pipedrive_data.persons;
        CREATE TRIGGER contacts_persons_insert_trigger
        AFTER INSERT ON pipedrive_data.persons
        FOR EACH ROW EXECUTE FUNCTION sync_contact_insert();

        -- Update
        drop trigger if exists contacts_persons_update_trigger on pipedrive_data.persons;
        CREATE TRIGGER contacts_persons_update_trigger
        AFTER UPDATE ON pipedrive_data.persons
        FOR EACH ROW EXECUTE FUNCTION sync_contact_update();
                   
        -- Insert email
        drop trigger if exists contacts_email_insert_trigger on pipedrive_data.persons__email;
        CREATE TRIGGER contacts_email_insert_trigger
        AFTER INSERT ON pipedrive_data.persons__email
        FOR EACH ROW EXECUTE FUNCTION sync_contact_email();

        -- Update email
        drop trigger if exists contacts_email_update_trigger on pipedrive_data.persons__email;
        CREATE TRIGGER contacts_email_update_trigger
        AFTER UPDATE ON pipedrive_data.persons__email
        FOR EACH ROW EXECUTE FUNCTION sync_contact_email();

        -- Insert phone
        drop trigger if exists contacts_phone_insert_trigger on pipedrive_data.persons__phone;
        CREATE TRIGGER contacts_phone_insert_trigger
        AFTER INSERT ON pipedrive_data.persons__phone
        FOR EACH ROW EXECUTE FUNCTION sync_contact_phone();

        -- Update phone
        drop trigger if exists contacts_phone_update_trigger on pipedrive_data.persons__phone;
        CREATE TRIGGER contacts_phone_update_trigger
        AFTER UPDATE ON pipedrive_data.persons__phone
        FOR EACH ROW EXECUTE FUNCTION sync_contact_phone();
    """)

    connection.commit()
    print("Persons triggers added successfully.")

def add_deals_triggers(connection, cursor):
    print("Adding triggers for any changes on pipedrive's tables deals, deal asset_type, and deal financing_type.")
    cursor.execute("""
        drop trigger if exists trg_sync_deal on pipedrive_data.deals;
        create trigger trg_sync_deal
        after insert or update on pipedrive_data.deals
        for each row
        execute function trigger_sync_deal();
    """)
    connection.commit()
    print("Deals triggers added successfully.")

    cursor.execute("""
        drop trigger if exists trg_sync_deal_asset_type on pipedrive_data.deals__asset_type;
        create trigger trg_sync_deal_asset_type
        after insert or update or delete on pipedrive_data.deals__asset_type
        for each row execute function trigger_sync_deal_asset_type();      
                      
        drop trigger if exists trg_sync_deal_financing_type on pipedrive_data.deals__financing_type;
        create trigger trg_sync_deal_financing_type
        after insert or update or delete on pipedrive_data.deals__financing_type
        for each row execute function trigger_sync_deal_financing_type();
    """)

    connection.commit()

    # touch all rows in deals__asset_type and deals__financing_type to fire the triggers and sync data
    cursor.execute("""
        update pipedrive_data.deals__asset_type set _dlt_id = _dlt_id;
        update pipedrive_data.deals__financing_type set _dlt_id = _dlt_id;
    """)

    connection.commit()

    print("Deals asset_type & financing_type triggers added successfully.")