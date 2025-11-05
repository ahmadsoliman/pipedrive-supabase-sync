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



# NEW Trigger functions to keep Supabase tables in sync with Pipedrive data
def add_all_new_trigger_functions(connection, cursor):
    print("Adding all new triggers to keep Supabase tables in sync with Pipedrive data.")

    cursor.execute("""
        -- 1) Harden SECURITY DEFINER functions with explicit search_path and qualify names
        -- trigger_sync_deal (public)
        create or replace function public.trigger_sync_deal()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        -- Only run when meaningful columns change on UPDATE
        if tg_op = 'INSERT' or tg_op = 'UPDATE' then
            perform public.sync_deal_from_pipedrive(new.id);
            -- Also refresh asset types here (merging trg_sync_deal_asset_type_parent)
            perform public.sync_deal_asset_type(new.id);
        end if;
        return new;
        end;
        $$;

        -- trigger_sync_deal_asset_type (public)
        create or replace function public.trigger_sync_deal_asset_type()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        declare
        v_parent_id text;
        v_deal_id bigint;
        begin
        if tg_op = 'DELETE' then
            v_parent_id := old._dlt_parent_id;
        else
            v_parent_id := new._dlt_parent_id;
        end if;

        select d.id into v_deal_id
        from pipedrive_data.deals d
        where d._dlt_id = v_parent_id;

        if v_deal_id is null then
            return coalesce(new, old);
        end if;

        perform public.map_all_deal_asset_types_for_deal(v_deal_id);
        perform public.sync_deal_asset_type(v_deal_id);

        return coalesce(new, old);
        end;
        $$;

        -- trigger_sync_deal_financing_type (public)
        create or replace function public.trigger_sync_deal_financing_type()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        declare
        v_deal_id bigint;
        begin
        if tg_op = 'DELETE' then
            select id into v_deal_id from pipedrive_data.deals where _dlt_id = old._dlt_parent_id;
        else
            select id into v_deal_id from pipedrive_data.deals where _dlt_id = new._dlt_parent_id;
        end if;

        if v_deal_id is not null then
            perform public.sync_deal_financing_type(v_deal_id);
        end if;
        return coalesce(new, old);
        end;
        $$;

        -- sync_organization_insert (public)
        create or replace function public.sync_organization_insert()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        insert into public.organizations (pipedrive_id, name, hq_location)
        values (new.id, new.name, new.address)
        on conflict (pipedrive_id) do update
            set name = excluded.name,
                hq_location = excluded.hq_location;
        return new;
        end;
        $$;

        -- sync_organization_update (public) with change guard
        create or replace function public.sync_organization_update()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        if (new.name is distinct from old.name)
            or (new.address is distinct from old.address) then
            update public.organizations
            set name = new.name,
                hq_location = new.address
            where pipedrive_id = new.id;
        end if;
        return new;
        end;
        $$;

        -- sync_contact_insert (public)
        create or replace function public.sync_contact_insert()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        insert into public.contacts (pipedrive_id, name, title, location, linkedin, email, organization_id)
        values (
            new.id,
            new.name,
            new.job_title,
            new.person_address,
            new.linked_in,
            new.primary_email,
            (select id from public.organizations where pipedrive_id = new.org_id__value)
        )
        on conflict (pipedrive_id) do update
            set name = excluded.name,
                title = excluded.title,
                location = excluded.location,
                linkedin = excluded.linkedin,
                email = excluded.email,
                organization_id = excluded.organization_id;
        return new;
        end;
        $$;

        -- sync_contact_update (public) with change guards
        create or replace function public.sync_contact_update()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        if (new.name is distinct from old.name)
            or (new.job_title is distinct from old.job_title)
            or (new.person_address is distinct from old.person_address)
            or (new.linked_in is distinct from old.linked_in)
            or (new.primary_email is distinct from old.primary_email)
            or (new.org_id__value is distinct from old.org_id__value) then
            update public.contacts
            set name = new.name,
                title = new.job_title,
                location = new.person_address,
                linkedin = new.linked_in,
                email = new.primary_email,
                organization_id = (
                select id from public.organizations where pipedrive_id = new.org_id__value
                )
            where pipedrive_id = new.id;
        end if;
        return new;
        end;
        $$;

        -- sync_contact_email consolidated (public)
        create or replace function public.sync_contact_email()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        if (tg_op = 'INSERT' and new.primary is true)
            or (tg_op = 'UPDATE' and (new.primary is true) and (new.value is distinct from old.value or new.primary is distinct from old.primary)) then
            update public.contacts c
            set email = new.value
            where c.pipedrive_id = (
            select id from pipedrive_data.persons where _dlt_id = new._dlt_parent_id
            );
        end if;
        return new;
        end;
        $$;

        -- sync_contact_phone consolidated (public)
        create or replace function public.sync_contact_phone()
        returns trigger
        language plpgsql
        security definer
        set search_path to public, pipedrive_data, extensions
        as $$
        begin
        if (tg_op = 'INSERT' and new.primary is true)
            or (tg_op = 'UPDATE' and (new.primary is true) and (new.value is distinct from old.value or new.primary is distinct from old.primary)) then
            update public.contacts c
            set phone = new.value
            where c.pipedrive_id = (
            select id from pipedrive_data.persons where _dlt_id = new._dlt_parent_id
            );
        end if;
        return new;
        end;
        $$;
    """)

    connection.commit()
    print("All new trigger functions added successfully.")