-- create or replace function sync_deal_from_pipedrive(p_deal_id bigint)
-- returns void language plpgsql as $$
-- begin

insert into public.deals (
    id,
    title,
    value,
    currency,
    stage,
    status,
    probability,
    organization_id,
    primary_contact_id,
    owner_user_id,
    financing_type,
    deal_assist_user,
    capital_advisor_fee,
    referral_fee,
    referral_partner_id,
    winning_capital_provider_id,
    occupancy,
    ground_lease,
    property_address,
    asset_type,
    investment_strategy,
    tenancy,
    hotel_flag_id,
    hotel_type,
    single_tenant_name_id,
    guarantor_type,
    sponsor_location,
    experience_level,
    net_worth,
    liquidity,
    assets_under_management,
    credit_score,
    us_citizenship,
    deal_file_folder_link,
    offering_memorandum_link,
    add_time,
    won_time,
    lost_time,
    close_time,
    expected_close_date,
    last_synced_at,
    created_at,
    updated_at
)
select
    d.id,
    d.title,
    d.value,
    d.currency,
    d.stage_id,
    d.status,
    null as probability,
    org.id as organization_id,
    c.id   as primary_contact_id,
    d.user_id__id as owner_user_id,
    '{}'::text[] as financing_type,
    d.deal_assist__id,
    d.capital_advisor_fee,
    d.referral_fee,
    ref.id as referral_partner_id,
    win_org.id as winning_capital_provider_id,
    d.occupancy,
    d.ground_lease,
    d.full_combined_address_of_property_address as property_address,
    '{}'::text[] as asset_type,
    d.investment_strategy,
    d.tenancy,
    hotel_org.id as hotel_flag_id,
    d.hotel_type,
    tenant_org.id as single_tenant_name_id,
    d.guarantor_type,
    d.full_combined_address_of_sponsor_location as sponsor_location,
    d.experience_level,
    d.net_worth,
    d.liquidity,
    null as assets_under_management,
    d.credit_score,
    d.us_citizenship,
    d.deal_file_folder_link,
    d.offering_memorandum_link,
    d.add_time,
    d.won_time,
    d.lost_time,
    d.close_time,
    d.expected_close_date::date,
    now() as last_synced_at,
    now() as created_at,
    now() as updated_at
from pipedrive_data.deals d
left join public.organizations org
  on org.pipedrive_id = d.org_id__value
   and org.pipedrive_id is not null
left join public.contacts c
  on c.pipedrive_id = d.person_id__value
   and c.pipedrive_id is not null
left join public.contacts ref
  on ref.pipedrive_id = d.referral_partner__value
   and ref.pipedrive_id is not null
left join public.organizations win_org
  on win_org.pipedrive_id = d.winning_capital_provider__value
   and win_org.pipedrive_id is not null
left join public.organizations hotel_org
  on hotel_org.pipedrive_id = d.hotel_flag__value
   and hotel_org.pipedrive_id is not null
left join public.organizations tenant_org
  on tenant_org.pipedrive_id = d.single_tenant_name__value
   and tenant_org.pipedrive_id is not null

where d.id = p_deal_id

on conflict (id) do update set
    title = excluded.title,
    value = excluded.value,
    currency = excluded.currency,
    stage = excluded.stage,
    status = excluded.status,
    probability = excluded.probability,
    organization_id = excluded.organization_id,
    primary_contact_id = excluded.primary_contact_id,
    owner_user_id = excluded.owner_user_id,
    financing_type = excluded.financing_type,
    deal_assist_user = excluded.deal_assist_user,
    capital_advisor_fee = excluded.capital_advisor_fee,
    referral_fee = excluded.referral_fee,
    referral_partner_id = excluded.referral_partner_id,
    winning_capital_provider_id = excluded.winning_capital_provider_id,
    occupancy = excluded.occupancy,
    ground_lease = excluded.ground_lease,
    property_address = excluded.property_address,
    asset_type = excluded.asset_type,
    investment_strategy = excluded.investment_strategy,
    tenancy = excluded.tenancy,
    hotel_flag_id = excluded.hotel_flag_id,
    hotel_type = excluded.hotel_type,
    single_tenant_name_id = excluded.single_tenant_name_id,
    guarantor_type = excluded.guarantor_type,
    sponsor_location = excluded.sponsor_location,
    experience_level = excluded.experience_level,
    net_worth = excluded.net_worth,
    liquidity = excluded.liquidity,
    assets_under_management = excluded.assets_under_management,
    credit_score = excluded.credit_score,
    us_citizenship = excluded.us_citizenship,
    deal_file_folder_link = excluded.deal_file_folder_link,
    offering_memorandum_link = excluded.offering_memorandum_link,
    add_time = excluded.add_time,
    won_time = excluded.won_time,
    lost_time = excluded.lost_time,
    close_time = excluded.close_time,
    expected_close_date = excluded.expected_close_date,
    last_synced_at = excluded.last_synced_at,
    updated_at = excluded.updated_at;
end;
$$;

-- create or replace function trigger_sync_deal()
-- returns trigger language plpgsql as $$
-- begin
--     perform sync_deal_from_pipedrive(new.id);
--     return new;
-- end;
-- $$;

drop trigger if exists trg_sync_deal on pipedrive_data.deals;

create trigger trg_sync_deal
after insert or update on pipedrive_data.deals
for each row
execute function trigger_sync_deal();


-- create or replace function sync_deal_asset_type(p_deal_id bigint)
-- returns void language plpgsql as $$
-- begin
--     update public.deals d
--     set asset_type = coalesce((
--         select array_agg(at.value order by at.value)
--         from pipedrive_data.deals__asset_type at
--         join pipedrive_data.deals pd on pd._dlt_id = at._dlt_parent_id
--         where pd.id = p_deal_id
--     ), '{}')
--     where d.id = p_deal_id;
-- end;
-- $$;

-- create or replace function sync_deal_financing_type(p_deal_id bigint)
-- returns void language plpgsql as $$
-- begin
--     update public.deals d
--     set financing_type = coalesce((
--         select array_agg(ft.value order by ft.value)
--         from pipedrive_data.deals__financing_type ft
--         join pipedrive_data.deals pd on pd._dlt_id = ft._dlt_parent_id
--         where pd.id = p_deal_id
--     ), '{}')
--     where d.id = p_deal_id;
-- end;
-- $$;

-- -- Asset type trigger
-- create or replace function trigger_sync_deal_asset_type()
-- returns trigger language plpgsql as $$
-- declare
--     v_deal_id bigint;
-- begin
--     select id into v_deal_id
--     from pipedrive_data.deals
--     where _dlt_id = new._dlt_parent_id;

--     if v_deal_id is not null then
--         perform sync_deal_asset_type(v_deal_id);
--     end if;

--     return new;
-- end;
-- $$;

drop trigger if exists trg_sync_deal_asset_type on pipedrive_data.deals__asset_type;

create trigger trg_sync_deal_asset_type
after insert or update or delete on pipedrive_data.deals__asset_type
for each row execute function trigger_sync_deal_asset_type();


-- -- Financing type trigger
-- create or replace function trigger_sync_deal_financing_type()
-- returns trigger language plpgsql as $$
-- declare
--     v_deal_id bigint;
-- begin
--     select id into v_deal_id
--     from pipedrive_data.deals
--     where _dlt_id = new._dlt_parent_id;

--     if v_deal_id is not null then
--         perform sync_deal_financing_type(v_deal_id);
--     end if;

--     return new;
-- end;
-- $$;

drop trigger if exists trg_sync_deal_financing_type on pipedrive_data.deals__financing_type;

create trigger trg_sync_deal_financing_type
after insert or update or delete on pipedrive_data.deals__financing_type
for each row execute function trigger_sync_deal_financing_type();

-- Backfill asset_type
do $$
declare
    r record;
begin
    for r in select id from pipedrive_data.deals loop
        perform sync_deal_asset_type(r.id);
    end loop;
end;
$$;

-- Backfill financing_type
do $$
declare
    r record;
begin
    for r in select id from pipedrive_data.deals loop
        perform sync_deal_financing_type(r.id);
    end loop;
end;
$$;









create or replace function public.sync_deal_from_pipedrive(p_deal_id bigint)
returns void
language plpgsql
security invoker
set search_path to public, pipedrive_data, extensions
as $$
begin
  -- Upsert the core deal fields from pipedrive_data.deals into public.deals
  insert into public.deals (
    id,
    title,
    value,
    currency,
    stage,
    status,
    probability,
    organization_id,
    primary_contact_id,
    owner_user_id,
    financing_type,
    deal_assist_user,
    capital_advisor_fee,
    referral_fee,
    referral_partner_id,
    winning_capital_provider_id,
    occupancy,
    ground_lease,
    property_address,
    asset_type,
    investment_strategy,
    tenancy,
    hotel_flag_id,
    hotel_type,
    single_tenant_name_id,
    guarantor_type,
    sponsor_location,
    experience_level,
    net_worth,
    liquidity,
    assets_under_management,
    credit_score,
    us_citizenship,
    deal_file_folder_link,
    offering_memorandum_link,
    add_time,
    won_time,
    lost_time,
    close_time,
    expected_close_date,
    last_synced_at,
    created_at,
    updated_at
  )
  select
    d.id,
    d.title,
    d.value,
    d.currency,
    d.stage_id,
    d.status,
    null as probability,
    org.id as organization_id,
    c.id   as primary_contact_id,
    d.user_id__id as owner_user_id,
    coalesce(array(
      select distinct v.value::text
      from pipedrive_data.deals__financing_type v
      where v._dlt_parent_id = d._dlt_id
      order by 1
    ), '{}'::text[]) as financing_type,
    d.deal_assist__id,
    d.capital_advisor_fee,
    d.referral_fee,
    ref.id as referral_partner_id,
    win_org.id as winning_capital_provider_id,
    d.occupancy,
    d.ground_lease,
    d.full_combined_address_of_property_address as property_address,
    '{}'::text[] as asset_type, -- populated by sync_deal_asset_type after upsert
    d.investment_strategy,
    d.tenancy,
    hotel_org.id as hotel_flag_id,
    d.hotel_type,
    tenant_org.id as single_tenant_name_id,
    d.guarantor_type,
    d.full_combined_address_of_sponsor_location as sponsor_location,
    d.experience_level,
    d.net_worth,
    d.liquidity,
    null as assets_under_management,
    d.credit_score,
    d.us_citizenship,
    d.deal_file_folder_link,
    d.offering_memorandum_link,
    d.add_time,
    d.won_time,
    d.lost_time,
    d.close_time,
    d.expected_close_date::date,
    now() as last_synced_at,
    coalesce((select created_at from public.deals where id = d.id), now()) as created_at,
    now() as updated_at
  from pipedrive_data.deals d
  left join public.organizations org
    on org.pipedrive_id = d.org_id__value and org.pipedrive_id is not null
  left join public.contacts c
    on c.pipedrive_id = d.person_id__value and c.pipedrive_id is not null
  left join public.contacts ref
    on ref.pipedrive_id = d.referral_partner__value and ref.pipedrive_id is not null
  left join public.organizations win_org
    on win_org.pipedrive_id = d.winning_capital_provider__value and win_org.pipedrive_id is not null
  left join public.organizations hotel_org
    on hotel_org.pipedrive_id = d.hotel_flag__value and hotel_org.pipedrive_id is not null
  left join public.organizations tenant_org
    on tenant_org.pipedrive_id = d.single_tenant_name__value and tenant_org.pipedrive_id is not null
  where d.id = p_deal_id
  on conflict (id) do update set
    title = excluded.title,
    value = excluded.value,
    currency = excluded.currency,
    stage = excluded.stage,
    status = excluded.status,
    probability = excluded.probability,
    organization_id = excluded.organization_id,
    primary_contact_id = excluded.primary_contact_id,
    owner_user_id = excluded.owner_user_id,
    financing_type = excluded.financing_type,
    deal_assist_user = excluded.deal_assist_user,
    capital_advisor_fee = excluded.capital_advisor_fee,
    referral_fee = excluded.referral_fee,
    referral_partner_id = excluded.referral_partner_id,
    winning_capital_provider_id = excluded.winning_capital_provider_id,
    occupancy = excluded.occupancy,
    ground_lease = excluded.ground_lease,
    property_address = excluded.property_address,
    -- asset_type is maintained by sync_deal_asset_type; do not overwrite here
    investment_strategy = excluded.investment_strategy,
    tenancy = excluded.tenancy,
    hotel_flag_id = excluded.hotel_flag_id,
    hotel_type = excluded.hotel_type,
    single_tenant_name_id = excluded.single_tenant_name_id,
    guarantor_type = excluded.guarantor_type,
    sponsor_location = excluded.sponsor_location,
    experience_level = excluded.experience_level,
    net_worth = excluded.net_worth,
    liquidity = excluded.liquidity,
    assets_under_management = excluded.assets_under_management,
    credit_score = excluded.credit_score,
    us_citizenship = excluded.us_citizenship,
    deal_file_folder_link = excluded.deal_file_folder_link,
    offering_memorandum_link = excluded.offering_memorandum_link,
    add_time = excluded.add_time,
    won_time = excluded.won_time,
    lost_time = excluded.lost_time,
    close_time = excluded.close_time,
    expected_close_date = excluded.expected_close_date,
    last_synced_at = excluded.last_synced_at,
    updated_at = excluded.updated_at;

  -- After core upsert, recompute derived arrays for financing and asset types.
  -- Financing type array is already computed above, but recompute to ensure UPDATE path applies even if no row inserted
  update public.deals d2 set financing_type = coalesce(
    array(
      select distinct v.value::text from pipedrive_data.deals__financing_type v
      join pipedrive_data.deals pd on pd._dlt_id = v._dlt_parent_id
      where pd.id = p_deal_id
      order by 1
    ), '{}'::text[]
  )
  where d2.id = p_deal_id;

  -- Rebuild deal_asset_types mapping and sync the aggregate array on deals
  perform public.map_all_deal_asset_types_for_deal(p_deal_id);
  perform public.sync_deal_asset_type(p_deal_id);
end;
$$;