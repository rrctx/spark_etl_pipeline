COPY(
    SELECT
        id, name, localized_name, primary_attr, attack_type, roles, query_date
    FROM
        opendota_main.heroes_dm
    WHERE
        cast(query_date as date) <= '{{ ds }}')
TO '{{ params.temp_filtered_dota_heroes }}' WITH (FORMAT CSV, HEADER)