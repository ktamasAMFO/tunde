from datetime import timedelta as td
import pandas as pd
import configparser
import argparse
from typing import Any, Optional, Dict, List
import pyodbc


query_update = '''
update charges 
set finished = ?
where id = ?'''

query_charge_finish = '''
select case when max(coalesce(Finished, GETDATE())) = GETDATE() then null else max(Finished) end as finish
from {recipe_view}
where ChargeNr = '{charge_nr}'
and Started between '{started}' and {{fn TIMESTAMPADD(SQL_TSI_DAY, {maxdays}, '{started}')}}'''

query_new_recipes = '''
    select ChargeNr, Started, Finished
    from {recipe_view}
    where 1 = 1
        and ChargeNr <> ''
        and ChargeNr not like 'T%%'
        and Started > (select {{fn TIMESTAMPADD(SQL_TSI_DAY, -{maxdays}, coalesce(max(started), '1900-01-01'))}}
                        from charges
                        where api = '{api}')
    order by Started
'''

query_new_recipes_kemia = '''
select distinct ChargeID as 'ChargeNr',
       RecipeStartTime as Started,
       NULL as Finished
from {tabla2}
    LEFT JOIN {recipe_view} g
        ON RecipeNr = g.recipenum
        and RecipeStartTime = g.[Started]
WHERE g.recipenum IS NULL
'''

query_charges = '''select id, api, charge_nr, started, finished from charges'''

query_insert_charge = '''
insert into charges (api, charge_nr, started, finished) 
values (?, ?, ?, ?)'''


def define_charges(maxdays: int) -> None:
    apis = (('EFR_S0000034.k1_apimpp1_gb_batch_view_tunde_view', '1'), ('EFR_S0000067.k1_apimpp2_gb_batch_view_tunde_view', '2'))

    for recipe_view, api in apis:
        query_params = {'api': api, 'maxdays': maxdays, 'recipe_view': recipe_view}
        sql_query_new_recipes = query_new_recipes.format(**query_params)
        new_recipes_rows = run_query(sql_query_new_recipes) or []
        new_recipes = pd.DataFrame(new_recipes_rows)

        for idx, recipe in new_recipes.iterrows():
            recipe['ChargeNr'] = recipe['ChargeNr'].strip()
            charges_rows = run_query(query_charges) or []
            for row in charges_rows:
                row['api'] = row['api'].strip()
                row['charge_nr'] = row['charge_nr'].strip()
            charges = pd.DataFrame.from_records(charges_rows)

            overlap = charges[
                (charges['api'] == api) &
                (recipe['ChargeNr'] == charges['charge_nr']) &
                (recipe['Started'] <= charges['started'] + td(days=maxdays)) &
                (recipe['Started'] >= charges['started'])
                ] if len(charges) else pd.DataFrame()

            if len(overlap) > 1:
                print(overlap)
                print(recipe)
                raise AssertionError
            if len(overlap) == 0:
                # új sarzsot viszünk fel
                query_params = {'maxdays': maxdays,
                                'started': recipe['Started'],
                                'charge_nr': recipe['ChargeNr'],
                                'recipe_view': recipe_view
                                }
                sql_query_charge_finish = query_charge_finish.format(**query_params)
                charge_finish = run_query(sql_query_charge_finish)[0]['finish']

                query_params = {
                    'api': api,
                    'charge_nr': recipe['ChargeNr'],
                    'started': recipe['Started'],
                    'finished': charge_finish}

                insert(query_params)

            else:
                # a meglévőt frissítjük
                finished_table = overlap['finished'].iloc[0]
                finished_recipe = recipe['Finished']
                if not pd.isna(finished_recipe):
                    if not pd.isna(finished_table):
                        max_finish = max(finished_table, finished_recipe) if not pd.isna(finished_recipe) else None
                    else:
                        max_finish = finished_recipe
                else:
                    max_finish = None
                query_params = {'id': overlap['id'].iloc[0], 'finished': max_finish}

                update(query_params)
                # sql_query_update = query_update.format(**query_params)
                # run_query(sql_query_update)


def define_charges_kemia(maxdays: int) -> None:
    apis = (('EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view', 'kemia1'),
            ('EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view', 'kemia2'),
            ('EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view', 'kemia3'))

    for recipe_view, api in apis:
        if api == 'kemia1':
            tabla2 = 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view'
        elif api == 'kemia2':
            tabla2 = 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view'
        else:
            tabla2 = 'EFR_24EPFIR.k2_24epfir_batch_data_operation_p_tunde_view'

        query_params = {'api': api, 'maxdays': maxdays, 'recipe_view': recipe_view}
        sql_query_new_recipes = query_new_recipes.format(**query_params)
        new_recipes_rows = run_query(sql_query_new_recipes) or []
        new_recipes = pd.DataFrame(new_recipes_rows)

        query_params = {'tabla2': tabla2, 'recipe_view': recipe_view}
        sql_query_new_recipes_kemia = query_new_recipes_kemia.format(**query_params)
        new_recipes_rows_kemia = run_query(sql_query_new_recipes_kemia) or []
        new_recipes_rows_kemia = pd.DataFrame(new_recipes_rows_kemia)

        new_recipes = pd.concat([new_recipes, new_recipes_rows_kemia])

        for idx, recipe in new_recipes.iterrows():
            if type(recipe['ChargeNr']) is pd.Timestamp:
                recipe['ChargeNr'] = int(new_recipes.loc[idx, 'ChargeNr'])
                recipe['ChargeNr'] = str(recipe['ChargeNr'])
            recipe['ChargeNr'] = recipe['ChargeNr'].strip()
            charges_rows = run_query(query_charges) or []
            for row in charges_rows:
                row['api'] = row['api'].strip()
                row['charge_nr'] = row['charge_nr'].strip()
            charges = pd.DataFrame.from_records(charges_rows)

            overlap = charges[
                (charges['api'] == api) &
                (recipe['ChargeNr'] == charges['charge_nr']) &
                (recipe['Started'] <= charges['started'] + td(days=maxdays)) &
                (recipe['Started'] >= charges['started'])
                ] if len(charges) else pd.DataFrame()

            if len(overlap) > 1:
                print(overlap)
                print(recipe)
                raise AssertionError
            if len(overlap) == 0:
                # új sarzsot viszünk fel
                query_params = {'maxdays': maxdays,
                                'started': recipe['Started'],
                                'charge_nr': recipe['ChargeNr'],
                                'recipe_view': recipe_view
                                }
                sql_query_charge_finish = query_charge_finish.format(**query_params)
                charge_finish = run_query(sql_query_charge_finish)[0]['finish']

                query_params = {
                    'api': api,
                    'charge_nr': recipe['ChargeNr'],
                    'started': recipe['Started'],
                    'finished': charge_finish}

                insert(query_params)

            else:
                # a meglévőt frissítjük
                finished_table = overlap['finished'].iloc[0]
                finished_recipe = recipe['Finished']
                if not pd.isna(finished_recipe):
                    if not pd.isna(finished_table):
                        max_finish = max(finished_table, finished_recipe) if not pd.isna(finished_recipe) else None
                    else:
                        max_finish = finished_recipe
                else:
                    max_finish = None
                query_params = {'id': overlap['id'].iloc[0], 'finished': max_finish}

                update(query_params)
                # sql_query_update = query_update.format(**query_params)
                # run_query(sql_query_update)


def update(query_params):
    conn, cursor = connect_to_db()
    cursor.execute(query_update, query_params['finished'], int(query_params['id']))
    conn.commit()
    cursor.close()
    conn.close()


def insert(query_params):
    conn, cursor = connect_to_db()
    cursor.execute(query_insert_charge, query_params['api'], query_params['charge_nr'],
                   query_params['started'], query_params['finished'])
    conn.commit()
    cursor.close()
    conn.close()


def parse_config() -> configparser.RawConfigParser:
    config = configparser.ConfigParser()
    config.read('./config.ini')
    if config['environment']['env'] not in ('prod', 'test'):
        config['database']['enet-schema'] = 'enet_dev'
    return config


def connect_to_db():
    server = 'hun-dev-claudia-psql01.database.windows.net'
    database = 'HUN-dev-claudia-Pdb01'
    username = 'tunde_dev_app'
    password = '{70afrwot6KGthd3T2QWbzwkE}'
    driver = '{ODBC Driver 18 for SQL Server}'

    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            return conn, cursor


def run_query(query: str) -> Optional[List[Dict[str, Any]]]:
    conn, cursor = connect_to_db()
    try:
        cursor.execute(query)

        columnNames = [column[0] for column in cursor.description]
        results = []

        for record in cursor.fetchall():
            results.append(dict(zip(columnNames, record)))
        # print(results)
        conn.commit()
        if results:
            return results
        else:
            return None
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    define_charges(maxdays=8)
    define_charges_kemia(maxdays=8)
