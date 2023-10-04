import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Iterable
import configparser
from typing import Any, Optional, Dict, List, Union, Tuple
import MySQLdb
import pandas as pd
from MySQLdb.cursors import DictCursor
import argparse
from MySQLdb._exceptions import MySQLError
import sys
from dash import callback_context
import calculated
import charges
import operation_run_stats
import ts_stat
import unitrecipe_run_stats

import re
import subprocess
import uuid

Options = List[Dict[str, str]]


def parse_config() -> configparser.RawConfigParser:
    config = configparser.ConfigParser()
    config.read('../app/config.ini')
    if config['environment']['env'] not in ('prod', 'test'):
        config['database']['enet-schema'] = 'enet_dev'
    return config


def connect_to_db() -> MySQLdb.Connection:
    cfg = parse_config()
    db_cfg = cfg['database']
    return MySQLdb.Connect(host=db_cfg['host'],
                           port=int(db_cfg.get('port', '3306')),
                           user=db_cfg['user'],
                           password=db_cfg['password'],
                           db=db_cfg['enet-schema'],
                           charset='utf8',
                           cursorclass=DictCursor)


def run_query(query: str,
              query_params: Optional[Union[Dict[str, Union[str, int]], List[Union[str, int]]]] = None
              ) -> Optional[List[Dict[str, Any]]]:
    conn = connect_to_db()
    cursor = conn.cursor()
    try:
        cursor.execute(query, query_params)
    except MySQLdb.MySQLError as e:
        conn.rollback()
        # from ... import app
        # app.app.logger.error(f'database error during query execution: {str(e)} - query: {query} - query_params: {query_params}')
        raise
    else:
        # néha a cursor.nextset() hívásnál száll el a query
        # például inserteknél, amikor kulcs duplikáció lép fel
        try:
            results = [cursor.fetchall()]
            while cursor.nextset():
                results.append(cursor.fetchall())
            results = [res for res in results if res != ()]  # discard the not empty results
            conn.commit()
            if results:
                return list(results[-1])  # for now we only return the last result set
            else:
                return None
        except MySQLdb.MySQLError as e:
            conn.rollback()
            query_oneline = query.replace('\n', ' ')
            message = (f'database error during query execution: {str(e)} - '
                       f'query: {query_oneline} - '
                       f'query_params: {query_params}')
            # from app import app
            # app.logger.error(message)
            raise
    finally:
        cursor.close()
        conn.close()


def insert_df(cursor: MySQLdb.cursors.DictCursor, df: pd.DataFrame) -> None:
    """
    Egy dataframe tartalmát adatbázisba insertálja 1000-es batchekben.
    A tábla amibe insertál, a df dataframe df.name attribútumában kell,
    hogy legyen. A tábla oszlopainak nevét a dataframe oszlopneveiből veszi.
    :param cursor: Az adatbáziskapcsolatból nyitott kurzor
    :param df: Az összes szükséges adatot tartalmazó pandas dataframe
    :return: None
    """
    # a hierarchikus indexet oszlopokká alakítjuk
    df.reset_index(inplace=True)
    # df.columns = [level0 for level0, level1 in df.columns.values]
    insert = f"insert into {df.name} ({', '.join(df.columns)}) values "
    insert_rows_values = df_to_insert_values(df)
    step = 1000
    for i in range(0, len(insert_rows_values), step):
        insert_vals = insert_rows_values[i:i + step].str.cat(sep=', ')
        insert_vals = insert_vals.replace("'NULL'", "NULL")
        cursor.execute(insert + insert_vals)


def df_to_insert_values(df: pd.DataFrame) -> pd.DataFrame:
    # a string oszlopok értékei köré idézőjeleket teszünk
    stats_str = df.select_dtypes(include='object')
    df.loc[:, stats_str.columns] = stats_str.apply(lambda x: "'" + x + "'", axis=1)
    insert_values = '(' + df.astype(str).apply(', '.join, axis=1) + ')'
    return insert_values


def update_stat_state(stat_id: int, state: str) -> None:
    """
    A statisztikákat tartalmazó táblában frissítjük az adott statisztika státuszát.
    """
    query_params = {'stat_id': stat_id, 'state': state}
    upd_query = 'update run_stat set state = %(state)s where id = %(stat_id)s'
    run_query(upd_query, query_params)


def update_ts_state(stat_id: int, state: str) -> None:
    """
    A statisztikákat tartalmazó táblában frissítjük az adott statisztika státuszát.
    """
    query_params = {'stat_id': stat_id, 'state': state}
    upd_query = 'update ts_stat set state = %(state)s where id = %(stat_id)s'
    run_query(upd_query, query_params)


def execute_inserts(db_conn: MySQLdb.Connection, data: Union[Iterable[pd.DataFrame], pd.DataFrame]) -> None:
    """
    Egy tranzakción belül batch-ekben lefuttatja a megadott dataframeket.
    Hiba esetén rollbackel mindent és kivételt dob.
    :return: None
    """
    if not isinstance(data, Iterable):
        data = [data]
    db_conn.rollback()  # csak a rend kedvéért
    db_conn.begin()
    try:
        cursor = db_conn.cursor()
    except MySQLdb.MySQLError as ex:
        raise ex
    else:
        try:
            for df in data:
                insert_df(cursor, df)
        except MySQLdb.MySQLError as ex:
            db_conn.rollback()
            raise ex
        else:
            db_conn.commit()
        finally:
            cursor.close()


def recipe_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == '1':
        recipe_view = 'k1_apimpp_1_gb_batch'
    elif api == '2':
        recipe_view = 'k1_apimpp_2_gb_batch'
    else:
        raise ValueError

    # recept statisztikák
    # receptszám és recept csoport alapján csoportosítva számolunk statisztikákat
    # azokra a receptekre amik a kiválasztott sarzsokban szerepelnek
    query_recipes = f'''
select v.RecipeGrpNum, 
       v.RecipeNum, 
       timestampdiff(minute, v.Started, v.Finished) as duration,
       timestampdiff(minute, c.started, v.Started) as offset
  from charges c
  join run_stat_charges rsc
       on c.charge_nr = rsc.charge_nr
           and c.api = %(api)s
           and date(c.started) = rsc.charge_start_date
           and rsc.stat_id = %(stat_id)s
  join efr_data.{recipe_view} v
       on v.ChargeNr = c.charge_nr
           and v.Started between c.started and c.finished
'''
    recipes_data = run_query(query_recipes, {'api': api, 'stat_id': stat_id})
    recipes_df = pd.DataFrame().from_records(recipes_data)
    stats = recipes_df.groupby(['RecipeGrpNum', 'RecipeNum']).agg('describe')
    stats['stat_id'] = stat_id
    stats.columns = [f'{level0}_{level1}' if level1 is not '' else level0
                     for level0, level1 in stats.columns.values]
    stats.drop(labels=['duration_count', 'duration_mean', 'duration_std',
                       'offset_count', 'offset_mean', 'offset_std'],
               axis='columns', inplace=True)
    stats.columns = [column.replace('25%', 'q1').replace('75%', 'q3').replace('50%', 'median')
                     for column in stats.columns]
    stats.name = 'recipe_run_stats'

    return stats


def create_stat_charges(api: Optional[str], itemnr: Optional[str],
                        months: Optional[List[str]]):
    if api == '1':
        recipe_view = 'k1_apimpp_1_gb_batch'
    elif api == '2':
        recipe_view = 'k1_apimpp_2_gb_batch'
    else:
        return []
    if months is None or not len(months):
        return []

    months_param_list = ', '.join([f'%(month_{n})s' for n in range(len(months))])
    charges_query = f'''
select distinct concat(charge_nr, date_format(c.started, ' (%%Y-%%m-%%d)')) as charge
from charges c
join efr_data.{recipe_view} v
    on c.charge_nr = v.ChargeNr
    and v.Started between c.started and c.finished
    and c.api = %(api)s
join recipe_details rd
    on rd.recipe_group = v.RecipeGrpNum
    and rd.recipe_num = v.RecipeNum
    and rd.item_num = %(item_nr)s
where date_format(c.started, '%%Y-%%m') in ({months_param_list})
'''
    query_params = {f'month_{n}': month for n, month in enumerate(months)}
    query_params.update({'item_nr': itemnr, 'api': api})
    charge_rows = run_query(charges_query, query_params)
    options = [charge_row['charge']
               for charge_row in charge_rows] if charge_rows is not None else []
    return options


def create_new_run_stat(api, itemnr, name):
    # hónapok amikor futott ez az Item
    months = stat_time_chkbxs(itemnr, api)

    # aktuális hónap
    current_month = datetime.now().strftime('%Y-%m')

    # ha az aktuális hónapban futott, akkor csinálunk róla stat-ot
    if current_month in months:
        charges_dates = create_stat_charges(api, itemnr, [current_month])
        # charges_dates = [date for date in create_stat_charges(api, itemnr, months) if current_month in date]
    else:
        return

    # hiányos sarzs-ot töröljük
    for current_charges_date in charges_dates:
        if not re.search('[0-9]+-[0-9]+ \([0-9]{4}-[0-9]{2}-[0-9]{2}\)', current_charges_date):
            charges_dates.remove(current_charges_date)

    # név ellenőrzése
    exists = run_query('select 1 from run_stat where stat_name = %(name)s',
                       {'name': name})
    if exists is not None:
        # ha már létezik ilyen nevű, ideiglenes nevet adunk az újnak
        used_name = f'{name}-{str(uuid.uuid4())[:6]}'
    else:
        used_name = name

    # run_stat létrehozása
    current_date = datetime.now()
    insert_stat_query = (
        'insert into run_stat (stat_name, api, item_nr, state, default_stat, update_date) '
        'values (%(name)s, %(api)s, %(item_nr)s, %(state)s, 0, %(current_date)s)')
    state = 'Folyamatban'
    run_query(insert_stat_query, {'name': used_name, 'api': api,
                                  'item_nr': itemnr, 'state': state, 'current_date': current_date})

    stat_id: int = run_query('select id from run_stat where stat_name = %(name)s',
                             {'name': used_name})[0]['id']

    # run_stat_charges létrehozása
    charges_dates = [re.search('(\d+-\d+) \((\d\d\d\d-\d\d-\d\d)\)', charge_date).groups()
                     for charge_date in charges_dates]
    insert_values = ', '.join([f'({stat_id}, "{charge}", "{date}")' for charge, date in charges_dates])
    insert_stat_charges_query = (
        f'insert into run_stat_charges (stat_id, charge_nr, charge_start_date) '
        f'values {insert_values}')
    run_query(insert_stat_charges_query)

    # recipe_run_stat
    recipe_stats = recipe_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db(), [recipe_stats])
    # unit_recipe_run_stat
    unitrecipe_stats = unitrecipe_run_stats.unitrecipe_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db(), [unitrecipe_stats])
    # operation_run_stat
    operation_stats = operation_run_stats.operation_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db(), [operation_stats])
    update_stat_state(stat_id, 'Siker')


def create_new_sensor_stat(api, itemnr, median_iqr, multiplier_iqr, multiplier_std, name):
    # hónapok amikor futott ez az Item
    months = stat_time_chkbxs(itemnr, api)

    # aktuális hónap
    current_month = datetime.now().strftime('%Y-%m')

    # ha az aktuális hónapban futott, akkor csinálunk róla stat-ot
    if current_month in months:
        charges_dates = create_stat_charges(api, itemnr, [current_month])
        # charges_dates = [date for date in create_stat_charges(api, itemnr, months) if current_month in date]
    else:
        return

    # ellenőrizzük, hogy ilyen statisztika létezik-e már
    exists = run_query('select 1 from ts_stat where stat_name = %(name)s', {'name': name})
    if exists is not None:
        # ha már létezik, ideiglenes nevet adunk az újnak
        used_name = f'{name}-{str(uuid.uuid4())[:6]}'
    else:
        used_name = name
    current_date = datetime.now()
    insert_stat_query = (
        'insert into ts_stat (stat_name, api, item_nr, state, default_stat, use_median_iqr, deviation_multiplier_iqr, deviation_multiplier_std, create_date) '
        'values (%(name)s, %(api)s, %(item_nr)s, %(state)s, 0, %(median_iqr)s, %(multiplier_iqr)s, %(multiplier_std)s, %(current_date)s)')
    state = 'Folyamatban'
    run_query(insert_stat_query, {'name': used_name, 'api': api,
                                  'item_nr': itemnr, 'state': state,
                                  'median_iqr': median_iqr,
                                  'multiplier_iqr': str(multiplier_iqr).replace(',', '.'),
                                  'multiplier_std': str(multiplier_std).replace(',', '.'),
                                  'current_date': current_date})
    stat_id: int = run_query('select id from ts_stat where stat_name = %(name)s', {'name': used_name})[0]['id']

    charges_dates = [re.search('(\d+-\d*) \((\d\d\d\d-\d\d-\d\d)\)', charge_date).groups()
                     for charge_date in charges_dates]
    insert_values = ', '.join([f'({stat_id}, "{charge}", "{date}")' for charge, date in charges_dates])
    insert_stat_charges_query = (
        f'insert into ts_stat_charges (stat_id, charge_nr, charge_start_date) '
        f'values {insert_values}')
    run_query(insert_stat_charges_query)
    ts_stat.operation_stats(stat_id)
    update_ts_state(stat_id, 'Siker')


def stat_time_chkbxs(itemnr: str, api: str):
    if api == '1':
        recipe_view = 'k1_apimpp_1_gb_batch'
    elif api == '2':
        recipe_view = 'k1_apimpp_2_gb_batch'
    else:
        return []

    query_months = f'''
select distinct date_format(c.started, '%%Y-%%m') as month
  from efr_data.{recipe_view} v
  join recipe_details rd
       on v.RecipeGrpNum = rd.recipe_group
           and v.RecipeNum = rd.recipe_num
           and rd.item_num = %(item_num)s
  join charges c
       on c.charge_nr = v.ChargeNr
           and v.Started between c.started and c.finished
 where 1 = 1'''

    month_rows = run_query(query_months, {'item_num': itemnr})
    options = [month_row['month']
               for month_row in month_rows] if month_rows is not None else []
    return options


def create_all_run_stat_def():
    sql_files = ['operation_run_stat_def.sql', 'recipe_run_stats_def.sql', 'unitrecipe_run_stats_def.sql']
    for file in sql_files:
        with open(file, 'r') as f:
            update_run_stat_def = f.read()
        f.close()
        run_query(update_run_stat_def)


def get_itemnr():
    api_itemnr = []
    for api in [1, 2]:
        recipe_view = 'k1_apimpp_1_gb_batch' if api == 1 else 'k1_apimpp_2_gb_batch'
        itemnrs = run_query(f'''
            select distinct rd.item_num
              from efr_data.{recipe_view} v
              join recipe_details rd
                   on v.RecipeGrpNum = rd.recipe_group
                       and v.RecipeNum = rd.recipe_num ''')

        for itemnr in itemnrs:
            api_itemnr.append([str(api), itemnr['item_num']])

    return api_itemnr


if __name__ == '__main__':
    # # calculated.py-t futtatja
    # calculated.calc_ts()
    #
    # # charges.py-t futtatja
    parser_charges = argparse.ArgumentParser()
    parser_charges.add_argument('--maxdays', type=int)
    args_charges = parser_charges.parse_args()
    args_charges.maxdays = int(parse_config()['parameters']['maxdays'])
    charges.define_charges(args_charges.maxdays)

    # recipe_run_stats.py-t futtatja
    # unitrecipe_run_stats.py-t futtatja
    # operation_run_stats.py-t futtatja
    # run_stat-ot, run_stat_charges-t futtatja

    api_itemnr = get_itemnr()

    for api, itemnr in api_itemnr:
        create_new_run_stat(api=api, itemnr=itemnr, name='auto')

    create_all_run_stat_def()

    # ts_stat-ot futtatja
    for api, itemnr in api_itemnr:
        create_new_sensor_stat(api=api, name='auto', itemnr=itemnr, median_iqr=1, multiplier_iqr=1, multiplier_std=1)
