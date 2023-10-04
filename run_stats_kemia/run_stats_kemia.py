from datetime import timedelta as td
import pandas as pd
import configparser
import argparse
from MySQLdb.cursors import DictCursor
import MySQLdb
from typing import Any, Optional, Dict, List, Union
from datetime import datetime
import re
import uuid
import ts_stat_kemia
from typing import Iterable
import pyodbc


def create_new_run_stat(api, itemnr, name):
    # hónapok amikor futott ez az Item
    months = stat_time_chkbxs(itemnr, api)

    # aktuális hónap
    # current_month = datetime.now().strftime('%Y-%m')
    #
    # # ha az aktuális hónapban futott, akkor csinálunk róla stat-ot
    # if current_month in months:
    #     charges_dates = create_stat_charges(api, itemnr, [current_month])
    #     # charges_dates = [date for date in create_stat_charges(api, itemnr, months) if current_month in date]
    # else:
    #     return

    charges_dates = create_stat_charges(api, itemnr, [months[-1]])

    # hiányos sarzs-ot töröljük
    # for current_charges_date in charges_dates:
    #     if not re.search('[0-9]+-[0-9]+ \([0-9]{4}-[0-9]{2}-[0-9]{2}\)', current_charges_date):
    #         charges_dates.remove(current_charges_date)

    # név ellenőrzése
    sql_stat_id = '''select 1 from run_stat where stat_name = '{name}' '''
    sql_stat_id = sql_stat_id.format(**{'name': name})
    exists = run_query(sql_stat_id)
    if exists is not None:
        # ha már létezik ilyen nevű, ideiglenes nevet adunk az újnak
        used_name = f'{name}-{str(uuid.uuid4())[:6]}'
    else:
        used_name = name

    # run_stat létrehozása
    # current_date = datetime.now()
    insert_stat_query = '''insert into run_stat (stat_name, api, item_nr, state, default_stat) values ('{name}', '{api}', '{item_nr}', '{state}', 0)'''
    state = 'Folyamatban'
    sql_insert_stat_query = insert_stat_query.format(**{'name': used_name, 'api': 'kemia'+api, 'item_nr': itemnr, 'state': state})
    insert(sql_insert_stat_query)

    query = '''select id from run_stat where stat_name = '{name}' '''.format(**{'name': used_name})
    stat_id: int = run_query(query)[0]['id']

    # run_stat_charges létrehozása
    if itemnr == '70160110' and api == '2':
        charges_dates = [re.search('(\d+/\w+/\d+)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates]
    else:
        charges_dates = [re.search('(\d+/\d+\D*)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates]

    insert_values = ', '.join([f"({stat_id}, '{charge}', '{date}')" for charge, date in charges_dates])
    insert_stat_charges_query = (
       f'insert into run_stat_charges (stat_id, charge_nr, charge_start_date) '
       f'values {insert_values}')
    insert(insert_stat_charges_query)

    # recipe_run_stat
    recipe_stats = recipe_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db()[0], [recipe_stats])
    # unit_recipe_run_stat
    unitrecipe_stats = unitrecipe_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db()[0], [unitrecipe_stats])
    # operation_run_stat
    operation_stats = operation_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db()[0], [operation_stats])
    update_stat_state(stat_id, 'Siker')


def insert(query):
    conn, cursor = connect_to_db()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def stat_time_chkbxs(itemnr: str, api: str):
    if api == '1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
    elif api == '2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
    else:
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'

    query_months = '''
select distinct format(c.started, 'yyyy-MM') as month
  from {recipe_view} v
  join recipe_details_2 rd
       on v.Area = rd.recipe_group
           and v.RecipeNum = rd.recipe_num
           and rd.item_num = {item_num}
  join charges c
       on c.charge_nr = v.ChargeNr
           and v.Started between c.started and c.finished'''
    sql_query_months = query_months.format(**{'item_num': itemnr, 'recipe_view': recipe_view})
    month_rows = run_query(sql_query_months)
    options = [month_row['month']
               for month_row in month_rows] if month_rows is not None else []
    return options


def create_stat_charges(api: Optional[str], itemnr: Optional[str],
                        months: Optional[List[str]]):
    if api == '1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        api = 'kemia1'
    elif api == '2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        api = 'kemia2'
    else:
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        api = 'kemia3'

    if months is None or not len(months):
        return []

    months_param_list = ', '.join([f'{{month_{n}}}' for n in range(len(months))])
    charges_query = '''
select distinct concat(trim(charge_nr), format(c.started, ' (yyyy-MM-dd)')) as charge
from charges c
join {recipe_view} v
    on c.charge_nr = v.ChargeNr
    and v.Started between c.started and c.finished
    and c.api = '{api}'
join recipe_details_2 rd
    on rd.recipe_group = v.Area
    and rd.recipe_num = v.RecipeNum
    and rd.item_num = {item_nr}
where format(c.started, 'yyyy-MM') in ('{months_param_list}')
'''

    query_params = {'item_nr': itemnr, 'api': api, 'recipe_view': recipe_view,
                         'months_param_list': months_param_list}
    sql_charges_query = charges_query.format(**query_params)

    query_params = {f'month_{n}': month for n, month in enumerate(months)}
    sql_charges_query = sql_charges_query.format(**query_params)

    charge_rows = run_query(sql_charges_query)
    options = [charge_row['charge']
               for charge_row in charge_rows] if charge_rows is not None else []
    return options


def recipe_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == '1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        api = 'kemia1'
    elif api == '2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        api = 'kemia2'
    else:
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        api = 'kemia3'

    # recept statisztikák
    # receptszám és recept csoport alapján csoportosítva számolunk statisztikákat
    # azokra a receptekre amik a kiválasztott sarzsokban szerepelnek
    query_recipes = '''
select v.Area as RecipeGrpNum, 
       v.RecipeNum, 
       DATEDIFF(minute, v.Started, v.Finished) as duration,
       DATEDIFF(minute, c.started, v.Started) as offset
  from charges c
  join run_stat_charges rsc
       on c.charge_nr = rsc.charge_nr
           and c.api = '{api}'
           and format(c.started, 'yyyy-MM-dd') = rsc.charge_start_date
           and rsc.stat_id = {stat_id}
  join {recipe_view} v
       on v.ChargeNr = c.charge_nr
           and v.Started between c.started and c.finished
'''
    query_params = {'api': api, 'stat_id': stat_id, 'recipe_view': recipe_view}
    sql_query_recipes = query_recipes.format(**query_params)
    recipes_data = run_query(sql_query_recipes)
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
    stats.name = 'recipe_run_stats_2'

    return stats


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


def execute_inserts(db_conn, data: Union[Iterable[pd.DataFrame], pd.DataFrame]) -> None:
    """
    Egy tranzakción belül batch-ekben lefuttatja a megadott dataframeket.
    Hiba esetén rollbackel mindent és kivételt dob.
    :return: None
    """
    if not isinstance(data, Iterable):
        data = [data]
    # db_conn.rollback()  # csak a rend kedvéért
    # db_conn.begin()
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


def unitrecipe_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == '1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGYES.k2_vegyes_gb_unitrecipe_mg_tunde_view'
        api = 'kemia1'
    elif api == '2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_unitrecipe_mg_tunde_view'
        api = 'kemia2'
    elif api == '3':
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        unitrecipe_view = 'EFR_24EPFIR.k2_24epfir_gb_unitrecipe_mg_tunde_view'
        api = 'kemia3'
    else:
        raise ValueError

    # unitrecept statisztikák
    # receptszám, recept csoport és unitrecept szám alapján csoportosítva számolunk statisztikákat
    # azokra a unitreceptekre amik a kiválasztott sarzsokban szerepelnek
    query_unitrecipes = '''
select vu.Area as RecipeGrpNum,
       vu.RecipeNum,
       vu.UnitRecipeNum,
       DATEDIFF(minute, vu.Started, vu.Finished) as duration,
       DATEDIFF(minute, vr.Started, vu.Started) as offset,
--        DATEDIFF(minute, c.started, vu.Started) as total_offset_alt,
       rs.offset_median + DATEDIFF(minute, vr.Started, vu.Started) as total_offset
  from charges c
  join run_stat_charges rsc
       on c.charge_nr = rsc.charge_nr
           and c.api = '{api}'
           and format(c.started, 'yyyy-MM-dd') = rsc.charge_start_date
           and rsc.stat_id = {stat_id}
  join {unitrecipe_view} vu
       on vu.ChargeNr = c.charge_nr
           and vu.Started between c.started and c.finished
  join {recipe_view} vr
       on vr.Area = vu.Area
           and vr.RecipeNum = vu.RecipeNum
           and vr.ChargeNr = c.charge_nr
           and vu.Started >= vr.Started
           and vu.Finished <= vr.Finished
  join recipe_run_stats_2 rs
       on rs.stat_id = {stat_id}
           and rs.RecipeGrpNum = vr.Area
           and rs.RecipeNum = vr.RecipeNum
'''
    query_params = {'api': api, 'stat_id': stat_id, 'unitrecipe_view': unitrecipe_view, 'recipe_view': recipe_view}
    sql_query_unitrecipes = query_unitrecipes.format(**query_params)
    unitrecipes_data = run_query(sql_query_unitrecipes)
    unitrecipes_df = pd.DataFrame().from_records(unitrecipes_data)
    unitrecipes_df['total_offset'] = unitrecipes_df['total_offset'].map(lambda x: float(x))
    stats = unitrecipes_df.groupby(['RecipeGrpNum', 'RecipeNum', 'UnitRecipeNum']).agg('describe')
    stats['stat_id'] = stat_id
    stats.columns = [f'{level0}_{level1}' if level1 is not '' else level0
                     for level0, level1 in stats.columns.values]
    stats.drop(labels=['duration_count', 'duration_mean', 'duration_std',
                       'offset_count', 'offset_mean', 'offset_std',
                       'total_offset_count', 'total_offset_mean', 'total_offset_std'],
               axis='columns', inplace=True)
    stats.columns = [column.replace('25%', 'q1').replace('75%', 'q3').replace('50%', 'median')
                     for column in stats.columns]
    stats.name = 'unitrecipe_run_stats_2'

    return stats


def update_stat_state(stat_id: int, state: str) -> None:
    """
    A statisztikákat tartalmazó táblában frissítjük az adott statisztika státuszát.
    """
    query_params = {'stat_id': stat_id, 'state': state}
    upd_query = '''update run_stat set state = '{state}' where id = '{stat_id}' '''.format(**query_params)
    insert(upd_query)


def operation_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == '1':
        operation_view = 'EFR_18VEGYES.k2_vegyes_gb_operation_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGYES.k2_vegyes_gb_unitrecipe_mg_tunde_view'
        api = 'kemia1'
    elif api == '2':
        operation_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_operation_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_unitrecipe_mg_tunde_view'
        api = 'kemia2'
    elif api == '3':
        operation_view = 'EFR_24EPFIR.k2_24epfir_gb_operation_mg_tunde_view'
        unitrecipe_view = 'EFR_24EPFIR.k2_24epfir_gb_unitrecipe_mg_tunde_view'
        api = 'kemia3'
    else:
        raise ValueError

    # művelet statisztikák
    # receptszám és recept csoport alapján csoportosítva számolunk statisztikákat
    # azokra a receptekre amik a kiválasztott sarzsokban szerepelnek
    query_recipes = '''
select vu.RecipeGrpNum,
       vu.RecipeNum,
       vu.UnitRecipeNum,
       vo.OpId,
       trim(vo.opsts) as OpType,
       DATEDIFF(minute, vo.Started, vo.Finished) as duration,
       DATEDIFF(minute, vu.Started, vo.Started) as offset,
--        DATEDIFF(minute, c.started, vu.Started) as total_offset_old,
       rs.offset_median + us.offset_median + DATEDIFF(minute, vu.Started, vo.Started) as total_offset
  from charges c
  join run_stat_charges rsc
       on c.charge_nr = rsc.charge_nr
           and c.api = '{api}'
           and format(c.started, 'yyyy-MM-dd') = rsc.charge_start_date
           and rsc.stat_id = {stat_id}
  join {unitrecipe_view} vu
       on vu.ChargeNr = c.charge_nr
           and vu.Started between c.started and c.finished
  join {operation_view} vo
       on vo.Area = vu.Area
           and vo.RecipeNum = vu.RecipeNum
           and vo.UnitRecipeNum = vu.UnitRecipeNum
           and vo.ChargeNr = c.charge_nr
           and vo.Started >= vu.Started
           and vo.Finished <= vu.Finished
  join recipe_run_stats_2 rs
       on rs.stat_id = {stat_id}
           and rs.RecipeGrpNum = vu.Area
           and rs.RecipeNum = vu.RecipeNum
  join unitrecipe_run_stats_2 us
       on us.stat_id = {stat_id}
           and us.RecipeGrpNum = vu.Area
           and us.RecipeNum = vu.RecipeNum
           and us.UnitRecipeNum = vu.UnitRecipeNum'''
    query_params = {'api': api, 'stat_id': stat_id, 'operation_view': operation_view, 'unitrecipe_view': unitrecipe_view}
    sql_query_recipes = query_recipes.format(**query_params)
    operations_data = run_query(sql_query_recipes)
    operations_df = pd.DataFrame().from_records(operations_data)
    operations_df['total_offset'] = operations_df['total_offset'].map(lambda x: float(x))
    stats = operations_df.groupby(['RecipeGrpNum', 'RecipeNum', 'UnitRecipeNum', 'OpId', 'OpType']).agg('describe')
    stats['stat_id'] = stat_id
    stats.columns = [f'{level0}_{level1}' if level1 is not '' else level0
                     for level0, level1 in stats.columns.values]
    stats.drop(labels=['duration_count', 'duration_mean', 'duration_std',
                       'offset_count', 'offset_mean', 'offset_std',
                       'total_offset_count', 'total_offset_mean', 'total_offset_std'],
               axis='columns', inplace=True)
    stats.columns = [column.replace('25%', 'q1').replace('75%', 'q3').replace('50%', 'median')
                     for column in stats.columns]
    stats.name = 'operation_run_stats_2'

    return stats


def create_new_sensor_stat(api, itemnr, median_iqr, multiplier_iqr, multiplier_std, name):
    # hónapok amikor futott ez az Item
    months = stat_time_chkbxs(itemnr, api)

    # # aktuális hónap
    # current_month = datetime.now().strftime('%Y-%m')
    #
    # # ha az aktuális hónapban futott, akkor csinálunk róla stat-ot
    # if current_month in months:
    #     charges_dates = create_stat_charges(api, itemnr, [current_month])
    #     # charges_dates = [date for date in create_stat_charges(api, itemnr, months) if current_month in date]
    # else:
    #     return

    charges_dates = create_stat_charges(api, itemnr, [months[-1]])

    if api == '1':
        api = 'Vegyes'
    elif api == '2':
        api = 'Vegtermek'
    else:
        api = '24epfir'

    # ellenőrizzük, hogy ilyen statisztika létezik-e már
    sql_stat_id = '''select 1 from ts_stat where stat_name = '{name}' '''.format(**{'name': name})
    exists = run_query(sql_stat_id)
    if exists is not None:
        # ha már létezik, ideiglenes nevet adunk az újnak
        used_name = f'{name}-{str(uuid.uuid4())[:6]}'
    else:
        used_name = name

    # current_date = datetime.now()

    insert_stat_query = '''
insert into ts_stat (stat_name, api, item_nr, state, default_stat, use_median_iqr, deviation_multiplier_iqr, deviation_multiplier_std)
values ('{name}', '{api}', '{item_nr}', '{state}', 0, {median_iqr}, {multiplier_iqr}, {multiplier_std})'''

    state = 'Folyamatban'
    sql_insert_ts_query = insert_stat_query.format(**{'name': used_name, 'api': api,
                                  'item_nr': itemnr, 'state': state,
                                  'median_iqr': median_iqr,
                                  'multiplier_iqr': str(multiplier_iqr).replace(',', '.'),
                                  'multiplier_std': str(multiplier_std).replace(',', '.')})
    insert(sql_insert_ts_query)

    sql_get_id = '''select id from ts_stat where stat_name = '{name}' '''.format(**{'name': used_name})

    stat_id: int = run_query(sql_get_id)[0]['id']

    if itemnr == '70160110' and api == 'Vegtermek':
        charges_dates = [re.search('(\d+/\w+/\d+)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates]
    elif itemnr == '70026110':
        charges_dates_1 = [re.search('(\d+/\w+/\d+)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates if re.search('(\d+/\w+/\d+)\D+(\d{4}-\d{2}-\d{2})', charge_date) is not None]
        charges_dates_2 = [re.search('(\d+/\d+\D*)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates if re.search('(\d+/\d+\D*)\D+(\d{4}-\d{2}-\d{2})', charge_date) is not None]
        charges_dates = charges_dates_1 + charges_dates_2
    else:
        charges_dates = [re.search('(\d+/\d+\D*)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates]

    insert_values = ', '.join([f"({stat_id}, '{charge}', '{date}')" for charge, date in charges_dates])
    insert_stat_charges_query = (
        f'insert into ts_stat_charges (stat_id, charge_nr, charge_start_date) '
        f'values {insert_values}')
    insert(insert_stat_charges_query)

    ts_stat_kemia.operation_stats(stat_id)
    update_ts_state(stat_id, 'Siker')


def insert_df(cursor, df: pd.DataFrame) -> None:
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
    df.rename(columns={'OpId': 'OpInst', 'OpType': 'Op'}, inplace=True)
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


def get_itemnr():
    api_itemnr = []
    for api in [1, 2, 3]:
        if api == 1:
            recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        elif api == 2:
            recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        else:
            recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'

        itemnrs = run_query(f'''
            select distinct rd.item_num
              from {recipe_view} v
              join recipe_details_2 rd
                   on v.Area = rd.recipe_group
                       and v.recipenum = rd.recipe_num''')

        for itemnr in itemnrs:
            api_itemnr.append([str(api), itemnr['item_num'].strip()])

    return api_itemnr


def update_ts_state(stat_id: int, state: str) -> None:
    """
    A statisztikákat tartalmazó táblában frissítjük az adott statisztika státuszát.
    """
    query_params = {'stat_id': stat_id, 'state': state}
    upd_query = '''update ts_stat set state = '{state}' where id = '{stat_id}' '''.format(**query_params)
    insert(upd_query)


def create_all_run_stat_def():
    sql_files = ['operation_run_stat_def.sql', 'recipe_run_stats_def.sql', 'unitrecipe_run_stats_def.sql']
    for file in sql_files:
        with open(file, 'r') as f:
            update_run_stat_def = f.read()
        f.close()
        run_query(update_run_stat_def)


if __name__=='__main__':
    api_itemnr = get_itemnr()

    # run_stat-ot futtatja
    for api, itemnr in api_itemnr:
        create_new_run_stat(api=api, itemnr=itemnr, name='auto')

    # create_all_run_stat_def()

    # ts_stat-ot futtatja
    for api, itemnr in api_itemnr:
        create_new_sensor_stat(api=api, name='auto', itemnr=itemnr, median_iqr=1, multiplier_iqr=1, multiplier_std=1)
