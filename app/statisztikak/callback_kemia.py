from typing import List, Optional, Iterable, Union
import pandas as pd

from dash.exceptions import PreventUpdate

import app.common.utils as utils
import pyodbc
from . import sql


def stat_itemnr_options(api: str) -> utils.Options:
    if api not in ('kemia1', 'kemia2', 'kemia3'):
        return []

    if api == 'kemia1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
    elif api == 'kemia2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
    else:
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'

    itemnrs = utils.run_query(f'''
select distinct rd.item_num
  from {recipe_view} v
  join recipe_details_2 rd
       on v.Area = rd.recipe_group
           and v.RecipeNum = rd.recipe_num ''')
    options = [{'label': itemnr['item_num'].strip(), 'value': itemnr['item_num'].strip()} for itemnr in itemnrs]
    return options


def stat_chosen_product(itemnr: Optional[str]) -> str:
    if itemnr is None:
        return 'Válasszon cikkszámot'
    # csak az SAP-s táblából lehet kinézni hogy milyen itemnr-hez (cikkszám) milyen termék tartozik...
    query = f"select distinct ProductName from [EFR_18VEGYES].[k2_vv_gb_sap_view_tunde_view] where ItemNr = '{itemnr.strip()}'"
    product = utils.run_query(query)
    return product[0]['ProductName'] if product is not None else 'Nem található terméknév'


def stat_time_chkbxs(itemnr: str, api: str) -> utils.Options:
    if api == 'kemia1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
    elif api == 'kemia2':
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
    month_rows = utils.run_query(sql_query_months)
    options = [{'label': month_row['month'], 'value': month_row['month']} for month_row in month_rows] if month_rows is not None else []
    return options


def stat_charge_chkbxs(api: Optional[str], itemnr: Optional[str],
                       months: Optional[List[str]], trig_ids: Iterable[str]
                       ) -> utils.Options:
    trig_id, _ = utils.triggered()
    if trig_id in trig_ids:
        return []
    options = create_stat_charges(api, itemnr, months)
    return options


def create_stat_charges(api: Optional[str], itemnr: Optional[str],
                        months: Optional[List[str]]) -> utils.Options:
    if api == 'kemia1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
    elif api == 'kemia2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
    else:
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'

    if months is None or not len(months):
        return []

    months_param_list = ', '.join([f"'{{month_{n}}}'" for n in range(len(months))])
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
    where format(c.started, 'yyyy-MM') in ({months_param_list})
    '''

    query_params = {'item_nr': itemnr, 'api': api, 'recipe_view': recipe_view,
                    'months_param_list': months_param_list}
    sql_charges_query = charges_query.format(**query_params)

    query_params = {f'month_{n}': month for n, month in enumerate(months)}
    sql_charges_query = sql_charges_query.format(**query_params)

    charge_rows = utils.run_query(sql_charges_query)
    options = [{'label': charge_row['charge'], 'value': charge_row['charge']}
               for charge_row in charge_rows] if charge_rows is not None else []
    return options


def stat_sel_charges(api: str, charge_opts: utils.Options, itemnr: str,
                     months: List[str], prev_sel_charges: List[str],
                     api_trig: str, itemnr_trig: str, time_trig: str):
    trig_id, _ = utils.triggered()
    if trig_id in [api_trig, itemnr_trig]:
        # ha az api vagy cikkszám változik, lenullázzuk a kiválasztott sarzsok halmazát
        return []
    if trig_id == time_trig:
        # ha a hónapokat változtatják meg, akkor csak azokat a sarzsokat tartjuk meg a
        # kiválasztottak halmazában, amik az új opciókban is megjelennek
        return filter_sel_charges(api, itemnr, months, prev_sel_charges)
    else:
        return [option['value'] for option in charge_opts] if charge_opts is not None else []


def filter_sel_charges(api: str, itemnr: str, months: List[str], prev_sel_charges: List[str]) -> List[str]:
    charge_opts = create_stat_charges(api, itemnr, months)
    charge_opt_vals = [charge_option['value'] for charge_option in charge_opts]
    return [] if prev_sel_charges is None else [
        prev_sel_charge for prev_sel_charge in prev_sel_charges
        if prev_sel_charge in charge_opt_vals]


def stat_fill_selected_values(data, selected_row, table):
    valid_inputs = data is not None and selected_row is not None
    if not selected_row:
        raise PreventUpdate
    identifier = data[selected_row[0]]['id'] if valid_inputs else ''
    name = data[selected_row[0]]['stat_name'] if valid_inputs else ''
    item_nr = data[selected_row[0]]['item_nr'] if valid_inputs else ''
    item_nr = item_nr.strip()
    query_params = {'item_nr': item_nr}
    query = f"select ProductName from [EFR_18VEGYES].[k2_vv_gb_sap_view_tunde_view] where ItemNr = '{item_nr.strip()}'".format(**query_params)
    product = utils.run_query(query)
    product = product[0]['ProductName'] if product is not None else ''
    api = data[selected_row[0]]['api'] if valid_inputs else ''
    api = api.strip()
    if api == 'Vegyes':
        api = 'kemia1'
    elif api == 'Vegtermek':
        api = 'kemia2'
    elif api == '24epfir':
        api = 'kemia3'
    stat_id = data[selected_row[0]]['id'] if valid_inputs else ''
    query_params = {'stat_id': stat_id, 'table': table}
    query = "select distinct format(charge_start_date, 'yyyy-MM') as month from {table} where stat_id = '{stat_id}'".format(**query_params)
    times = utils.run_query(query)
    times_values = [row['month'] for row in times] if times is not None else []
    times_options = [{'label': time, 'value': time, 'disabled': True} for time in times_values]
    query = "select distinct concat(charge_nr, ' (', FORMAT(charge_start_date, 'yyyy-MM-dd'), ')') as charge from {table} where stat_id = '{stat_id}'".format(**query_params)
    charges = utils.run_query(query)
    charges_values = [row['charge'] for row in charges] if charges is not None else []
    charges_options = [{'label': charge, 'value': charge, 'disabled': True} for charge in charges_values]
    returnvals = [identifier, name, product, api, item_nr, times_options, times_values, charges_options, charges_values]

    if table == 'ts_stat_charges':
        median_iqr = str(data[selected_row[0]]['use_median_iqr']) if valid_inputs else ''
        multiplier_iqr = data[selected_row[0]]['deviation_multiplier_iqr'] if valid_inputs else ''
        multiplier_std = data[selected_row[0]]['deviation_multiplier_std'] if valid_inputs else ''
        returnvals.extend([median_iqr, multiplier_iqr, multiplier_std])
    return returnvals


def insert(query):
    conn, cursor = connect_to_db()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


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
    except:
        pass
    else:
        try:
            for df in data:
                insert_df(cursor, df)
        except:
            db_conn.rollback()
            pass
        else:
            db_conn.commit()
        finally:
            cursor.close()


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


def recipe_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == 'kemia1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
    elif api == 'kemia2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
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
    recipes_data = utils.run_query(sql_query_recipes)
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


def unitrecipe_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == 'kemia1':
        recipe_view = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGYES.k2_vegyes_gb_unitrecipe_mg_tunde_view'
    elif api == 'kemia2':
        recipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_unitrecipe_mg_tunde_view'
    elif api == 'kemia3':
        recipe_view = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        unitrecipe_view = 'EFR_24EPFIR.k2_24epfir_gb_unitrecipe_mg_tunde_view'
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
    unitrecipes_data = utils.run_query(sql_query_unitrecipes)
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


def operation_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == 'kemia1':
        operation_view = 'EFR_18VEGYES.k2_vegyes_gb_operation_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGYES.k2_vegyes_gb_unitrecipe_mg_tunde_view'
    elif api == 'kemia2':
        operation_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_operation_mg_tunde_view'
        unitrecipe_view = 'EFR_18VEGTERMEK.k2_vegtermek_gb_unitrecipe_mg_tunde_view'
    elif api == 'kemia3':
        operation_view = 'EFR_24EPFIR.k2_24epfir_gb_operation_mg_tunde_view'
        unitrecipe_view = 'EFR_24EPFIR.k2_24epfir_gb_unitrecipe_mg_tunde_view'
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
    operations_data = utils.run_query(sql_query_recipes)
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


def update_stat_state(stat_id: int, state: str) -> None:
    """
    A statisztikákat tartalmazó táblában frissítjük az adott statisztika státuszát.
    """
    query_params = {'stat_id': stat_id, 'state': state}
    upd_query = '''update ts_stat set state = '{state}' where id = '{stat_id}' '''.format(**query_params)
    insert(upd_query)


def insert(query):
    conn, cursor = connect_to_db()
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()


def operation_stats(stat_id: int):
    """Végigmegyünk az operation_details táblán egyesével. Minden műveletnél megnézzük, hogy milyen unitokon fut,
    mert attól függ, hogy milyen oszlopok kellenek a szenzoradatos táblákból. """
    conn, cursor = utils.connect_to_db()
    # cursor.execute(query_all_sensor_cols)
    # all_sensor_cols = cursor.fetchall()
    all_sensor_cols = utils.run_query(sql.query_all_sensor_cols)
    all_sensor_cols = [row['COLUMN_NAME'] for row in all_sensor_cols]
    sql_query_operations = sql.query_operations_kemia.format(**{'stat_id': stat_id})
    # cursor.execute(sql_query_operations)
    operations = utils.run_query(sql_query_operations)

    for idx, op in enumerate(operations):
        op_desc = (f"recipe group {op['recipe_group']}, "
                   f"recipe num {op['recipe_num']}, "
                   f"unitrecipe num {op['unit_recipe_num']}, "
                   f"op type {op['op_type']}, "
                   f"op inst {op['op_inst']}")
        update_stat_state(stat_id, f'Folyamatban: {(idx / len(operations) * 100) // 1}%')
        op_identifier_params = {'recipe_group': op['recipe_group'],
                                'recipe_num': op['recipe_num'],
                                'unit_recipe_num': op['unit_recipe_num'],
                                'op_type': op['op_type'],
                                'op_inst': op['op_inst']}
        # vannak olyan műveletek, amikhez a k1_apimpp_1_gb_operation táblában találni unitokat,
        # de mégsem a k1_apimppi_p táblában vannak hozzá a szenzoradatok...
        # ilyen elméletileg csak lényegtelen esetben lehet, ha előfordul ilyen, szólni kell Matolcsy Gézának
        op_identifier_params.update({'table': 'EFR_18VEGYES.k2_vegyes_gb_operation_mg_tunde_view'})
        sql_query_op_units_api12 = sql.query_op_units_api12.format(**op_identifier_params)
        if utils.run_query(sql_query_op_units_api12):
            # az execute visszatérési értéke hogy hány sor jött, ha több mint 0, akkor api1-es műveletről van szó
            apii2 = 'kemia1'
            api12 = 'Vegyes'
        else:
            op_identifier_params.update({'table': 'EFR_18VEGTERMEK.k2_vegtermek_gb_operation_mg_tunde_view'})
            sql_query_op_units_api12 = sql.query_op_units_api12.format(**op_identifier_params)
            if utils.run_query(sql_query_op_units_api12):
                # az execute visszatérési értéke hogy hány sor jött, ha több mint 0, akkor api1-es műveletről van szó
                apii2 = 'kemia2'
                api12 = 'Vegtermek'
            else:
                op_identifier_params.update({'table': 'EFR_24EPFIR.k2_24epfir_gb_operation_mg_tunde_view'})
                sql_query_op_units_api12 = sql.query_op_units_api12.format(**op_identifier_params)
                utils.run_query(sql_query_op_units_api12)
                apii2 = 'kemia3'
                api12 = '24epfir'

        units_data = utils.run_query(sql_query_op_units_api12)
        if units_data is None:
            continue
        units: List[str] = [row['Unit'] for row in units_data if row['Unit'] is not 'NULL']
        if len(units) == 0:
            print(f"no units for {op_desc}")

        spec_columns = [op['stat_param1'], op['stat_param2'], op['stat_param3'], op['stat_param4'],
                        op['stat_param5'], op['stat_param6'],
                        op['dash_param1'], op['dash_param2'], op['dash_param3'], op['dash_param4']]
        spec_columns = [col for col in spec_columns if col is not None]
        # néhány érték elején van egy felesleges x karakter
        columns = []
        for col in spec_columns:
            if col[0] != 'x':
                pass
            else:
                col = col[1:]
            all_col = [a for a in all_sensor_cols if col in a]
            spec_col = [a for a in all_col if 'ActValue' in a or 'PresentValue' in a][0]
            columns.append(spec_col)

        columns = list(set(columns).intersection(set(all_sensor_cols)))
        missing_cols = list(set(columns).difference(set(all_sensor_cols)))
        if missing_cols:
            print(f'A ({op_desc}) művelethez megadott ({missing_cols}) értékek nem találhatók meg')

        if not columns:
            if spec_columns:
                print(f'A ({op_desc}) művelethez '
                                   f'az operation_details táblában megadott '
                                   f'szenzoradatok közül egyik sem található meg ({spec_columns})')
            continue

        for unit in units:
            if api12 == 'Vegyes':
                table_operation = 'EFR_18VEGYES.k2_vegyes_gb_operation_mg_tunde_view'
                table_p = 'EFR_S0000058.k2_vegyes_p_tunde_view'
            elif api12 == 'Vegtermek':
                table_operation = 'EFR_18VEGTERMEK.k2_vegtermek_gb_operation_mg_tunde_view'
                table_p = 'EFR_S0000057.k2_vegtermek_p_tunde_view'
            else:
                table_operation = 'EFR_24EPFIR.k2_24epfir_gb_operation_mg_tunde_view'
                table_p = 'EFR_24EPFIR.k2_24epfir_p_tunde_view'

            query_offset_ts = sql.query_op_offset_ts
            query_offset_ts_24 = sql.query_op_offset_ts_24
            query_min_max_dt = sql.query_op_min_max_dt

            query_offset_ts_params = {'unit': unit,
                                      'recipe_group': op['recipe_group'],
                                      'recipe_num': op['recipe_num'],
                                      'unit_recipe_num': op['unit_recipe_num'],
                                      'op_type': op['op_type'],
                                      'op_inst': op['op_inst'],
                                      'stat_id': stat_id,
                                      'table_operation': table_operation,
                                      'api12': api12}

            # a szenzoradatokat tartalmazó táblát szűkítjük a legkorábbi
            # és legkésőbbi szóba jöhető időpontokkal
            sql_query_min_max_dt = query_min_max_dt.format(**query_offset_ts_params)
            min_max_dt_data = utils.run_query(sql_query_min_max_dt)

            if min_max_dt_data[0]['min_start'] is not None:
                query_offset_ts_params.update({'min_start': min_max_dt_data[0]['min_start'],
                                               'max_end': min_max_dt_data[0]['max_end'],
                                               'table_p': table_p,
                                               'apii2': apii2,
                                               'columns': ',\n'.join(columns)})

                if api12 == '24epfir':
                    sql_query_offset_ts = query_offset_ts_24.format(**query_offset_ts_params)
                else:
                    sql_query_offset_ts = query_offset_ts.format(**query_offset_ts_params)
                ts_data = utils.run_query(sql_query_offset_ts)
                op_df = pd.DataFrame.from_records(ts_data)
                op_df = op_df.dropna()
            else:
                ts_data = tuple()
                op_df = pd.DataFrame.from_records(ts_data)
                op_df = op_df.dropna()

            if not op_df.empty:
                # áttranszformáljuk a kapott adattáblát Entity–attribute–value struktúrába (entity:offset)
                op_df = op_df.set_index('offset').stack().reset_index().rename({'level_1': 'quantity', 0: 'value'}, axis=1)
                # elvégezzük a statisztikákat
                op_df = op_df.groupby(['offset', 'quantity']).aggregate(sql.stat_fns)
                # megfelelő formára hozzuk a dataframe-et
                op_df = pd.DataFrame(op_df.to_records())
                op_df.columns = ['offset', 'quantity', 'MD', 'Q1', 'Q3', 'MN', 'SD', 'MIN', 'MAX']
                op_df['recipe_group'] = op['recipe_group']
                op_df['recipe_no'] = op['recipe_num']
                op_df['unit_recipe_no'] = op['unit_recipe_num']
                op_df['op_type'] = op['op_type']
                op_df['op_inst'] = op['op_inst']
                op_df['unit'] = unit
                op_df_str_cols = op_df.select_dtypes(include='object')
                op_df.loc[:, op_df_str_cols.columns] = op_df_str_cols.apply(lambda x: "'" + x + "'", axis=1)
                op_df['stat_id'] = stat_id
                op_df = op_df.astype(str)
                op_df.name = 'timeseries_stats_op_2'
                insert_stats(cursor=cursor, df=op_df, conn=conn)
            else:
                print(f"no statistics for unit {unit}, {op_desc}")


def insert_stats(cursor, df: pd.DataFrame, conn) -> None:
    insert = f'insert into {df.name} ' + '(' + ', '.join(df.columns) + ')' + ' values '
    insert_rows_values = '(' + df.apply(', '.join, axis=1) + ')'
    step = 1000
    for i in range(0, len(insert_rows_values), step):
        insert_vals = insert_rows_values[i:i + step].str.cat(sep=', ')
        insert_vals = insert_vals.replace("'NULL'", "NULL")
        # std számoláskor előfordulhat, hogy csak 1 elem miatt 0 szabadsági fokkal számol, innen a nan értékek
        insert_vals = insert_vals.replace("nan", "0")
        cursor.execute(insert + insert_vals)
        conn.commit()
