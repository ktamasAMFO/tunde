from typing import List

import pandas as pd
import MySQLdb
import run_stats


def update_stat_state(stat_id: int, state: str) -> None:
    """
    A statisztikákat tartalmazó táblában frissítjük az adott statisztika státuszát.
    """
    query_params = {'stat_id': stat_id, 'state': state}
    upd_query = '''update ts_stat set state = '{state}' where id = '{stat_id}' '''.format(**query_params)
    run_stats.insert(upd_query)


def mdm(series: pd.Series) -> pd.Series:
    """Median of absolute deviations from the median"""
    return (series.median() - series).abs().median()


def q1(series: pd.Series) -> pd.Series:
    return series.quantile(0.25)


def q3(series: pd.Series) -> pd.Series:
    return series.quantile(0.75)


def cols_to_strings(df: pd.DataFrame) -> None:
    for col, dtype in df.dtypes.iteritems():
        if dtype == 'object':
            df[col] = "'" + df[col].astype(str) + "'"
        else:
            df[col] = df[col].astype(str)


def insert_stats(cursor: MySQLdb.cursors.DictCursor, df: pd.DataFrame, conn) -> None:
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


# %%
query_all_sensor_cols = '''
select COLUMN_NAME
from information_schema.columns
where table_name = 'k1_apimppi_p_tunde_view' 
    or table_name = 'k1_apimpp2_p_tunde_view'
'''

query_operations = '''
select recipe_group,
       recipe_num,
       unit_recipe_num,
       op_type,
       op_inst,
       op_comment,
       gantt_visible,
       dash_visible,
       dash_param1,
       dash_param2,
       dash_param3,
       dash_param4,
       stat_param1,
       stat_param2,
       stat_param3,
       stat_param4,
       stat_param5,
       stat_param6
from operation_details od
join ts_stat s
     on s.id = {stat_id}
         and s.api = od.recipe_group'''
# TODO: more filtering?

query_op_units_api12 = '''
select distinct Unit
from {table}
where RecipeGrpNum = {recipe_group}
        and RecipeNum = {recipe_num}
        and UnitRecipeNum = {unit_recipe_num}
        and OpType = '{op_type}'
        and OpId = {op_inst}'''

query_op_min_max_dt = '''
select min(v.Started) as min_start,
   max(v.Finished) as max_end
    from charges c
    join ts_stat_charges sc
         on c.charge_nr = sc.charge_nr
             and FORMAT(c.started, 'yyyy-MM-dd') = sc.charge_start_date
             and sc.stat_id = {stat_id}
    join {table_operation} v
         on 1 = 1
             and v.Started >= c.started
             and v.finished <= c.finished
             and v.ChargeNr = c.charge_nr
             and Unit = '{unit}'
             and RecipeGrpNum = {recipe_group}
             and RecipeNum = {recipe_num}
             and UnitRecipeNum = {unit_recipe_num}
             and OpType = '{op_type}'
             and OpId = {op_inst}
             and DATEDIFF_BIG(second, v.[Started], v.Finished) < 86400'''

query_op_offset_ts = '''
select floor(DATEDIFF_BIG(second,ts.TimeStamp, op.Started) / 60) as offset,
       {columns}
  from (
      select v.Started, v.Finished
        from charges c
        join ts_stat_charges sc
             on c.charge_nr = sc.charge_nr
                 and FORMAT(c.started, 'yyyy-MM-dd') = sc.charge_start_date
                 and sc.stat_id = {stat_id}
        join {table_operation} v
             on 1 = 1
                 and v.Started >= c.started
                 and v.finished <= c.finished
                 and v.ChargeNr = c.charge_nr
                 and Unit = {unit}
                 and RecipeGrpNum = {recipe_group}
                 and RecipeNum = {recipe_num}
                 and UnitRecipeNum = {unit_recipe_num}
                 and OpType = '{op_type}'
                 and OpId = {op_inst}
                 and DATEDIFF_BIG(second, v.[Started], v.Finished) < 86400
  ) op
  join (select * 
          from {table_p}
         where RAMP in (select RAMP
                          from {table_p}
                         where timestamp between '{min_start}' and '{max_end}'
             )
  ) ts
       on ts.TimeStamp between op.Started and op.Finished
'''

query_op_offset_ts_end_calc = '''
join calculated c
     on c.Datum = ts.TimeStamp
        and c.Datum between op.Started and op.Finished
        and c.Datum between {start_date} and {end_date}'''

stat_fns = [pd.DataFrame.median, q1, q3, pd.DataFrame.mean, pd.DataFrame.std, pd.DataFrame.min, pd.DataFrame.max]


def operation_stats(stat_id: int):
    """Végigmegyünk az operation_details táblán egyesével. Minden műveletnél megnézzük, hogy milyen unitokon fut,
    mert attól függ, hogy milyen oszlopok kellenek a szenzoradatos táblákból. """
    conn, cursor = run_stats.connect_to_db()
    # cursor.execute(query_all_sensor_cols)
    # all_sensor_cols = cursor.fetchall()
    all_sensor_cols = run_stats.run_query(query_all_sensor_cols)
    all_sensor_cols = [row['COLUMN_NAME'] for row in all_sensor_cols]
    sql_query_operations = query_operations.format(**{'stat_id': stat_id})
    # cursor.execute(sql_query_operations)
    operations = run_stats.run_query(sql_query_operations)

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
        op_identifier_params.update({'table': 'EFR_S0000034.k1_apimpp1_gb_operation_view_tunde_view'})
        sql_query_op_units_api12 = query_op_units_api12.format(**op_identifier_params)
        if run_stats.run_query(sql_query_op_units_api12):
            # az execute visszatérési értéke hogy hány sor jött, ha több mint 0, akkor api1-es műveletről van szó
            apii2 = 'i'
            api12 = '1'
        else:
            op_identifier_params.update({'table': 'EFR_S0000067.k1_apimpp2_gb_operation_view_tunde_view'})
            sql_query_op_units_api12 = query_op_units_api12.format(**op_identifier_params)
            run_stats.run_query(sql_query_op_units_api12)
            apii2 = '2'
            api12 = '2'
        units_data = run_stats.run_query(sql_query_op_units_api12)
        if units_data is None:
            continue
        units: List[str] = [row['Unit'] for row in units_data if len(row['Unit']) == 3]
        if len(units) == 0:
            print(f"no units for {op_desc}")

        spec_columns = [op['stat_param1'], op['stat_param2'], op['stat_param3'], op['stat_param4'],
                        op['stat_param5'], op['stat_param6'],
                        op['dash_param1'], op['dash_param2'], op['dash_param3'], op['dash_param4']]
        spec_columns = [col for col in spec_columns if col is not None]
        # néhány érték elején van egy felesleges x karakter
        columns = [col if col[0] != 'x' else col[1:] for col in spec_columns]
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
            if api12 == '1':
                table_operation = 'EFR_S0000034.k1_apimpp1_gb_operation_view_tunde_view'
                table_p = 'EFR_S0000034.k1_apimppi_p_tunde_view'
            else:
                table_operation = 'EFR_S0000067.k1_apimpp2_gb_operation_view_tunde_view'
                table_p = 'EFR_S0000067.k1_apimpp2_p_tunde_view'

            query_offset_ts = query_op_offset_ts
            query_min_max_dt = query_op_min_max_dt

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
            min_max_dt_data = run_stats.run_query(sql_query_min_max_dt)

            if min_max_dt_data[0]['min_start'] is not None:
                query_offset_ts_params.update({'min_start': min_max_dt_data[0]['min_start'],
                                               'max_end': min_max_dt_data[0]['max_end'],
                                               'table_p': table_p,
                                               'apii2': apii2,
                                               'columns': ',\n'.join(columns)})

                sql_query_offset_ts = query_offset_ts.format(**query_offset_ts_params)
                ts_data = run_stats.run_query(sql_query_offset_ts)
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
                op_df = op_df.groupby(['offset', 'quantity']).aggregate(stat_fns)
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
                op_df.name = 'timeseries_stats_op'
                insert_stats(cursor=cursor, df=op_df, conn=conn)
            else:
                print(f"no statistics for unit {unit}, {op_desc}")
