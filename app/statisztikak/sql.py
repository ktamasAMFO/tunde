import pandas as pd

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

stat_fns = [pd.DataFrame.median, q1, q3, pd.DataFrame.mean, pd.DataFrame.std, pd.DataFrame.min, pd.DataFrame.max]


query_all_sensor_cols = '''
select COLUMN_NAME
from information_schema.columns
where table_name = 'k2_vegyes_p_tunde_view' 
    or table_name = 'k2_vegtermek_p_tunde_view'
    or table_name = 'k2_24epfir_p_tunde_view'
'''

query_all_sensor_cols_api = '''
select COLUMN_NAME
from information_schema.columns
where table_name = 'k1_apimpp2_p_tunde_view' 
    or table_name = 'k1_apimppi_p_tunde_view'
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


query_operations_kemia = '''
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
from operation_details_2 od
join ts_stat s
     on s.id = {stat_id}
         and s.api = od.recipe_group'''

query_op_units_api12 = '''
select distinct Unit
from {table}
where Area = '{recipe_group}'
        and recipenum = '{recipe_num}'
        and UnitRecipeNum = '{unit_recipe_num}'
        and opsts = '{op_type}'
        and opid = '{op_inst}'
        '''

query_op_units_api = '''
select distinct Unit
from {table}
where RecipeGrpNum = '{recipe_group}'
        and recipenum = '{recipe_num}'
        and UnitRecipeNum = '{unit_recipe_num}'
        and op = '{op_type}'
        and OpInst = {op_inst}
        '''

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
             and Area = '{recipe_group}'
             and recipenum = '{recipe_num}'
             and UnitRecipeNum = '{unit_recipe_num}'
             and opsts = '{op_type}'
             and opid = '{op_inst}'
             and DATEDIFF_BIG(second, v.[Started], v.Finished) < 86400'''

query_op_min_max_dt_api = '''
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
             and RecipeGrpNum = '{recipe_group}'
             and recipenum = '{recipe_num}'
             and UnitRecipeNum = '{unit_recipe_num}'
             and op = '{op_type}'
             and OpInst = {op_inst}
             and DATEDIFF_BIG(second, v.[Started], v.Finished) < 86400'''


query_op_offset_ts_api = '''
select floor(DATEDIFF_BIG(second, op.Started, ts.TimeStamp) / 60) as offset,
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
                 and Unit = '{unit}'
                 and RecipeGrpNum = '{recipe_group}'
                 and recipenum = '{recipe_num}'
                 and UnitRecipeNum = '{unit_recipe_num}'
                 and op = '{op_type}'
                 and OpInst = {op_inst}
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


query_op_offset_ts = '''
select floor(DATEDIFF_BIG(second, op.Started, ts.TimeStamp) / 60) as offset,
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
                 and Unit = '{unit}'
                 and Area = '{recipe_group}'
                 and recipenum = '{recipe_num}'
                 and UnitRecipeNum = '{unit_recipe_num}'
                 and opsts = '{op_type}'
                 and opid = '{op_inst}'
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

query_op_offset_ts_24 = '''
select floor(DATEDIFF_BIG(second, op.Started, ts.igyarto_TimeStamp) / 60) as offset,
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
                 and Unit = '{unit}'
                 and Area = '{recipe_group}'
                 and recipenum = '{recipe_num}'
                 and UnitRecipeNum = '{unit_recipe_num}'
                 and opsts = '{op_type}'
                 and opid = '{op_inst}'
                 and DATEDIFF_BIG(second, v.[Started], v.Finished) < 86400
  ) op
  join (select * 
          from {table_p}
         where vgyarto_RAMP in (select vgyarto_RAMP
                          from {table_p}
                         where igyarto_TimeStamp between '{min_start}' and '{max_end}'
             )
  ) ts
       on ts.igyarto_TimeStamp between op.Started and op.Finished
'''
