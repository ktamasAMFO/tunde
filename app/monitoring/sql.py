query_details_for_operation = '''
select recipe_comment,
       unit_recipe_comment,
       op_comment,
       item_num
from operation_details od
join unit_recipe_details ud
     on ud.recipe_group = od.recipe_group
         and ud.recipe_num = od.recipe_num
         and ud.unit_recipe_num = od.unit_recipe_num
join recipe_details rd
     on rd.recipe_group = od.recipe_group
         and rd.recipe_num = od.recipe_num
where od.recipe_num = {recipe_num}
  and od.unit_recipe_num = {unit_recipe_num}
  and op_inst = {op_inst}
  and op_type = '{op_type}'
  '''


query_details_for_operation_kemia = '''
select recipe_comment,
       unit_recipe_comment,
       op_comment,
       item_num
from operation_details_2 od
join unit_recipe_details_2 ud
     on ud.recipe_group = od.recipe_group
         and ud.recipe_num = od.recipe_num
         and ud.unit_recipe_num = od.unit_recipe_num
join recipe_details_2 rd
     on rd.recipe_group = od.recipe_group
         and rd.recipe_num = od.recipe_num
where od.recipe_num = '{recipe_num}'
  and od.unit_recipe_num = '{unit_recipe_num}'
  and op_type = '{op_inst}'
  and op_inst = '{op_type}'
  '''

query_charge_running_unit_recipe = '''
select ChargeNr
from unitrecipes_v
where RecipeNum = {recipe_num}
  and UnitRecipeNum = {unit_recipe_num}
  and FinishType = 'Running'
  and GETDATE() > Started
  and Finished is null'''

query_charge_running_unit_recipe_kemia = '''
select ChargeId as ChargeNr
from {table}
where RecipeNr = '{recipe_num}'
  and UnitRecipeNr = '{unit_recipe_num}'
  and GETDATE() > RecipeStartTime
  and State = 'Running'
  '''

query_duration =  '''select {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, '{start}')}} as median_end 
                    from operation_run_stats 
                    where RecipeNum = {recipe_num} 
                      and UnitRecipeNum = {unit_recipe_num} 
                      and Op = '{op_type}'
                      and OpInst = {op_inst} 
                      and stat_id = (select id 
                                     from run_stat 
                                     where default_stat = 1 
                                       and item_nr = {item_nr}) '''

query_duration_kemia =  '''select {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, '{start}')}} as median_end 
                    from operation_run_stats_2 
                    where RecipeNum = '{recipe_num}' 
                      and UnitRecipeNum = '{unit_recipe_num}' 
                      and Op = '{op_type}'
                      and OpInst = '{op_inst}' 
                      and stat_id = (select id 
                                     from run_stat 
                                     where default_stat = 1 
                                       and item_nr = {item_nr}) '''

query_from_records = """
        SELECT * FROM {table}
        WHERE TimeStamp = (SELECT MAX(TimeStamp) FROM {table})
        """

query_from_records_kemia = """
        SELECT * FROM {table}
        WHERE TimeStamp = (SELECT MAX(TimeStamp) FROM {table}) and TimeStamp > {{fn TIMESTAMPADD(SQL_TSI_MINUTE, -60, GETDATE())}}
        """

query_dash_visible = '''select dash_visible 
            from operation_details 
            where dash_visible = 1 
               and recipe_group = {api} 
               and recipe_num = {recipe_num} 
               and unit_recipe_num = {unit_recipe_num} 
               and op_type = '{op_type}' 
               and op_inst = {op_inst} '''

query_dash_visible_kemia = '''select dash_visible 
            from operation_details_2 
            where dash_visible = 1
               and recipe_num = '{recipe_num}' 
               and unit_recipe_num = '{unit_recipe_num}' 
               and op_type = '{op_type}' 
               and op_inst = '{op_inst}' '''

query_span_query = """select TimeStamp, 
               U{unit}_BSTS 
        from {source} 
        where U{unit}_BATCHID = '{BATCHID}' 
            AND U{unit}_RECIPE = '{RECIPE}' 
            AND U{unit}_PARTNO = '{PARTNO}' 
            AND U{unit}_OPNAME = '{OPNAME}' 
            AND U{unit}_OPNR = {OPNR} 
            AND TimeStamp > {{fn TIMESTAMPADD(SQL_TSI_DAY, -2, '{datetime}')}}"""

query_span_query_kemia = """select TimeStamp, 
               State 
        from {source} 
        where BatchID = '{BATCHID}' 
            AND UnitRecipeNr = '{PARTNO}' 
            AND OperationName = '{OPNAME}' 
            AND OperationType = '{OPNR}' 
            AND TimeStamp > {{fn TIMESTAMPADD(SQL_TSI_DAY, -2, '{datetime}')}}"""

query_four_stats_query = """
        select dash_param1, 
               dash_param2, 
               dash_param3, 
               dash_param4 
        from (select rd.recipe_num, 
                     unit_recipe_num, 
                     op_type, 
                     op_inst, 
                     dash_visible, 
                     dash_param1, 
                     dash_param2, 
                     dash_param3, 
                     dash_param4 
              from operation_details od 
              join recipe_details rd 
                on rd.recipe_group = od.recipe_group 
               and rd.recipe_num = od.recipe_num) op 
        where op.recipe_num = {MR} 
          and op.unit_recipe_num = {PARTNO} 
          and op.op_type = '{OPNAME}' 
          and op.op_inst = {OPNR} 
          and op.dash_visible = 1 """

query_four_stats_query_kemia = """
        select dash_param1, 
               dash_param2, 
               dash_param3, 
               dash_param4 
        from (select rd.recipe_num, 
                     unit_recipe_num, 
                     op_type, 
                     op_inst,
                     op_comment, 
                     dash_visible, 
                     dash_param1, 
                     dash_param2, 
                     dash_param3, 
                     dash_param4 
              from operation_details_2 od 
              join recipe_details_2 rd 
                on rd.recipe_group = od.recipe_group 
               and rd.recipe_num = od.recipe_num) op 
        where op.recipe_num = '{MR}' 
          and op.unit_recipe_num = '{PARTNO}' 
          and op.op_comment = '{OPNAME}' 
          and op.op_type = '{OPNR}' 
          and op.dash_visible = 1 """

query_available_columns = """select COLUMN_NAME 
        from information_schema.COLUMNS 
        where TABLE_NAME = '{source}' 
          and COLUMN_NAME like '%{unit}%' 
        order by ORDINAL_POSITION """

query_sensor_query = """select TimeStamp {stats} 
                    from {source}"""

query_sensor_query_24epfir = """select igyarto_TimeStamp {stats} 
                    from {source}"""

query_join_query = """select l.TimeStamp, 
                         U{unit}_BSTS 
                         {stats} 
                  from ({span_query}) l 
                  join ({sensor_query}) r 
                     on l.TimeStamp = r.TimeStamp"""

query_join_query_kemia = """select l.TimeStamp, 
                         State 
                         {stats} 
                  from ({span_query}) l 
                  join ({sensor_query}) r 
                     on l.TimeStamp = r.TimeStamp"""

query_join_query_kemia_24epfir = """select l.TimeStamp, 
                         State 
                         {stats} 
                  from ({span_query}) l 
                  join ({sensor_query}) r 
                     on l.TimeStamp = r.igyarto_TimeStamp"""

query_itemnr_query = '''select ItemNr 
        from EFR_APIbatch.k1_apimpp_gb_operation_tunde_view o 
        join EFR_APIbatch.k1_apimpp_gb_sap_tunde_view s 
          on s.OrderID = o.OrderID 
        where 1 = 1 
          and o.RecipeNum =  {recipe_no}
          and o.UnitRecipeNum = {unit_recipe_no} 
          and o.OpType = '{op_type}' 
          and o.OpId = {op_inst} 
          and o.Unit = '{unit}'
        order by o.Started desc '''

query_itemnr_query_kemia = '''select ItemNr 
        from {table_item} o 
        where 1 = 1 
          and o.RecipeNum =  '{recipe_no}'
          and o.UnitRecipeNum = '{unit_recipe_no}'
          and o.opid = '{op_type}' 
          and o.opsts = '{op_inst}'
          and o.Unit = '{unit}'
        order by o.Started desc '''

query_ts_query = """select offset as Offset, 
                   case when s.use_median_iqr = 1 then MD else MN end as '{stat_unit}MID', 
                   case when s.use_median_iqr = 1 then MD - (MD - Q1) * deviation_multiplier_iqr 
                        else MN - SD * deviation_multiplier_std end as '{stat_unit} low', 
                   case when s.use_median_iqr = 1 then MD - (MD - Q3) * deviation_multiplier_iqr 
                        else MN + SD * deviation_multiplier_std end as '{stat_unit} up', 
                   MIN as '{stat_unit}MIN', 
                   MAX as '{stat_unit}MAX'
            from timeseries_stats_op ts 
            join ts_stat s 
                  on s.id = ts.stat_id 
            where recipe_no = {recipe_no}
              and unit_recipe_no = {unit_recipe_no}
              and op_type = '{op_type}'
              and op_inst = {op_inst}
              and unit = {unit}
              and quantity = '{stat_unit}'
              and stat_id = (select s.id 
                             from ts_stat s 
                             where item_nr = {item_nr}
                               and default_stat = 1 
                               and state = 'SIKER')"""

query_ts_query_kemia = """select offset as Offset, 
                   case when s.use_median_iqr = 1 then MD else MN end as '{stat_unit}MID', 
                   case when s.use_median_iqr = 1 then MD - (MD - Q1) * deviation_multiplier_iqr 
                        else MN - SD * deviation_multiplier_std end as '{stat_unit} low', 
                   case when s.use_median_iqr = 1 then MD - (MD - Q3) * deviation_multiplier_iqr 
                        else MN + SD * deviation_multiplier_std end as '{stat_unit} up', 
                   MIN as '{stat_unit}MIN', 
                   MAX as '{stat_unit}MAX'
            from timeseries_stats_op_2 ts 
            join ts_stat s 
                  on s.id = ts.stat_id 
            where recipe_no = '{recipe_no}'
              and unit_recipe_no = '{unit_recipe_no}'
              and op_type = '{op_type}'
              and op_inst = '{op_inst}'
              and unit = '{unit}'
              and quantity = '{stat_unit}'
              and stat_id = (select s.id 
                             from ts_stat s 
                             where item_nr = {item_nr}
                               and default_stat = 1 
                               and state = 'SIKER')"""