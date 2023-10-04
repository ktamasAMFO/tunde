query_usual_duration = '''
select avg(DATEDIFF(minute, started, finished)) as usual_duration
from tunde_dev.charges
where api = '{api}'
'''

query_usual_duration_kemia = '''
select avg(DATEDIFF(minute, started, finished)) as usual_duration
from tunde_dev.charges
where api = '{api}'
and finished != '0001-01-01 00:00:00.0000000'
'''

query_max_ts = '''
select  max(TimeStamp) as max_ts
from EFR_S0000034.k1_apimppi_p_tunde_view
'''


query_max_ts_kemia = '''
select  max(TimeStamp) as max_ts
from EFR_S0000058.k2_vegyes_p_tunde_view
'''


query_running_charge_runs = '''
select charge_nr as 'Sarzs',
       c.started as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, {usual_duration}, c.started)}} as Finish, 
       'rgb(60, 180, 75)' as 'color'
from charges c
where c.Finished is null
    and api = '{api}'
    and c.started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -10, '{max_ts}')}}

union

select charge_nr as 'Sarzs',
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, {usual_duration}, c.started)}} as Start,
       '{max_ts}' as Finish,
       'rgb(230, 25, 75)' as 'color'
from charges c
where c.Finished is null
    and api = '{api}'
    and c.started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -10, '{max_ts}')}}
    and '{max_ts}' > {{fn TIMESTAMPADD(SQL_TSI_MINUTE, {usual_duration}, c.started)}}
    
union

select charge_nr as 'Sarzs',
       c.started as Start,
       finished as Finish,
       'rgb(60, 180, 75)' as 'color'
from charges c 
where c.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
    and api = '{api}'
'''

query_running_charge_runs_kemia = '''
select charge_nr as 'Sarzs',
       c.started as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, {usual_duration}, c.started)}} as Finish, 
       'rgb(60, 180, 75)' as 'color'
from charges c
where c.Finished is null
    and api = '{api}'
    and c.started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -10, '{max_ts}')}}

union

select charge_nr as 'Sarzs',
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, {usual_duration}, c.started)}} as Start,
       '{max_ts}' as Finish,
       'rgb(230, 25, 75)' as 'color'
from charges c
where c.Finished is null
    and api = '{api}'
    and c.started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -10, '{max_ts}')}}
    and '{max_ts}' > {{fn TIMESTAMPADD(SQL_TSI_MINUTE, {usual_duration}, c.started)}}
    
union

select charge_nr as 'Sarzs',
       c.started as Start,
       finished as Finish,
       'rgb(60, 180, 75)' as 'color'
from charges c 
where c.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}}
    and api = '{api}'
'''

query_all_running_recipes = '''
-- kék
select b.Finished as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} as Finish, 
       rd.recipe_comment as 'Recept komment',
       ChargeNr, 
       concat(ChargeNr, ' / ', rd.recipe_comment) as 'sarzs-recept',
       s.RecipeNum,
       'rgb(70, 240, 240)' as 'color'
from {tabla} b
join charges c
    on b.ChargeNr = c.charge_nr
         and b.Started >= c.started
         and coalesce(b.Finished, '1990-01-01') <= coalesce(c.finished, {{fn TIMESTAMPADD(SQL_TSI_DAY, {maxdays}, c.started)}}) 
left join recipe_run_stats_def s
    on s.RecipeNum = b.RecipeNum
         and s.RecipeGrpNum = b.RecipeGrpNum
join recipe_details rd
    on b.RecipeNum = rd.recipe_num
    and b.RecipeGrpNum = rd.recipe_group
join run_stat rs
    on rs.id = s.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where b.Finished is not null
  and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} > b.Finished
  and ChargeNr in (select distinct ChargeNr
                    from {tabla} v
                    join recipe_details rd
                        on rd.recipe_group = v.RecipeGrpNum
                     and rd.recipe_num = v.RecipeNum
                    where v.Finished is null or v.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
                        and LOWER(LEFT(ChargeNr, 1)) <> 't'
                        and v.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}})
  and rd.item_num in (select distinct rd.item_num
                    from {tabla} v
                    join recipe_details rd
                        on rd.recipe_group = v.RecipeGrpNum
                     and rd.recipe_num = v.RecipeNum
                    where v.Finished is null or v.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
                        and LOWER(LEFT(ChargeNr, 1)) <> 't'
                        and v.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}})
    and b.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}}

union

-- piros
select {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} as Start,
       coalesce(b.Finished, '{max_ts}') as Finish,
       rd.recipe_comment as 'Recept komment',
       ChargeNr, 
       concat(ChargeNr, ' / ', rd.recipe_comment) as 'sarzs-recept',
       s.RecipeNum,
       'rgb(230, 25, 75)' as 'color'
from {tabla} b
join charges c
    on b.ChargeNr = c.charge_nr
         and b.Started >= c.started
         and coalesce(b.Finished, '1990-01-01') <= coalesce(c.finished, {{fn TIMESTAMPADD(SQL_TSI_DAY, {maxdays}, c.started)}})
left join recipe_run_stats_def s
    on s.RecipeNum = b.RecipeNum
         and s.RecipeGrpNum = b.RecipeGrpNum
join recipe_details rd
    on b.RecipeNum = rd.recipe_num
    and b.RecipeGrpNum = rd.recipe_group
join run_stat rs
    on rs.id = s.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where ((b.Finished is not null and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} < b.Finished)
   or (b.Finished is null and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} < '{max_ts}'))
  and ChargeNr in (select distinct ChargeNr
                    from {tabla} v
                    join recipe_details rd
                        on rd.recipe_group = v.RecipeGrpNum
                     and rd.recipe_num = v.RecipeNum
                    where v.Finished is null or v.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
                        and LOWER(LEFT(ChargeNr, 1)) <> 't'
                        and v.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}})
  and rd.item_num in (select distinct rd.item_num
                    from {tabla} v
                    join recipe_details rd
                        on rd.recipe_group = v.RecipeGrpNum
                     and rd.recipe_num = v.RecipeNum
                    where v.Finished is null or v.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
                        and LOWER(LEFT(ChargeNr, 1)) <> 't'
                        and v.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}})
    and b.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}}

union

-- zöld
SELECT b.Started as Start,
case when b.Finished is null and '{max_ts}' < {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} then {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}} 
                else least({{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, b.Started)}}, coalesce(b.Finished, '{max_ts}'))
end as Finish,
rd.recipe_comment as 'Recept komment',
ChargeNr, 
concat(ChargeNr, ' / ', rd.recipe_comment) as 'sarzs-recept',
s.RecipeNum,
'rgb(60, 180, 75)' as 'color'
from {tabla} b
join charges c
    on b.ChargeNr = c.charge_nr
         and b.Started >= c.started
         and coalesce(b.Finished, '1990-01-01') <= coalesce(c.finished, {{fn TIMESTAMPADD(SQL_TSI_DAY, {maxdays}, c.started)}})
left join recipe_run_stats_def s
    on s.RecipeNum = b.RecipeNum
         and s.RecipeGrpNum = b.RecipeGrpNum
join recipe_details rd
    on b.RecipeNum = rd.recipe_num
    and b.RecipeGrpNum = rd.recipe_group
join run_stat rs
    on rs.id = s.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where 1=1
  and ChargeNr in (select distinct ChargeNr
                    from {tabla} v
                    join recipe_details rd
                        on rd.recipe_group = v.RecipeGrpNum
                     and rd.recipe_num = v.RecipeNum
                    where v.Finished is null or v.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
                        and LOWER(LEFT(ChargeNr, 1)) <> 't'
                        and v.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}})
  and rd.item_num in (select distinct rd.item_num
                    from {tabla} v
                    join recipe_details rd
                        on rd.recipe_group = v.RecipeGrpNum
                     and rd.recipe_num = v.RecipeNum
                    where v.Finished is null or v.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -1, '{max_ts}')}}
                        and LOWER(LEFT(ChargeNr, 1)) <> 't'
                        and v.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}})
    and b.Started > {{fn TIMESTAMPADD(SQL_TSI_DAY, -14, GETDATE())}}
'''

query_all_running_recipes_kemia = '''
SELECT DISTINCT b.RecipeNr as RecipeNum,
                b.RecipeStartTime AS Start,
                b.RecipeNr as 'Recept komment',
                case when '{max_ts}' < {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} then {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}}
                else {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}}
                end as Finish,
                b.ChargeID as ChargeNr, 
                CONCAT(b.ChargeID, ' / ', b.RecipeNr) AS 'sarzs-recept',
                'rgb(60, 180, 75)' AS 'color'
FROM {tabla2} b
left join recipe_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE g.recipenum IS NULL or g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}}

union

SELECT DISTINCT b.RecipeNr as RecipeNum,
                {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} as Start,
                b.RecipeNr as 'Recept komment',
                coalesce(g.Finished, '{max_ts}') as Finish,
                b.ChargeID as ChargeNr, 
                CONCAT(b.ChargeID, ' / ', b.RecipeNr) AS 'sarzs-recept',
                'rgb(230, 25, 75)' as 'color'
FROM {tabla2} b
left join recipe_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} < '{max_ts}')
or (g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}} and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} < g.Finished) 

union

select DISTINCT s.RecipeNum,
       g.Finished as Start,
       b.RecipeNr as 'Recept komment',
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} as Finish, 
       b.ChargeID as ChargeNr, 
       CONCAT(b.ChargeID, ' / ', b.RecipeNr) AS 'sarzs-recept',
       'rgb(70, 240, 240)' as 'color'
FROM {tabla2} b
left join recipe_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE g.recipenum IS NULL or (g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}} and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} > g.Finished)
'''

query_running_unit_recipes_in_charge = '''
-- kék
select Finished as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} as Finish,
       rd.recipe_comment as 'Recept komment',
       ud.unit_recipe_comment as 'Unit recept komment',
       Unit,
       BatchID,
       ChargeNr, 
       u.RecipeNum, 
       u.UnitRecipeNum,
       'rgb(70, 240, 240)' as 'color'
from unitrecipes_v u
join recipe_details rd
        on rd.recipe_group = u.RecipeGrpNum
             and rd.recipe_num = u.RecipeNum
join unit_recipe_details ud
        on ud.recipe_group = u.RecipeGrpNum
             and ud.recipe_num = u.RecipeNum
             and ud.unit_recipe_num = u.UnitRecipeNum
             and ud.gantt_visible = 1
left join unitrecipe_run_stats_def stat
         on u.RecipeNum = stat.RecipeNum
             and u.UnitRecipeNum = stat.UnitRecipeNum
             and u.RecipeGrpNum = stat.RecipeGrpNum
join run_stat rs
    on rs.id = stat.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where Finished is not null
        and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} > Finished
        and ChargeNr = '{charge_no}'
        and u.RecipeNum = {recipe_no}

union

-- piros
select {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} as Start,
       coalesce(Finished, '{max_ts}') as Finish,
       rd.recipe_comment as 'Recept komment',
       ud.unit_recipe_comment as 'Unit recept komment',
       Unit,
       BatchID,
       ChargeNr, 
       u.RecipeNum, 
       u.UnitRecipeNum,
       'rgb(230, 25, 75)' as 'color'
from unitrecipes_v u
join recipe_details rd
        on rd.recipe_group = u.RecipeGrpNum
             and rd.recipe_num = u.RecipeNum
join unit_recipe_details ud
        on ud.recipe_group = u.RecipeGrpNum
             and ud.recipe_num = u.RecipeNum
             and ud.unit_recipe_num = u.UnitRecipeNum
             and ud.gantt_visible = 1
left join unitrecipe_run_stats_def stat
         on u.RecipeNum = stat.RecipeNum
             and u.UnitRecipeNum = stat.UnitRecipeNum
             and u.RecipeGrpNum = stat.RecipeGrpNum
join run_stat rs
    on rs.id = stat.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where ((Finished is not null and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} < Finished)
   or (Finished is null and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} < '{max_ts}'))
        and ChargeNr = '{charge_no}'
        and u.RecipeNum = {recipe_no}

union

-- zöld
select Started as Start,
       case when Finished is null and '{max_ts}' < {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} then {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}} 
                else least({{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration, Started)}}, coalesce(Finished, '{max_ts}'))
       end as Finish,
       rd.recipe_comment as 'Recept komment',
       ud.unit_recipe_comment as 'Unit recept komment',
       Unit,
       BatchID,
       ChargeNr, 
       u.RecipeNum, 
       u.UnitRecipeNum,
       'rgb(60, 180, 75)' as 'color'
from unitrecipes_v u
join recipe_details rd
        on rd.recipe_group = u.RecipeGrpNum
             and rd.recipe_num = u.RecipeNum
join unit_recipe_details ud
        on ud.recipe_group = u.RecipeGrpNum
             and ud.recipe_num = u.RecipeNum
             and ud.unit_recipe_num = u.UnitRecipeNum
             and ud.gantt_visible = 1
left join unitrecipe_run_stats_def stat
         on u.RecipeNum = stat.RecipeNum
             and u.UnitRecipeNum = stat.UnitRecipeNum
             and u.RecipeGrpNum = stat.RecipeGrpNum
join run_stat rs
    on rs.id = stat.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where 1 = 1
        and ChargeNr = '{charge_no}'
        and u.RecipeNum = {recipe_no}'''


query_all_running_unit_recipes_kemia = '''
SELECT DISTINCT b.RecipeNr as RecipeNum,
                b.RecipeStartTime AS Start,
                case when '{max_ts}' < {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} then {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}}
                else {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}}
                end as Finish,
                b.RecipeNr as 'Recept komment',
                b.UnitRecipeNr as 'Unit recept komment',
                b.Unit,
                b.BatchID,
                b.ChargeID as ChargeNr, 
                b.UnitRecipeNr as UnitRecipeNum,
                'rgb(60, 180, 75)' AS 'color'
FROM {tabla2} b
left join unitrecipe_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
    and s.UnitRecipeNum = b.UnitRecipeNr
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL or g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}})
and ChargeID = '{charge_no}' and b.RecipeNr = '{recipe_no}'

union

select DISTINCT b.RecipeNr as RecipeNum,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} as Start,
       coalesce(g.Finished, '{max_ts}') as Finish,
       b.RecipeNr as 'Recept komment',
       b.UnitRecipeNr as 'Unit recept komment',
       b.Unit,
       b.BatchID,
       b.ChargeID as ChargeNr, 
       b.UnitRecipeNr,
       'rgb(230, 25, 75)' as 'color'
FROM {tabla2} b
left join unitrecipe_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
    and s.UnitRecipeNum = b.UnitRecipeNr
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} < '{max_ts}')
or (g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}} and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} < g.Finished) 
and ChargeID = '{charge_no}' and b.RecipeNr = '{recipe_no}'

union

select DISTINCT s.RecipeNum,
       g.Finished as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} as Finish, 
       b.RecipeNr as 'Recept komment',
       b.UnitRecipeNr as 'Unit recept komment',
       b.Unit,
       b.BatchID,
       b.ChargeID as ChargeNr, 
       b.UnitRecipeNr,
       'rgb(70, 240, 240)' as 'color'
FROM {tabla2} b
left join unitrecipe_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
    and s.UnitRecipeNum = b.UnitRecipeNr
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL or (g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}} and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} > g.Finished))
and ChargeID = '{charge_no}' and b.RecipeNr = '{recipe_no}'
'''

query_running_operations_in_charge = '''
-- kék
select max(k1.TimeStamp) as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.duration, min(k1.TimeStamp))}} as Finish,
       rd.recipe_comment as 'Recept komment',
       k1.U{unit}_MR,
       ud.unit_recipe_comment as 'Unit recept komment',
       k1.U{unit}_OPNAME,
       od.op_comment as 'Művelet komment',
       '{unit}' as Unit,
       'rgb(70, 240, 240)' as 'color',
       k1.U{unit}_OPNAME as Op,
       k1.U{unit}_OPNR as OpInst
from {table} k1
left join operation_run_stats_def stats
         on stats.RecipeNum = k1.U{unit}_MR
             and stats.UnitRecipeNum = k1.U{unit}_PARTNO
             and stats.Op = k1.U{unit}_OPNAME
             and stats.OpInst = k1.U{unit}_OPNR
join recipe_details rd
         on rd.recipe_num = k1.U{unit}_MR
join unit_recipe_details ud
         on ud.recipe_num = k1.U{unit}_MR
             and ud.unit_recipe_num = k1.U{unit}_PARTNO
join operation_details od
         on od.recipe_num = k1.U{unit}_MR
             and od.unit_recipe_num = k1.U{unit}_PARTNO
             and od.op_type = k1.U{unit}_OPNAME
             and od.op_inst = k1.U{unit}_OPNR
             and od.gantt_visible = 1
join run_stat rs
    on rs.id = stats.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where 1 = 1
  and k1.U{unit}_BATCHID = '{batch_id}'
  and k1.U{unit}_MR = '{recipe_no}'
  and k1.U{unit}_PARTNO = '{unit_recipe_no}'
  and k1.TimeStamp > '{unit_start}'
  and k1.U{unit}_BSTS = 'RUNNING'
group by k1.U{unit}_RECIPE, k1.U{unit}_PARTNO, k1.U{unit}_OPNAME, k1.U{unit}_OPNR, k1.U{unit}_MR, rd.recipe_comment, ud.unit_recipe_comment, od.op_comment, stats.duration
having max(k1.TimeStamp) <> '{max_ts}'
   and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, min(duration), min(k1.TimeStamp))}} > max(k1.TimeStamp) 

union

-- piros
select {{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.duration, min(k1.TimeStamp))}} as Start,
       max(k1.TimeStamp) as Finish,
       rd.recipe_comment as 'Recept komment',
       k1.U{unit}_MR,
       ud.unit_recipe_comment as 'Unit recept komment',
       k1.U{unit}_OPNAME,
       od.op_comment as 'Művelet komment',
       '{unit}' as Unit,
       'rgb(230, 25, 75)' as 'color',
       k1.U{unit}_OPNAME as Op,
       k1.U{unit}_OPNR as OpInst
from {table} k1
left join operation_run_stats_def stats
         on stats.RecipeNum = k1.U{unit}_MR
             and stats.UnitRecipeNum = k1.U{unit}_PARTNO
             and stats.Op = k1.U{unit}_OPNAME
             and stats.OpInst = k1.U{unit}_OPNR
join recipe_details rd
         on rd.recipe_num = k1.U{unit}_MR
join unit_recipe_details ud
         on ud.recipe_num = k1.U{unit}_MR
             and ud.unit_recipe_num = k1.U{unit}_PARTNO
join operation_details od
         on od.recipe_num = k1.U{unit}_MR
             and od.unit_recipe_num = k1.U{unit}_PARTNO
             and od.op_type = k1.U{unit}_OPNAME
             and od.op_inst = k1.U{unit}_OPNR
             and od.gantt_visible = 1
join run_stat rs
    on rs.id = stats.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where 1 = 1
  and k1.U{unit}_BATCHID = '{batch_id}'
  and k1.U{unit}_MR = '{recipe_no}'
  and k1.U{unit}_PARTNO = '{unit_recipe_no}'
  and k1.TimeStamp > '{unit_start}'
  and k1.U{unit}_BSTS = 'RUNNING'
group by k1.U{unit}_RECIPE, k1.U{unit}_PARTNO, k1.U{unit}_OPNAME, k1.U{unit}_OPNR, k1.U{unit}_MR, rd.recipe_comment, ud.unit_recipe_comment, od.op_comment, stats.duration
having (
        (max(k1.TimeStamp) <> '{max_ts}' and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, min(duration), min(k1.TimeStamp))}} < max(k1.TimeStamp))
        or
        (max(k1.TimeStamp) = '{max_ts}' and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, min(duration), min(k1.TimeStamp))}} < '{max_ts}')
    )

union

-- zöld
select min(k1.TimeStamp) as Start,
        case when max(k1.TimeStamp) = ('{max_ts}') and '{max_ts}' < {{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.duration, min(k1.TimeStamp))}} then {{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.duration, min(k1.TimeStamp))}}
                else least(max(k1.TimeStamp), {{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.duration, min(k1.TimeStamp))}})
        end as Finish,
       rd.recipe_comment as 'Recept komment',
       k1.U{unit}_MR,
       ud.unit_recipe_comment as 'Unit recept komment',
       k1.U{unit}_OPNAME,
       od.op_comment as 'Művelet komment',
       '{unit}' as Unit,
       'rgb(60, 180, 75)' as 'color',
       k1.U{unit}_OPNAME as Op,
       k1.U{unit}_OPNR as OpInst
from {table} k1
left join operation_run_stats_def stats
         on stats.RecipeNum = k1.U{unit}_MR
             and stats.UnitRecipeNum = k1.U{unit}_PARTNO
             and stats.Op = k1.U{unit}_OPNAME
             and stats.OpInst = k1.U{unit}_OPNR
join recipe_details rd
         on rd.recipe_num = k1.U{unit}_MR
join unit_recipe_details ud
         on ud.recipe_num = k1.U{unit}_MR
             and ud.unit_recipe_num = k1.U{unit}_PARTNO
join operation_details od
         on od.recipe_num = k1.U{unit}_MR
             and od.unit_recipe_num = k1.U{unit}_PARTNO
             and od.op_type = k1.U{unit}_OPNAME
             and od.op_inst = k1.U{unit}_OPNR
             and od.gantt_visible = 1
join run_stat rs
    on rs.id = stats.stat_id
         and rs.api = '{api}'
         and rs.item_nr = rd.item_num
where 1 = 1
  and k1.U{unit}_BATCHID = '{batch_id}'
  and k1.U{unit}_MR = '{recipe_no}'
  and k1.U{unit}_PARTNO = '{unit_recipe_no}'
  and k1.TimeStamp > '{unit_start}'
  and k1.U{unit}_BSTS = 'RUNNING'
group by k1.U{unit}_RECIPE, k1.U{unit}_PARTNO, k1.U{unit}_OPNAME, k1.U{unit}_OPNR, k1.U{unit}_MR, rd.recipe_comment, ud.unit_recipe_comment, od.op_comment, stats.duration
'''

query_all_running_operation_kemia = '''
SELECT DISTINCT b.RecipeNr as RecipeNum,
                b.OperationStart AS Start,
                case when '{max_ts}' < {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.OperationStart)}} then {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.OperationStart)}}
                else {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.OperationStart)}}
                end as Finish,
                b.RecipeNr as 'Recept komment',
                b.UnitRecipeNr as 'Unit recept komment',
                b.Unit,
                b.BatchID,
                b.ChargeID as ChargeNr, 
                b.UnitRecipeNr as UnitRecipeNum,
                b.OperationName as 'Művelet komment',
                b.OperationName as 'Op',
                b.OperationType as 'OpInst',
                'rgb(60, 180, 75)' AS 'color'
FROM {tabla2} b
left join operation_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
    and s.UnitRecipeNum = b.UnitRecipeNr
    and s.Op = b.OperationType
    and s.OpInst = b. OperationName
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL or g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}})
and ChargeID = '{charge_no}' and b.RecipeNr = '{recipe_no}' and b.UnitRecipeNr = '{unit_recipe_no}'

union

select DISTINCT b.RecipeNr as RecipeNum,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.OperationStart)}} as Start,
       max(b.Timestamp) as Finish,
       b.RecipeNr as 'Recept komment',
       b.UnitRecipeNr as 'Unit recept komment',
       b.Unit,
       b.BatchID,
       b.ChargeID as ChargeNr, 
       b.UnitRecipeNr,
       b.OperationName as 'Művelet komment',
       b.OperationName as 'Op',
       b.OperationType as 'OpInst',
       'rgb(230, 25, 75)' as 'color'
FROM {tabla2} b
left join operation_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
    and s.UnitRecipeNum = b.UnitRecipeNr
    and s.Op = b.OperationType
    and s.OpInst = b. OperationName
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} < '{max_ts}')
or (g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}} and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.RecipeStartTime)}} < g.Finished) 
and ChargeID = '{charge_no}' and b.RecipeNr = '{recipe_no}' and b.UnitRecipeNr = '{unit_recipe_no}'
group by b.RecipeNr, b.UnitRecipeNr, b.OperationName, b.OperationType, s.duration_median, b.OperationStart, b.Unit, b.BatchID, b.ChargeID

union

select DISTINCT b.RecipeNr as RecipeNum,
       max(b.Timestamp) as Start,
       {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.OperationStart)}} as Finish, 
       b.RecipeNr as 'Recept komment',
       b.UnitRecipeNr as 'Unit recept komment',
       b.Unit,
       b.BatchID,
       b.ChargeID as ChargeNr, 
       b.UnitRecipeNr,
       b.OperationName as 'Művelet komment',
       b.OperationName as 'Op',
       b.OperationType as 'OpInst',
       'rgb(70, 240, 240)' as 'color'
FROM {tabla2} b
left join operation_run_stats_2 s
    on s.RecipeNum = b.RecipeNr
    and s.UnitRecipeNum = b.UnitRecipeNr
    and s.Op = b.OperationType
    and s.OpInst = b. OperationName
LEFT JOIN {tabla} g
ON b.RecipeNr = g.recipenum
and b.RecipeStartTime = g.[Started]
WHERE (g.recipenum IS NULL or (g.finished > {{fn TIMESTAMPADD(SQL_TSI_DAY, -28, '{max_ts}')}} and {{fn TIMESTAMPADD(SQL_TSI_MINUTE, duration_median, b.OperationStart)}} > g.Finished))
and ChargeID = '{charge_no}' and b.RecipeNr = '{recipe_no}' and b.UnitRecipeNr = '{unit_recipe_no}'
group by b.RecipeNr, b.UnitRecipeNr, b.OperationName, b.OperationType, s.duration_median, b.OperationStart, b.Unit, b.BatchID, b.ChargeID
'''

query_columns_for_operation = '''
select distinct quantity
from timeseries_stats_op
where stat_id = (select id from ts_stat where default_stat = 1 and item_nr = {item_nr}) 
and recipe_no = {recipe_no}
and unit_recipe_no = {unit_recipe_no}
and op_type = '{op_type}'
and op_inst = {op_inst}
and unit = '{unit}'
'''

query_columns_for_operation_kemia = '''
select distinct quantity
from timeseries_stats_op_2
where stat_id = (select id from ts_stat where default_stat = 1 and item_nr = {item_nr}  and api= '{api}') 
and recipe_no = '{recipe_no}'
and unit_recipe_no = '{unit_recipe_no}'
and op_inst = '{op_type}'
and op_type = '{op_inst}'
and unit = '{unit}'
'''

query_operation_itemnr = '''
select item_num 
from recipe_details
where recipe_num = {recipe_no}
'''

query_operation_itemnr_kemia = '''
select item_num 
from recipe_details_2
where recipe_num = '{recipe_no}'
'''

query_item_nm = '''select stat_name from ts_stat where default_stat = 1 and item_nr = {item_nr}'''

query_item_nm_kemia = '''select stat_name from ts_stat where default_stat = 1 and item_nr = {item_nr} and api = '{api}'
'''

query_tagname_single = '''
select TagName, Dimension, TagID
from {table} 
where TagID = '{col}'
'''


query_when_calculate = '''
select k1.{column_select},
       case when {median_iqr} = 1 then MD else MN end as 'MID',
       case when {median_iqr} = 1 then MD - (MD - Q3) * deviation_multiplier_iqr 
                                else MN + SD * deviation_multiplier_std end as 'up',
       case when {median_iqr} = 1 then MD - (MD - Q1) * deviation_multiplier_iqr 
                                else MN - SD * deviation_multiplier_std end as 'low',
       MIN,
       MAX,
       case when {median_iqr} = 1 then deviation_multiplier_iqr 
                                else deviation_multiplier_std end as deviation_multiplier,
       stats.offset,
       k1.TimeStamp as 'Datum'
from {table} k1
join (
    select Started, Finished, UnitRecipeNum, Op, OpInst, RecipeGrpNum, RecipeNum, Unit
    from EFR_S0000034.k1_apimpp1_gb_operation_view_tunde_view op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = {recipe_no}
      and op.UnitRecipeNum = {unit_recipe_no}
      and op.Op = '{op_type}'
      and op.OpInst = {op_inst}
    union all
    select Started, Finished, UnitRecipeNum, Op, OpInst, RecipeGrpNum, RecipeNum, Unit
    from EFR_S0000067.k1_apimpp2_gb_operation_view_tunde_view op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = {recipe_no}
      and op.UnitRecipeNum = {unit_recipe_no}
      and op.Op = '{op_type}'
      and op.OpInst = {op_inst}
    ) op
         on k1.TimeStamp between op.Started and op.Finished
join timeseries_stats_op stats
    on stats.recipe_group = op.RecipeGrpNum
        and stats.recipe_no = op.RecipeNum
        and stats.unit_recipe_no = op.UnitRecipeNum
        and stats.op_type = op.Op
        and stats.op_inst = op.OpInst
        and stats.quantity = '{column_select}'
        and stats.unit = op.Unit
        and format({{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.offset, op.Started)}}, 'yyyy-MM-dd HH:mm:00') = k1.TimeStamp
join ts_stat s
    on s.id = stats.stat_id
        and s.item_nr = '{item_nr}'
        and s.stat_name = '{stat_name}'   
order by k1.TimeStamp'''


query_when_calculate_kemia = '''
select k1.{column_select},
       case when {median_iqr} = 1 then MD else MN end as 'MID',
       case when {median_iqr} = 1 then MD - (MD - Q3) * deviation_multiplier_iqr 
                                else MN + SD * deviation_multiplier_std end as 'up',
       case when {median_iqr} = 1 then MD - (MD - Q1) * deviation_multiplier_iqr 
                                else MN - SD * deviation_multiplier_std end as 'low',
       MIN,
       MAX,
       case when {median_iqr} = 1 then deviation_multiplier_iqr 
                                else deviation_multiplier_std end as deviation_multiplier,
       stats.offset,
       k1.TimeStamp as 'Datum'
from {table} k1
join (
    select Started, Finished, UnitRecipeNum, opid, opsts, Area, RecipeNum, Unit
    from [EFR_18VEGYES].[k2_vegyes_gb_operation_mg_tunde_view] op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = '{recipe_no}'
      and op.UnitRecipeNum = '{unit_recipe_no}'
      and op.opid = '{op_type}'
      and op.opsts = '{op_inst}'
    union all
    select Started, Finished, UnitRecipeNum, opid, opsts, Area, RecipeNum, Unit
    from [EFR_18VEGTERMEK].[k2_vegtermek_gb_operation_mg_tunde_view] op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = '{recipe_no}'
      and op.UnitRecipeNum = '{unit_recipe_no}'
      and op.opid = '{op_type}'
      and op.opsts = '{op_inst}'
    union all
    select Started, Finished, UnitRecipeNum, opid, opsts, Area, RecipeNum, Unit
    from [EFR_24EPFIR].[k2_24epfir_gb_operation_mg_tunde_view] op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = '{recipe_no}'
      and op.UnitRecipeNum = '{unit_recipe_no}'
      and op.opid = '{op_type}'
      and op.opsts = '{op_inst}'
    ) op
         on k1.TimeStamp between op.Started and op.Finished
join timeseries_stats_op_2 stats
    on stats.recipe_group = op.Area
        and stats.recipe_no = op.RecipeNum
        and stats.unit_recipe_no = op.UnitRecipeNum
        and stats.op_type = op.opsts
        and stats.op_inst = op.opid
        and stats.quantity = '{column_select}'
        and stats.unit = op.Unit
        and format({{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.offset, op.Started)}}, 'yyyy-MM-dd HH:mm:00') = k1.TimeStamp
join ts_stat s
    on s.id = stats.stat_id
        and s.item_nr = '{item_nr}'
        and s.stat_name = '{stat_name}'   
order by k1.TimeStamp'''

query_when_calculate_kemia_24epfir = '''
select k1.{column_select},
       case when {median_iqr} = 1 then MD else MN end as 'MID',
       case when {median_iqr} = 1 then MD - (MD - Q3) * deviation_multiplier_iqr 
                                else MN + SD * deviation_multiplier_std end as 'up',
       case when {median_iqr} = 1 then MD - (MD - Q1) * deviation_multiplier_iqr 
                                else MN - SD * deviation_multiplier_std end as 'low',
       MIN,
       MAX,
       case when {median_iqr} = 1 then deviation_multiplier_iqr 
                                else deviation_multiplier_std end as deviation_multiplier,
       stats.offset,
       k1.igyarto_TimeStamp as 'Datum'
from {table} k1
join (
    select Started, Finished, UnitRecipeNum, opid, opsts, Area, RecipeNum, Unit
    from [EFR_18VEGYES].[k2_vegyes_gb_operation_mg_tunde_view] op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = '{recipe_no}'
      and op.UnitRecipeNum = '{unit_recipe_no}'
      and op.opid = '{op_type}'
      and op.opsts = '{op_inst}'
    union all
    select Started, Finished, UnitRecipeNum, opid, opsts, Area, RecipeNum, Unit
    from [EFR_18VEGTERMEK].[k2_vegtermek_gb_operation_mg_tunde_view] op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = '{recipe_no}'
      and op.UnitRecipeNum = '{unit_recipe_no}'
      and op.opid = '{op_type}'
      and op.opsts = '{op_inst}'
    union all
    select Started, Finished, UnitRecipeNum, opid, opsts, Area, RecipeNum, Unit
    from [EFR_24EPFIR].[k2_24epfir_gb_operation_mg_tunde_view] op
    where 1 = 1
      and op.ChargeNr = '{charge_no}'
      and op.RecipeNum = '{recipe_no}'
      and op.UnitRecipeNum = '{unit_recipe_no}'
      and op.opid = '{op_type}'
      and op.opsts = '{op_inst}'
    ) op
         on k1.igyarto_TimeStamp between op.Started and op.Finished
join timeseries_stats_op_2 stats
    on stats.recipe_group = op.Area
        and stats.recipe_no = op.RecipeNum
        and stats.unit_recipe_no = op.UnitRecipeNum
        and stats.op_type = op.opsts
        and stats.op_inst = op.opid
        and stats.quantity = '{column_select}'
        and stats.unit = op.Unit
        and format({{fn TIMESTAMPADD(SQL_TSI_MINUTE, stats.offset, op.Started)}}, 'yyyy-MM-dd HH:mm:00') = k1.igyarto_TimeStamp
join ts_stat s
    on s.id = stats.stat_id
        and s.item_nr = '{item_nr}'
        and s.stat_name = '{stat_name}'   
order by k1.igyarto_TimeStamp'''