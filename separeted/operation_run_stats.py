import pandas as pd
import run_all


def operation_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == '1':
        operation_view = 'k1_apimpp_1_gb_operation'
        unitrecipe_view = 'k1_apimpp_1_gb_unitrecipe'
    elif api == '2':
        operation_view = 'k1_apimpp_2_gb_operation'
        unitrecipe_view = 'k1_apimpp_2_gb_unitrecipe'
    else:
        raise ValueError

    # művelet statisztikák
    # receptszám és recept csoport alapján csoportosítva számolunk statisztikákat
    # azokra a receptekre amik a kiválasztott sarzsokban szerepelnek
    query_recipes = f'''
select vu.RecipeGrpNum,
       vu.RecipeNum,
       vu.UnitRecipeNum,
       vo.OpInst,
       vo.Op,
       timestampdiff(minute, vo.Started, vo.Finished) as duration,
       timestampdiff(minute, vu.Started, vo.Started) as offset,
#        timestampdiff(minute, c.started, vu.Started) as total_offset_old,
       rs.offset_median + us.offset_median + timestampdiff(minute, vu.Started, vo.Started) as total_offset
  from charges c
  join run_stat_charges rsc
       on c.charge_nr = rsc.charge_nr
           and c.api = %(api)s
           and date(c.started) = rsc.charge_start_date
           and rsc.stat_id = %(stat_id)s
  join efr_data.{unitrecipe_view} vu
       on vu.ChargeNr = c.charge_nr
           and vu.Started between c.started and c.finished
  join efr_data.{operation_view} vo
       on vo.RecipeGrpNum = vu.RecipeGrpNum
           and vo.RecipeNum = vu.RecipeNum
           and vo.UnitRecipeNum = vu.UnitRecipeNum
           and vo.ChargeNr = c.charge_nr
           and vo.Started >= vu.Started
           and vo.Finished <= vu.Finished
  join recipe_run_stats rs
       on rs.stat_id = %(stat_id)s
           and rs.RecipeGrpNum = vu.RecipeGrpNum
           and rs.RecipeNum = vu.RecipeNum
  join unitrecipe_run_stats us
       on us.stat_id = %(stat_id)s
           and us.RecipeGrpNum = vu.RecipeGrpNum
           and us.RecipeNum = vu.RecipeNum
           and us.UnitRecipeNum = vu.UnitRecipeNum'''
    operations_data = run_all.run_query(query_recipes, {'api': api, 'stat_id': stat_id})
    operations_df = pd.DataFrame().from_records(operations_data)
    stats = operations_df.groupby(['RecipeGrpNum', 'RecipeNum', 'UnitRecipeNum', 'OpInst', 'Op']).agg('describe')
    stats['stat_id'] = stat_id
    stats.columns = [f'{level0}_{level1}' if level1 is not '' else level0
                     for level0, level1 in stats.columns.values]
    stats.drop(labels=['duration_count', 'duration_mean', 'duration_std',
                       'offset_count', 'offset_mean', 'offset_std',
                       'total_offset_count', 'total_offset_mean', 'total_offset_std'],
               axis='columns', inplace=True)
    stats.columns = [column.replace('25%', 'q1').replace('75%', 'q3').replace('50%', 'median')
                     for column in stats.columns]
    stats.name = 'operation_run_stats'

    return stats
