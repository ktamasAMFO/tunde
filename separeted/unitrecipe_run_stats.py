import pandas as pd


import run_all


def unitrecipe_run_stats(api: str, stat_id: int) -> pd.DataFrame:
    """
    A futási statisztikákról generál pandas dataframe-et.
    """
    if api == '1':
        recipe_view = 'k1_apimpp_1_gb_batch'
        unitrecipe_view = 'k1_apimpp_1_gb_unitrecipe'
    elif api == '2':
        recipe_view = 'k1_apimpp_2_gb_batch'
        unitrecipe_view = 'k1_apimpp_2_gb_unitrecipe'
    else:
        raise ValueError

    # unitrecept statisztikák
    # receptszám, recept csoport és unitrecept szám alapján csoportosítva számolunk statisztikákat
    # azokra a unitreceptekre amik a kiválasztott sarzsokban szerepelnek
    query_unitrecipes = f'''
select vu.RecipeGrpNum,
       vu.RecipeNum,
       vu.UnitRecipeNum,
       timestampdiff(minute, vu.Started, vu.Finished) as duration,
       timestampdiff(minute, vr.Started, vu.Started) as offset,
#        timestampdiff(minute, c.started, vu.Started) as total_offset_alt,
       rs.offset_median + timestampdiff(minute, vr.Started, vu.Started) as total_offset
  from charges c
  join run_stat_charges rsc
       on c.charge_nr = rsc.charge_nr
           and c.api = %(api)s
           and date(c.started) = rsc.charge_start_date
           and rsc.stat_id = %(stat_id)s
  join efr_data.{unitrecipe_view} vu
       on vu.ChargeNr = c.charge_nr
           and vu.Started between c.started and c.finished
  join efr_data.{recipe_view} vr
       on vr.RecipeGrpNum = vu.RecipeGrpNum
           and vr.RecipeNum = vu.RecipeNum
           and vr.ChargeNr = c.charge_nr
           and vu.Started >= vr.Started
           and vu.Finished <= vr.Finished
  join recipe_run_stats rs
       on rs.stat_id = %(stat_id)s
           and rs.RecipeGrpNum = vr.RecipeGrpNum
           and rs.RecipeNum = vr.RecipeNum
'''
    unitrecipes_data = run_all.run_query(query_unitrecipes, {'api': api, 'stat_id': stat_id})
    unitrecipes_df = pd.DataFrame().from_records(unitrecipes_data)
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
    stats.name = 'unitrecipe_run_stats'

    return stats
