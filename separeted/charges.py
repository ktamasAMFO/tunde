from datetime import timedelta as td
import pandas as pd


import run_all


query_update = '''
update charges 
set finished = %(finished)s 
where id = %(id)s'''


def define_charges(maxdays: int) -> None:
    apis = (('k1_apimpp_1_gb_batch', '1'), ('k1_apimpp_2_gb_batch', '2'))

    for recipe_view, api in apis:

        query_new_recipes = (
            f"select ChargeNr, Started, Finished "
            f"from efr_data.{recipe_view} "
            f"where 1 = 1 "
            f"    and ChargeNr <> '' "
            f"    and ChargeNr not like 'T%%' "
            f"    and Started > (select date_add(coalesce(max(started), '1900-01-01'), "
            f"                                   interval - %(maxdays)s day) "
            f"                   from charges "
            f"                   where api = %(api)s) "
            f"order by Started")
        query_charges = 'select id, api, charge_nr, started, finished from charges'

        new_recipes_rows = run_all.run_query(query_new_recipes,
                                           {'api': api,
                                            'maxdays': maxdays}) or []
        new_recipes = pd.DataFrame(new_recipes_rows)

        for idx, recipe in new_recipes.iterrows():
            charges_rows = run_all.run_query(query_charges) or []
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

                query_charge_finish = f'''
select IF(max(coalesce(Finished, now())) = now(), null, max(Finished)) as finish
from efr_data.{recipe_view}
where ChargeNr = %(charge_nr)s
and Started between %(started)s and date_add(%(started)s, interval %(maxdays)s day)'''

                charge_finish = run_all.run_query(query_charge_finish, {
                    'maxdays': maxdays,
                    'started': recipe['Started'],
                    'charge_nr': recipe['ChargeNr']
                })[0]['finish']

                query_insert_charge = '''
insert into charges (api, charge_nr, started, finished) 
values (%(api)s, %(charge_nr)s, %(started)s, %(finished)s)'''
                run_all.run_query(query_insert_charge, {
                    'api': api,
                    'charge_nr': recipe['ChargeNr'],
                    'started': recipe['Started'],
                    'finished': charge_finish})
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
                run_all.run_query(query_update, {'id': overlap['id'].iloc[0],
                                               'finished': max_finish})
