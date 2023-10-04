import re
import uuid
import time
from typing import List
from typing import Optional

import dash_bootstrap_components as dbc
from dash.dependencies import Input
from dash.dependencies import Output
from dash.dependencies import State
from dash.exceptions import PreventUpdate

from app import app
from app.common import utils as utils
from app.statisztikak.callback_kemia import stat_charge_chkbxs
from app.statisztikak.callback_kemia import stat_chosen_product
from app.statisztikak.callback_kemia import stat_fill_selected_values
from app.statisztikak.callback_kemia import stat_itemnr_options
from app.statisztikak.callback_kemia import stat_sel_charges
from app.statisztikak.callback_kemia import stat_time_chkbxs
from app.statisztikak.callback_kemia import connect_to_db
from app.statisztikak.callback_kemia import execute_inserts
from app.statisztikak.callback_kemia import recipe_run_stats
from app.statisztikak.callback_kemia import unitrecipe_run_stats
from app.statisztikak.callback_kemia import operation_run_stats


@app.callback(
    Output('create-run-stat-itemnr-dropdown-kemia', 'options'),
    [Input('create-run-stat-api-kemia', 'value')])
def run_stat_itemnr_options(api: str) -> utils.Options:
    options = stat_itemnr_options(api)
    return options


@app.callback(
    Output('create-run-stat-chosen-product-kemia', 'value'),
    [Input('create-run-stat-itemnr-dropdown-kemia', 'value'),
     Input('delete-run-stat-new-kemia', 'n_clicks')])
def run_stat_chosen_product(itemnr: Optional[str], click: Optional[int]) -> str:
    trig_id, _ = utils.triggered()
    if trig_id == 'create-run-stat-itemnr-dropdown-kemia':
        return stat_chosen_product(itemnr)
    elif trig_id == 'delete-run-stat-new-kemia':
        return ''


@app.callback(
    Output('create-run-stat-time-checkboxes-kemia', 'options'),
    [Input('create-run-stat-itemnr-dropdown-kemia', 'value'),
     Input('create-run-stat-api-kemia', 'value')],
)
def run_stat_time_chkbxs(itemnr: str, api: str) -> utils.Options:
    if len(itemnr) == 0:
        return []
    times = stat_time_chkbxs(itemnr, api)
    return times


@app.callback(
    Output('create-run-stat-time-checkboxes-kemia', 'value'),
    [Input('create-run-stat-api-kemia', 'value'),
     Input('create-run-stat-itemnr-dropdown-kemia', 'value'),
     Input('run-stats-table-kemia', 'selected_rows')],
     [State('run-stats-table-kemia', 'data'),
      State('run-stats-table-kemia', 'selected_rows')]
)
def run_clr_times_sel(_api: Optional[str], _itemnr: Optional[str], _selected_row: List[int], data: utils.Table_data, selected_row: List[int]) -> List[str]:
    trig_id, _ = utils.triggered()
    if trig_id == 'run-stats-table-kemia':
        data_list = stat_fill_selected_values(data, selected_row, table='run_stat_charges')
        return data_list[6]
    else:
        return []



@app.callback(
    Output('create-run-stat-charge-checkboxes-kemia', 'options'),
    [Input('create-run-stat-time-checkboxes-kemia', 'value'),
     Input('create-run-stat-api-kemia', 'value'),
     Input('create-run-stat-itemnr-dropdown-kemia', 'value'),
     Input('run-stats-table-kemia', 'selected_rows')
     ])
def run_stat_charge_chkbxs(months: Optional[List[str]], api: Optional[str],
                           itemnr: Optional[str], _selected_row: List[int]) -> utils.Options:
    charge_options = stat_charge_chkbxs(api, itemnr, months, ('create-run-stat-api', 'create-run-stat-itemnr-dropdown'))
    return charge_options


@app.callback(
    Output('create-run-stat-charge-checkboxes-kemia', 'value'),
    [Input('create-run-stat-all-charges-kemia', 'n_clicks'),
     Input('create-run-stat-api-kemia', 'value'),
     Input('create-run-stat-itemnr-dropdown-kemia', 'value'),
     Input('create-run-stat-time-checkboxes-kemia', 'value'),
     Input('run-stats-table-kemia', 'selected_rows')
     ],
    [State('create-run-stat-charge-checkboxes-kemia', 'options'),
     State('create-run-stat-charge-checkboxes-kemia', 'value'),
     State('run-stats-table-kemia', 'data'),
     State('run-stats-table-kemia', 'selected_rows')
     ])
def run_stat_sel_charges(_button_click: Optional[int], api: str, itemnr: str,
                         months: List[str],_selected_row: List[int], charge_opts: utils.Options,
                         prev_sel_charges: List[str], data: utils.Table_data, selected_row: List[int]) -> List[str]:
    trig_id, _ = utils.triggered()
    if trig_id == 'run-stats-table-kemia':
        data_list = stat_fill_selected_values(data, selected_row, table='run_stat_charges')
        return data_list[8]
    else:
        return stat_sel_charges(api, charge_opts, itemnr, months, prev_sel_charges,
                            'create-run-stat-api', 'create-run-stat-itemnr-dropdown', 'create-run-stat-time-checkboxes')


def create_new_run_stat(api, charges_dates, itemnr, name):
    sql_name = '''select 1 from run_stat where stat_name = '{name}' '''.format(**{'name': name})
    exists = utils.run_query_new(sql_name)
    if exists is not None:
        # ha már létezik ilyen nevű, ideiglenes nevet adunk az újnak
        used_name = f'{name}-{str(uuid.uuid4())[:6]}'
    else:
        used_name = name
    if api == 'kemia1':
        api2 = 'Vegyes'
    elif api == 'kemia2':
        api2 = 'Vegtermek'
    elif api == 'kemia3':
        api2 = '24epfir'
    insert_stat_query = '''insert into run_stat (stat_name, api, item_nr, state, default_stat) values ('{name}', '{api}', '{item_nr}', '{state}', 0)'''
    state = 'Folyamatban'
    query_params = {'name': used_name, 'api': api2, 'item_nr': itemnr, 'state': state}
    sql_insert_stat_query = insert_stat_query.format(**query_params)
    utils.run_query_new(sql_insert_stat_query)
    query = '''select id from run_stat where stat_name = '{name}' '''.format(**{'name': used_name})
    stat_id: int = utils.run_query_new(query)[0]['id']
    if itemnr == '70160110' and api == 'kemia2':
        charges_dates = [re.search('(\d+/\w+/\d+)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates]
    elif itemnr == '70092110' and api == 'kemia3':
        charges_dates = [re.search('(G\d+/\d+) \((\d{4}-\d{2}-\d{2})\)', charge_date).groups()
                         for charge_date in charges_dates if
                         re.search('(G\d+/\d+) \((\d{4}-\d{2}-\d{2})\)', charge_date) is not None]
    else:
        charges_dates = [re.search('(\d+/\d+\D*)\D+(\d{4}-\d{2}-\d{2})', charge_date).groups()
                         for charge_date in charges_dates]
    insert_values = ', '.join([f"({stat_id}, '{charge}', '{date}')" for charge, date in charges_dates])
    insert_stat_charges_query = (
        f'insert into run_stat_charges (stat_id, charge_nr, charge_start_date) '
        f'values {insert_values}')
    utils.run_query_new(insert_stat_charges_query)
    # recipe_run_stat
    recipe_stats = recipe_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db()[0], [recipe_stats])
    # unit_recipe_run_stat
    unitrecipe_stats = unitrecipe_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db()[0], [unitrecipe_stats])
    # operation_run_stat
    operation_stats = operation_run_stats(str(api), stat_id)
    execute_inserts(connect_to_db()[0], [operation_stats])

    query_params = {'stat_id': stat_id, 'state': 'Siker'}
    upd_query = '''update run_stat set state = '{state}' where id = '{stat_id}' '''.format(**query_params)
    utils.run_query_new(upd_query)


@app.callback(
    [Output('create-run-stat-api-kemia', 'value'),
     Output('create-run-stat-itemnr-dropdown-kemia', 'value')],
    [Input('delete-run-stat-new-kemia', 'n_clicks'),
     Input('run-stats-table-kemia', 'selected_rows')],
     [State('run-stats-table-kemia', 'data')]
)
def delete_content(click: Optional[int], slct, data):
    trig_id, _ = utils.triggered()
    if trig_id == 'delete-run-stat-new-kemia':
        return None, []
    elif trig_id == 'run-stats-table-kemia' and slct != []:
        data_list = stat_fill_selected_values(data, slct, table='run_stat_charges')
        return data_list[3], data_list[4].strip()
    else:
        raise PreventUpdate

@app.callback(
    [Output('existing-run-stat-id-kemia', 'value'),
     Output('existing-run-stat-name-kemia', 'value'),
     Output('existing-run-stat-chosen-product-kemia', 'value'),
     Output('existing-run-stat-api-kemia', 'value'),
     Output('existing-run-stat-itemnr-dropdown-kemia', 'placeholder'),
     Output('existing-run-stat-time-checkboxes-kemia', 'options'),
     Output('existing-run-stat-time-checkboxes-kemia', 'value'),
     Output('existing-run-stat-charge-checkboxes-kemia', 'options'),
     Output('existing-run-stat-charge-checkboxes-kemia', 'value')],
    [Input('run-stats-table-kemia', 'selected_rows')],
    [State('run-stats-table-kemia', 'data')])
def run_stat_fill_selected_values(selected_row: List[int], data: utils.Table_data):
    time.sleep(1)
    fill = stat_fill_selected_values(data, selected_row, table='run_stat_charges')
    return fill

# @app.callback(
#     Output('run-stats-table-kemia', 'selected_rows'),
#     [Input('existing-run-stat-name-kemia', 'value')])
# def run_deselect_table_row(_):
#     return []


@app.callback(
    Output('run-stats-table-kemia', 'data'),
    [Input('run-stats-edit-refresh-kemia', 'n_clicks'),
     Input('create-run-stat-new-kemia', 'n_clicks'),
     Input('run-stats-edit-delete-kemia', 'n_clicks'),
     Input('run-stats-edit-default-kemia', 'n_clicks'),
     Input('run-stats-edit-rerun-kemia', 'n_clicks')],
    [State('create-run-stat-name-kemia', 'value'),
     State('create-run-stat-api-kemia', 'value'),
     State('create-run-stat-itemnr-dropdown-kemia', 'value'),
     State('create-run-stat-charge-checkboxes-kemia', 'value'),
     State('existing-run-stat-name-kemia', 'value'),
     State('existing-run-stat-api-kemia', 'value'),
     State('existing-run-stat-itemnr-dropdown-kemia', 'placeholder'),
     State('existing-run-stat-charge-checkboxes-kemia', 'value'),
     State('existing-run-stat-id-kemia', 'value')]
)
def run_refresh_table(_refresh_click: Optional[int], new_clicks: Optional[int],
                  del_clicks: Optional[int], def_clicks: Optional[int], rerun_clicks: Optional[int],
                  name: Optional[str], api: Optional[str], itemnr: Optional[str],
                  charges_dates: Optional[List[str]],
                  existing_name: Optional[str], existing_api: Optional[str], existing_itemnr: Optional[str],
                  existing_charges_dates: Optional[List[str]], edit_id: Optional[str]
                  ) -> utils.Table_data:
    trig_id, _ = utils.triggered()
    if trig_id == 'create-run-stat-new-kemia' and new_clicks is not None:
        if itemnr is None or charges_dates == []:
            raise PreventUpdate
        else:
            create_new_run_stat(api, charges_dates, itemnr, name)
    if trig_id == 'run-stats-edit-rerun-kemia' and rerun_clicks is not None:
        create_new_run_stat(existing_api, existing_charges_dates, existing_itemnr, existing_name)

    if trig_id == 'run-stats-edit-delete-kemia' and del_clicks is not None and edit_id is not None:
        query_params = {'stat_id': edit_id}
        query = "delete from operation_run_stats where stat_id = '{stat_id}'".format(**query_params)
        utils.run_query_new(query)
        query = "delete from unitrecipe_run_stats where stat_id = (select id from run_stat where id = '{stat_id}')".format(**query_params)
        utils.run_query_new(query)
        query = "delete from recipe_run_stats where stat_id = (select id from run_stat where id = '{stat_id}')".format(**query_params)
        utils.run_query_new(query)
        query = "delete from run_stat_charges where stat_id = (select id from run_stat  where id = '{stat_id}')".format(**query_params)
        utils.run_query_new(query)
        query = "delete from run_stat where id = '{stat_id}'".format(**query_params)
        utils.run_query_new(query)

    if trig_id == 'run-stats-edit-default-kemia' and def_clicks is not None and edit_id is not None:
        query_params = {'stat_id': edit_id}
        query = "select item_nr from run_stat where id = '{stat_id}'".format(**query_params)
        sel_itemnr = utils.run_query_new(query)[0]['item_nr']
        query_params.update({'item_nr': sel_itemnr.strip()})
        query = "update run_stat set default_stat = 0 where item_nr = '{item_nr}'".format(**query_params)
        utils.run_query_new(query)
        query = "update run_stat set default_stat = 1 where id = '{stat_id}'".format(**query_params)
        utils.run_query_new(query)

    stats_table_query = ("select id, stat_name, api, item_nr, state, default_stat from run_stat where (api != '1' and api != '2')")
    data = utils.run_query_new(stats_table_query)
    return data if data is not None else []


@app.callback(
    Output('alert-kemia', 'children'),
    [Input('create-run-stat-new-kemia', 'n_clicks')],
    [State('create-run-stat-name-kemia', 'value'),
     State('create-run-stat-api-kemia', 'value'),
     State('create-run-stat-itemnr-dropdown-kemia', 'value'),
     State('create-run-stat-charge-checkboxes-kemia', 'value')]
)
def run_stat_alert(new_clicks: Optional[int],name: Optional[str], api: Optional[str], itemnr: Optional[str],
                  charges_dates: Optional[List[str]],):
    trig_id, _ = utils.triggered()
    if trig_id == 'create-run-stat-new-kemia' and new_clicks is not None:
        if itemnr is None or charges_dates == [] or name is None or api is None:
            alert = dbc.Alert("A név, api, cikkszám megadása és a sarzsok kiválasztása kötelező", color="danger", dismissable=True)
            return alert
