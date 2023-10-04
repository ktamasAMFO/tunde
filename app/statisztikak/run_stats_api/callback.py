import re
import subprocess
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
from app.statisztikak.callback import stat_charge_chkbxs
from app.statisztikak.callback import stat_chosen_product
from app.statisztikak.callback import stat_fill_selected_values
from app.statisztikak.callback import stat_itemnr_options
from app.statisztikak.callback import stat_sel_charges
from app.statisztikak.callback import stat_time_chkbxs
from app.statisztikak.callback import connect_to_db
from app.statisztikak.callback import execute_inserts
from app.statisztikak.callback import recipe_run_stats
from app.statisztikak.callback import unitrecipe_run_stats
from app.statisztikak.callback import operation_run_stats


@app.callback(
    Output('create-run-stat-itemnr-dropdown', 'options'),
    [Input('create-run-stat-api', 'value')])
def run_stat_itemnr_options(api: str) -> utils.Options:
    options = stat_itemnr_options(api)
    return options


@app.callback(
    Output('create-run-stat-chosen-product', 'value'),
    [Input('create-run-stat-itemnr-dropdown', 'value'),
     Input('delete-run-stat-new', 'n_clicks')])
def run_stat_chosen_product(itemnr: Optional[str], click: Optional[int]) -> str:
    trig_id, _ = utils.triggered()
    if trig_id == 'create-run-stat-itemnr-dropdown':
        return stat_chosen_product(itemnr)
    elif trig_id == 'delete-run-stat-new':
        return ''


@app.callback(
    Output('create-run-stat-time-checkboxes', 'options'),
    [Input('create-run-stat-itemnr-dropdown', 'value'),
     Input('create-run-stat-api', 'value')],
)
def run_stat_time_chkbxs(itemnr: str, api: str) -> utils.Options:
    return stat_time_chkbxs(itemnr, api)


@app.callback(
    Output('create-run-stat-time-checkboxes', 'value'),
    [Input('create-run-stat-api', 'value'),
     Input('create-run-stat-itemnr-dropdown', 'value'),
     Input('run-stats-table', 'selected_rows')],
    [State('run-stats-table', 'data')]
)
def run_clr_times_sel(_api: Optional[str], _itemnr: Optional[str], _selected_row: List[int], data: utils.Table_data) -> List[str]:
    trig_id, _ = utils.triggered()
    if trig_id == 'run-stats-table':
        data_list = stat_fill_selected_values(data, _selected_row, table='run_stat_charges')
        return data_list[6]
    else:
        return []



@app.callback(
    Output('create-run-stat-charge-checkboxes', 'options'),
    [Input('create-run-stat-time-checkboxes', 'value'),
     Input('create-run-stat-api', 'value'),
     Input('create-run-stat-itemnr-dropdown', 'value'),
     Input('run-stats-table', 'selected_rows')])
def run_stat_charge_chkbxs(months: Optional[List[str]], api: Optional[str],
                           itemnr: Optional[str], _selected_row: List[int]) -> utils.Options:
    return stat_charge_chkbxs(api, itemnr, months, ('create-run-stat-api', 'create-run-stat-itemnr-dropdown'))


@app.callback(
    Output('create-run-stat-charge-checkboxes', 'value'),
    [Input('create-run-stat-all-charges', 'n_clicks'),
     Input('create-run-stat-api', 'value'),
     Input('create-run-stat-itemnr-dropdown', 'value'),
     Input('create-run-stat-time-checkboxes', 'value'),
     Input('run-stats-table', 'selected_rows')
     ],
    [State('create-run-stat-charge-checkboxes', 'options'),
     State('create-run-stat-charge-checkboxes', 'value'),
     State('run-stats-table', 'data'),
     State('run-stats-table', 'selected_rows')
     ])
def run_stat_sel_charges(_button_click: Optional[int], api: str, itemnr: str,
                         months: List[str],_selected_row: List[int], charge_opts: utils.Options,
                         prev_sel_charges: List[str], data: utils.Table_data, selected_row: List[int]) -> List[str]:
    trig_id, _ = utils.triggered()
    if trig_id == 'run-stats-table':
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
    insert_stat_query = '''insert into run_stat (stat_name, api, item_nr, state, default_stat) values ('{name}', '{api}', '{item_nr}', '{state}', 0)'''
    state = 'Folyamatban'
    query_params = {'name': used_name, 'api': api, 'item_nr': itemnr, 'state': state}
    sql_insert_stat_query = insert_stat_query.format(**query_params)
    utils.run_query_new(sql_insert_stat_query)
    query = '''select id from run_stat where stat_name = '{name}' '''.format(**{'name': used_name})
    stat_id: int = utils.run_query_new(query)[0]['id']
    charges_dates = [re.search('(\d+-\d*) \((\d\d\d\d-\d\d-\d\d)\)', charge_date).groups()
                     for charge_date in charges_dates]
    insert_values = ', '.join([f"({stat_id}, '{charge}', '{date}')" for charge, date in charges_dates])
    insert_stat_charges_query = (
        f'insert into run_stat_charges (stat_id, charge_nr, charge_start_date) '
        f'values {insert_values}')
    utils.run_query_new(insert_stat_charges_query)

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
    [Output('create-run-stat-api', 'value'),
     Output('create-run-stat-itemnr-dropdown', 'value')],
    [Input('delete-run-stat-new', 'n_clicks'),
     Input('run-stats-table', 'selected_rows')],
    [State('run-stats-table', 'data')]
)
def delete_content(click: Optional[int], slct, data):
    trig_id, _ = utils.triggered()
    if trig_id == 'delete-run-stat-new':
        return None, []
    elif trig_id == 'run-stats-table' and slct != []:
        data_list = stat_fill_selected_values(data, slct, table='run_stat_charges')
        return data_list[3], data_list[4].strip()
    else:
        raise PreventUpdate

@app.callback(
    [Output('existing-run-stat-id', 'value'),
     Output('existing-run-stat-name', 'value'),
     Output('existing-run-stat-chosen-product', 'value'),
     Output('existing-run-stat-api', 'value'),
     Output('existing-run-stat-itemnr-dropdown', 'placeholder'),
     Output('existing-run-stat-time-checkboxes', 'options'),
     Output('existing-run-stat-time-checkboxes', 'value'),
     Output('existing-run-stat-charge-checkboxes', 'options'),
     Output('existing-run-stat-charge-checkboxes', 'value')],
    [Input('run-stats-table', 'selected_rows')],
    [State('run-stats-table', 'data')])
def run_stat_fill_selected_values(selected_row: List[int], data: utils.Table_data):
    time.sleep(1)
    return stat_fill_selected_values(data, selected_row, table='run_stat_charges')

# @app.callback(
#     Output('run-stats-table', 'selected_rows'),
#     [Input('existing-run-stat-name', 'value')])
# def run_deselect_table_row(_):
#     return []


@app.callback(
    Output('run-stats-table', 'data'),
    [Input('run-stats-edit-refresh', 'n_clicks'),
     Input('create-run-stat-new', 'n_clicks'),
     Input('run-stats-edit-delete', 'n_clicks'),
     Input('run-stats-edit-default', 'n_clicks'),
     Input('run-stats-edit-rerun', 'n_clicks')],
    [State('create-run-stat-name', 'value'),
     State('create-run-stat-api', 'value'),
     State('create-run-stat-itemnr-dropdown', 'value'),
     State('create-run-stat-charge-checkboxes', 'value'),
     State('existing-run-stat-name', 'value'),
     State('existing-run-stat-api', 'value'),
     State('existing-run-stat-itemnr-dropdown', 'placeholder'),
     State('existing-run-stat-charge-checkboxes', 'value'),
     State('existing-run-stat-id', 'value')]
)
def run_refresh_table(_refresh_click: Optional[int], new_clicks: Optional[int],
                  del_clicks: Optional[int], def_clicks: Optional[int], rerun_clicks: Optional[int],
                  name: Optional[str], api: Optional[str], itemnr: Optional[str],
                  charges_dates: Optional[List[str]],
                  existing_name: Optional[str], existing_api: Optional[str], existing_itemnr: Optional[str],
                  existing_charges_dates: Optional[List[str]], edit_id: Optional[str]
                  ) -> utils.Table_data:
    trig_id, _ = utils.triggered()
    if trig_id == 'create-run-stat-new' and new_clicks is not None:
        if itemnr is None or charges_dates == []:
            raise PreventUpdate
        else:
            create_new_run_stat(api, charges_dates, itemnr, name)
    if trig_id == 'run-stats-edit-rerun' and rerun_clicks is not None:
        create_new_run_stat(existing_api, existing_charges_dates, existing_itemnr, existing_name)

    if trig_id == 'run-stats-edit-delete' and del_clicks is not None and edit_id is not None:
        query_params = {'stat_id': edit_id}
        query = "delete from operation_run_stats where stat_id = '{stat_id}'".format(**query_params)
        utils.run_query_new(query)
        query = "delete from unitrecipe_run_stats where stat_id = (select id from run_stat where id = '{stat_id}')".format(
            **query_params)
        utils.run_query_new(query)
        query = "delete from recipe_run_stats where stat_id = (select id from run_stat where id = '{stat_id}')".format(
            **query_params)
        utils.run_query_new(query)
        query = "delete from run_stat_charges where stat_id = (select id from run_stat  where id = '{stat_id}')".format(
            **query_params)
        utils.run_query_new(query)
        query = "delete from run_stat where id = '{stat_id}'".format(**query_params)
        utils.run_query_new(query)

    if trig_id == 'run-stats-edit-default' and def_clicks is not None and edit_id is not None:
        query_params = {'stat_id': edit_id}
        query = "select item_nr from run_stat where id = '{stat_id}'".format(**query_params)
        sel_itemnr = utils.run_query_new(query)[0]['item_nr']
        query_params.update({'item_nr': sel_itemnr})
        query = "update run_stat set default_stat = 0 where item_nr = '{item_nr}'".format(**query_params)
        utils.run_query_new(query)
        query = "update run_stat set default_stat = 1 where id = '{stat_id}'".format(**query_params)
        utils.run_query_new(query)

    stats_table_query = ("select id, stat_name, api, item_nr, state, default_stat from run_stat where (api = '1' or api = '2')")
    data = utils.run_query_new(stats_table_query)
    return data if data is not None else []

@app.callback(
    Output('alert', 'children'),
    [Input('create-run-stat-new', 'n_clicks')],
    [State('create-run-stat-name', 'value'),
     State('create-run-stat-api', 'value'),
     State('create-run-stat-itemnr-dropdown', 'value'),
     State('create-run-stat-charge-checkboxes', 'value'),]
)
def run_stat_alert(new_clicks: Optional[int],name: Optional[str], api: Optional[str], itemnr: Optional[str],
                  charges_dates: Optional[List[str]],):
    trig_id, _ = utils.triggered()
    if trig_id == 'create-run-stat-new' and new_clicks is not None:
        if itemnr is None or charges_dates == [] or name is None or api is None:
            alert = dbc.Alert("A név, api, cikkszám megadása és a sarzsok kiválasztása kötelező", color="danger", dismissable=True)
            return alert
