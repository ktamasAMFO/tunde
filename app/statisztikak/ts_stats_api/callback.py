import re
import subprocess
import uuid
import time
from typing import List
from typing import Optional
from typing import Union

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
from app.statisztikak.callback import operation_stats


@app.callback(
    Output('create-ts-stat-itemnr-dropdown', 'options'),
    [Input('create-ts-stat-api', 'value')])
def ts_stat_itemnr_options(api: str) -> utils.Options:
    trig_id, _ = utils.triggered()
    if not api:
        raise PreventUpdate
    else:
        options = stat_itemnr_options(api)
        return options

@app.callback(
    [Output('create-ts-stat-api', 'value'),
     Output('create-ts-stat-median_iqr', 'value'),
     Output('create-ts-stat-multiplier-iqr', 'value'),
     Output('create-ts-stat-multiplier-std', 'value'),
     Output('create-ts-stat-itemnr-dropdown', 'value')],
    [Input('ts-stats-table', 'selected_rows'),
     Input('delete-ts-stat-new', 'n_clicks')],
    [State('ts-stats-table', 'data')])
def ts_stat_fill_selected_values(selected_row: List[int], click: Optional[int], data: utils.Table_data):
    trig_id, _ = utils.triggered()
    if trig_id == 'delete-ts-stat-new':
        return None, None, None, None, []
    elif trig_id == 'ts-stats-table' and selected_row != []:
        data_list = stat_fill_selected_values(data, selected_row, table='ts_stat_charges')
        return data_list[3], data_list[9], data_list[10], data_list[11], data_list[4].strip()
    else:
        raise PreventUpdate


@app.callback(
    Output('create-ts-stat-chosen-product', 'value'),
    [Input('create-ts-stat-itemnr-dropdown', 'value'),
     Input('delete-ts-stat-new', 'n_clicks')])
def ts_stat_chosen_product(itemnr: Optional[str], delete) -> str:
    trig_id, _ = utils.triggered()
    if trig_id == 'delete-ts-stat-new':
        return ''
    if not itemnr:
        raise PreventUpdate
    else:
        return stat_chosen_product(itemnr)


@app.callback(
    Output('create-ts-stat-time-checkboxes', 'options'),
    [Input('create-ts-stat-itemnr-dropdown', 'value'),
     Input('create-ts-stat-api', 'value')])
def ts_stat_time_chkbxs(itemnr: str, api: str) -> utils.Options:
    times = stat_time_chkbxs(itemnr, api)
    return times


@app.callback(
    Output('create-ts-stat-time-checkboxes', 'value'),
    [Input('create-ts-stat-api', 'value'),
     Input('create-ts-stat-itemnr-dropdown', 'value'),
     Input('ts-stats-table', 'selected_rows')],
    [State('ts-stats-table', 'data')]
)
def ts_clr_times_sel(_api: Optional[str], _itemnr: Optional[str], selected_row: List[int], data: utils.Table_data) -> List[str]:
    trig_id, _ = utils.triggered()
    if trig_id == 'ts-stats-table':
        data_list = stat_fill_selected_values(data, selected_row, table='ts_stat_charges')
        return data_list[6]
    else:
        return []


@app.callback(
    Output('create-ts-stat-charge-checkboxes', 'options'),
    [Input('create-ts-stat-time-checkboxes', 'value'),
     Input('create-ts-stat-api', 'value'),
     Input('create-ts-stat-itemnr-dropdown', 'value'),
     Input('ts-stats-table', 'selected_rows')])
def ts_stat_charge_chkbxs(months: Optional[List[str]], api: Optional[str],
                          itemnr: Optional[str], _selected_row: List[int]) -> utils.Options:
    charges = stat_charge_chkbxs(api, itemnr, months, ('create-ts-stat-api', 'create-ts-stat-itemnr-dropdown'))
    return charges


@app.callback(
    Output('create-ts-stat-charge-checkboxes', 'value'),
    [Input('create-ts-stat-all-charges', 'n_clicks'),
     Input('create-ts-stat-api', 'value'),
     Input('create-ts-stat-itemnr-dropdown', 'value'),
     Input('create-ts-stat-time-checkboxes', 'value'),
     Input('ts-stats-table', 'selected_rows')],
    [State('create-ts-stat-charge-checkboxes', 'options'),
     State('create-ts-stat-charge-checkboxes', 'value'),
     State('ts-stats-table', 'data')])
def ts_stat_sel_charges(_create_click: Optional[int], api: str, itemnr: str,
                        months: List[str], selected_row: List[int], charge_opts: utils.Options,
                        prev_sel_charges: List[str], data: utils.Table_data) -> List[str]:
    trig_id, _ = utils.triggered()
    if trig_id == 'ts-stats-table':
        data_list = stat_fill_selected_values(data, selected_row, table='ts_stat_charges')
        return data_list[8]
    else:
        return stat_sel_charges(api, charge_opts, itemnr, months, prev_sel_charges,
                            'create-ts-stat-api', 'create-ts-stat-itemnr-dropdown', 'create-ts-stat-time-checkboxes')


def create_new_ts_stat(api, charges_dates, itemnr, median_iqr, multiplier_iqr, multiplier_std, name):
    # ellenőrizzük, hogy ilyen statisztika létezik-e már
    sql_name = '''select 1 from ts_stat where stat_name = '{name}' '''.format(**{'name': name})
    exists = utils.run_query_new(sql_name)
    if exists is not None:
        # ha már létezik, ideiglenes nevet adunk az újnak
        used_name = f'{name}-{str(uuid.uuid4())[:6]}'
    else:
        used_name = name
    insert_stat_query = '''
        insert into ts_stat (stat_name, api, item_nr, state, default_stat, use_median_iqr, deviation_multiplier_iqr, deviation_multiplier_std)
        values ('{name}', '{api}', '{item_nr}', '{state}', 0, {median_iqr}, {multiplier_iqr}, {multiplier_std})'''
    state = 'Folyamatban'

    sql_insert_ts_query = insert_stat_query.format(**{'name': used_name, 'api': api,
                                                      'item_nr': itemnr, 'state': state,
                                                      'median_iqr': median_iqr,
                                                      'multiplier_iqr': str(multiplier_iqr).replace(',', '.'),
                                                      'multiplier_std': str(multiplier_std).replace(',', '.')})
    utils.run_query_new(sql_insert_ts_query)
    sql_get_id = '''select id from ts_stat where stat_name = '{name}' '''.format(**{'name': used_name})

    stat_id: int = utils.run_query_new(sql_get_id)[0]['id']

    charges_dates = [re.search('(\d+-\d*) \((\d\d\d\d-\d\d-\d\d)\)', charge_date).groups()
                     for charge_date in charges_dates]
    insert_values = ', '.join([f"({stat_id}, '{charge.strip()}', '{date}')" for charge, date in charges_dates])
    insert_stat_charges_query = (
        f'insert into ts_stat_charges (stat_id, charge_nr, charge_start_date) '
        f'values {insert_values}')
    utils.run_query_new(insert_stat_charges_query)

    operation_stats(stat_id)

    query_params = {'stat_id': stat_id, 'state': 'Siker'}
    upd_query = '''update ts_stat set state = '{state}' where id = '{stat_id}' '''.format(**query_params)
    utils.run_query_new(upd_query)


@app.callback(
    [Output('existing-ts-stat-id', 'value'),
     Output('existing-ts-stat-name', 'value'),
     Output('existing-ts-stat-chosen-product', 'value'),
     Output('existing-ts-stat-api', 'value'),
     Output('existing-ts-stat-itemnr-dropdown', 'placeholder'),
     Output('existing-ts-stat-time-checkboxes', 'options'),
     Output('existing-ts-stat-time-checkboxes', 'value'),
     Output('existing-ts-stat-charge-checkboxes', 'options'),
     Output('existing-ts-stat-charge-checkboxes', 'value'),
     Output('existing-ts-stat-median_iqr', 'value'),
     Output('existing-ts-stat-multiplier-iqr', 'value'),
     Output('existing-ts-stat-multiplier-std', 'value')],
    [Input('ts-stats-table', 'selected_rows')],
    [State('ts-stats-table', 'data')])
def ts_stat_fill_selected_values(selected_row: List[int], data: utils.Table_data):
    time.sleep(1)
    return stat_fill_selected_values(data, selected_row, table='ts_stat_charges')


# @app.callback(
#     Output('ts-stats-table', 'selected_rows'),
#     [Input('existing-ts-stat-name', 'value')])
# def ts_deselect_table_row(_):
#     return []


@app.callback(
    Output('ts-stats-table', 'data'),
    [Input('ts-stats-edit-refresh', 'n_clicks'),
     Input('create-ts-stat-new', 'n_clicks'),
     Input('ts-stats-edit-delete', 'n_clicks'),
     Input('ts-stats-edit-change', 'n_clicks'),
     Input('ts-stats-edit-default', 'n_clicks'),
     Input('ts-stats-edit-rerun', 'n_clicks')],
    [State('create-ts-stat-name', 'value'),
     State('create-ts-stat-api', 'value'),
     State('create-ts-stat-itemnr-dropdown', 'value'),
     State('create-ts-stat-charge-checkboxes', 'value'),
     State('create-ts-stat-median_iqr', 'value'),
     State('create-ts-stat-multiplier-iqr', 'value'),
     State('create-ts-stat-multiplier-std', 'value'),
     State('existing-ts-stat-name', 'value'),
     State('existing-ts-stat-api', 'value'),
     State('existing-ts-stat-itemnr-dropdown', 'placeholder'),
     State('existing-ts-stat-charge-checkboxes', 'value'),
     State('existing-ts-stat-median_iqr', 'value'),
     State('existing-ts-stat-multiplier-iqr', 'value'),
     State('existing-ts-stat-multiplier-std', 'value'),
     State('existing-ts-stat-id', 'value')]
)
def ts_refresh_table(_refresh_click: Optional[int], new_clicks: Optional[int], delete_clicks: Optional[int],
                     change_click: Optional[int], def_clicks: Optional[int],
                     rerun_clicks: Optional[int], name: Optional[str], api: Optional[str], itemnr: Optional[str],
                     charges_dates: Optional[List[str]], median_iqr: Optional[str],
                     multiplier_iqr: Optional[Union[int, str]], multiplier_std: Optional[Union[int, str]],
                     existing_name: Optional[str], existing_api: Optional[str], existing_itemnr: Optional[str],
                     existing_charges_dates: Optional[List[str]], edit_median_iqr: Optional[str],
                     edit_multiplier_iqr: Optional[Union[int, str]], edit_multiplier_std: Optional[Union[int, str]],
                     edit_id: Optional[str]
                     ) -> utils.Table_data:
    trig_id, _ = utils.triggered()
    if trig_id == 'create-ts-stat-new' and new_clicks is not None:
        if charges_dates == [] or multiplier_iqr is None or multiplier_std is None:
            raise PreventUpdate
        else:
            try:
                int(multiplier_iqr), int(multiplier_std)
                create_new_ts_stat(api, charges_dates, itemnr, median_iqr, multiplier_iqr, multiplier_std, name)
            except:
                raise PreventUpdate

    if trig_id == 'ts-stats-edit-rerun' and rerun_clicks is not None:
        create_new_ts_stat(existing_api, existing_charges_dates, existing_itemnr, edit_median_iqr, edit_multiplier_iqr, edit_multiplier_std, existing_name)


    if trig_id == 'ts-stats-edit-change' and change_click is not None and edit_id is not None:
        query_params = {'multiplier_iqr': str(edit_multiplier_iqr).replace(',', '.'),
                        'multiplier_std': str(edit_multiplier_std).replace(',', '.'),
                        'median_iqr': edit_median_iqr, 'stat_id': edit_id}
        query = """update ts_stat
                    set deviation_multiplier_iqr = '{multiplier_iqr}', 
                        deviation_multiplier_std = '{multiplier_std}', 
                        use_median_iqr = '{median_iqr}' 
                    where id = '{stat_id}' """.format(**query_params)
        utils.run_query_new(query)

    if trig_id == 'ts-stats-edit-delete' and delete_clicks is not None:
        query_params = {'edit_id': edit_id}
        query = '''
                delete from ts_stat
                where id = '{edit_id}';

                delete from ts_stat_charges
                where stat_id = '{edit_id}';

                delete from timeseries_stats_op
                where stat_id = '{edit_id}'; 
                '''.format(**query_params)
        utils.run_query_new(query)

    if trig_id == 'ts-stats-edit-default' and def_clicks is not None:
        query_params = {'edit_id': edit_id, 'item_nr': itemnr}
        query = '''
                    update ts_stat 
                    set default_stat = 1 
                    where id = '{edit_id}';
                    update ts_stat
                    set default_stat = 0
                    where default_stat = 1
                        and id <> '{edit_id}'
                        and item_nr = '{item_nr}';
                        '''.format(**query_params)
        utils.run_query_new(query)

    stats_table_query = ("""select id, stat_name, api, item_nr, state, default_stat,
    use_median_iqr, deviation_multiplier_iqr, deviation_multiplier_std
    from ts_stat where (api = '1' or api = '2')""")
    data = utils.run_query_new(stats_table_query)
    return data if data is not None else []

@app.callback(
    Output('alert_ts', 'children'),
    [Input('create-ts-stat-new', 'n_clicks')],
    [State('create-ts-stat-name', 'value'),
     State('create-ts-stat-median_iqr', 'value'),
     State('create-ts-stat-multiplier-iqr', 'value'),
     State('create-ts-stat-multiplier-std', 'value'),
     State('create-ts-stat-itemnr-dropdown', 'value'),
     State('create-ts-stat-charge-checkboxes', 'value'),]
)
def ts_alert(new_clicks: Optional[int], name: Optional[str], median_iqr: Optional[str],
             multiplier_iqr: Optional[Union[int, str]], multiplier_std: Optional[Union[int, str]],
             itemnr: Optional[str], charges_dates: Optional[List[str]],):
    trig_id, _ = utils.triggered()
    if trig_id == 'create-ts-stat-new' and new_clicks is not None:
        if charges_dates == [] or multiplier_iqr is None or multiplier_std is None or itemnr is None or median_iqr is None or name is None:
            alert = dbc.Alert("A név, api, cikkszám megadása és a sarzsok kiválasztása kötelező", color="danger",
                              dismissable=True)
            return alert
        else:
            try:
                int(multiplier_iqr), int(multiplier_std)
            except:
                alert = dbc.Alert("Kérem az STD és IQR szorzókhoz számokat adjon meg", color="danger",
                                  dismissable=True)
                return alert
