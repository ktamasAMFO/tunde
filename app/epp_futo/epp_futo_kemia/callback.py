import copy
from datetime import datetime as dt
from typing import Optional
from typing import Tuple
import datetime
import pandas as pd
from dash.dash import no_update
import dash_core_components as dcc
from dash.dependencies import Output, Input, State
from plotly.graph_objs import Figure
from dash import callback_context

import app.common.utils as utils
from app import app
from .. import sql
from .. import viz

from . import __name__ as parent_module


df = utils.pages_with_abs_path()
epp_futo_path = df[df['layout_module'] == parent_module].iloc[0]['page_path_absolute']


@app.callback(
    Output('futtatasok-varhato-kemia', 'figure'),
    [Input('url', 'pathname'),
     Input('kemia_tab3', 'value')])
def update_figure_running_plan_sarzsok(path: Optional[str], api: str) -> Figure:
    if path.lstrip('/') != epp_futo_path:
        return no_update
    placeholder_fig = Figure(layout={'height': 600})
    api = 'kemia1' if api == 'kemiai' else api
    if api not in ['kemia1', 'kemia2', 'kemia3']:
        return placeholder_fig

    sql_usual_duration = sql.query_usual_duration_kemia.format(api=api)
    usual_duration = utils.run_query(sql_usual_duration)[0]['usual_duration']
    sql_max_ts = sql.query_max_ts_kemia
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    query = sql.query_running_charge_runs_kemia.format(api=api, usual_duration=usual_duration, max_ts=max_ts)
    data = utils.run_query(query)
    if data is None:
        return placeholder_fig
    df = pd.DataFrame.from_records(data)
    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000058.k2_vegyes_p_tunde_view")
    now: dt = data[0]['Now']
    return viz.gantt_fig_running_sarzsok(df, now)


@app.callback(
    Output('sarzs-varhato-kemia-container', 'children'),
    [Input('url', 'pathname'),
     Input('kemia_tab3', 'value')])
@utils.timeit
def update_figure_running_plan_receptek(path: Optional[str], api: str) -> Figure:
    if path.lstrip('/') != epp_futo_path:
        return no_update
    placeholder_graph = dcc.Graph(id='sarzs-varhato-kemia', figure=Figure(layout={'height': 600}))
    if api not in ['kemiai', 'kemia2', 'kemia3']:
        return placeholder_graph

    cfg = utils.parse_config()
    if cfg.has_option('parameters', 'maxdays'):
        maxdays = int(cfg['parameters']['maxdays'])
    else:
        maxdays = 8

    if api == 'kemiai':
        tabla = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view'
    elif api == 'kemia2':
        tabla = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view'
    else:
        tabla = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_24EPFIR.k2_24epfir_batch_data_operation_p_tunde_view'

    sql_max_ts = sql.query_max_ts_kemia
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    query = sql.query_all_running_recipes_kemia.format(max_ts=max_ts, tabla=tabla, tabla2=tabla2)
    data = utils.run_query(query)
    data_now = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000058.k2_vegyes_p_tunde_view")
    now = data_now[0]['Now']
    if data is None:
        return placeholder_graph
    for i in range(len(data)):
        if not data[i]['Finish']:
            if not data[i]['Start']:
                data[i]['Start'] = now - datetime.timedelta(days=1)
            data[i]['Finish'] = data[i]['Start'] + datetime.timedelta(days=1)

    df = pd.DataFrame.from_records(data)
    
    figure = viz.gantt_fig_running_receptek(df, now)
    return dcc.Graph(id='sarzs-varhato-kemia', figure=figure)


@app.callback(
    Output('recept-varhato-kemia-container', 'children'),
    [Input('sarzs-varhato-kemia', 'clickData'),
     Input('kemia_tab3', 'value')],
    [State('sarzs-varhato-kemia', 'figure')])
@utils.timeit
def update_figure_running_plan_unit_receptek(sarzs_click_data: utils.Click_data, api: str,
                                             sarzs_fig: Figure) -> Figure:
    placeholder_graph = dcc.Graph(id='recept-varhato-kemia', figure=Figure(layout={'height': 600}))

    clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
    if clicked == 'kemia_tab3':
        return placeholder_graph

    if sarzs_fig == {} or sarzs_click_data is None:
        return placeholder_graph
    customdata = utils.get_clicked_attribute_from_gantt(sarzs_click_data, sarzs_fig, 'customdata')[0]
    selected_charge = customdata['ChargeNr']
    selected_recipe = customdata['RecipeNum']
    recept_komment = customdata['Recept komment']

    if api == 'kemiai':
        tabla = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view'
    elif api == 'kemia2':
        tabla = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view'
    else:
        tabla = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_24EPFIR.k2_24epfir_batch_data_operation_p_tunde_view'

    query_params = {'charge_no': selected_charge, 'recipe_no': selected_recipe, 'api': api,
                    'tabla': tabla, 'tabla2': tabla2}

    sql_max_ts = sql.query_max_ts_kemia
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    query_params.update({'max_ts': max_ts})
    sql_query_running_unit_recipes_in_charge = sql.query_all_running_unit_recipes_kemia.format(**query_params)
    data = utils.run_query(sql_query_running_unit_recipes_in_charge)
    if data is None:
        return placeholder_graph
    if not data[0]['Finish']:
        data[0]['Finish'] = data[0]['Start'] + datetime.timedelta(days=1)
    else:
        pass
    df = pd.DataFrame.from_records(data)
    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000058.k2_vegyes_p_tunde_view")
    now = data[0]['Now']
    figure = viz.gantt_fig_running_unit_receptek(df, f'{selected_charge} / {recept_komment}', now)
    return dcc.Graph(id='recept-varhato-kemia', figure=figure)


@app.callback(
    Output('muvelet-varhato-kemia-container', 'children'),
    [Input('recept-varhato-kemia', 'clickData'),
     Input('kemia_tab3', 'value'),
     Input('only-data-kemia', 'value')],
    [State('recept-varhato-kemia', 'figure')])
@utils.timeit
def update_figure_running_plan_muveletek(recept_click_data: utils.Click_data, api: str, only_data,
                                         recept_fig: Figure) -> Figure:
    placeholder_graph = dcc.Graph(id='muvelet-varhato-kemia', figure=Figure(layout={'height': 600}))

    clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
    if clicked == 'kemia_tab3':
        return placeholder_graph

    if recept_fig == {} or recept_click_data is None:
        return placeholder_graph
    customdata = utils.get_clicked_attribute_from_gantt(recept_click_data, recept_fig, 'customdata')[0]
    selected_charge = customdata['ChargeNr']
    selected_recipe = customdata['RecipeNum']
    selected_unit_recipe = customdata['UnitRecipeNum']
    batch_id = customdata['BatchID']
    start = customdata['Start']
    unit = customdata['Unit']
    recept_komment = customdata['Recept komment']
    unit_recept_komment = customdata['Unit recept komment']

    if unit is None:
        return placeholder_graph
    query_params = {'charge_no': selected_charge,
                    'recipe_no': selected_recipe,
                    'unit_recipe_no': selected_unit_recipe,
                    'batch_id': batch_id.strip(),
                    'unit_start': start,
                    'unit': unit,
                    'api': api}

    sql_max_ts = sql.query_max_ts_kemia
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']

    if api == 'kemiai':
        tabla = 'EFR_18VEGYES.k2_vegyes_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view'
    elif api == 'kemia2':
        tabla = 'EFR_18VEGTERMEK.k2_vegtermek_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view'
    else:
        tabla = 'EFR_24EPFIR.k2_24epfir_gb_batch_mg_tunde_view'
        tabla2 = 'EFR_24EPFIR.k2_24epfir_batch_data_operation_p_tunde_view'

    query_params.update({'max_ts': max_ts, 'tabla': tabla, 'tabla2': tabla2})
    sql_query_running_operations_in_charge = sql.query_all_running_operation_kemia.format(**query_params)
    data = utils.run_query(sql_query_running_operations_in_charge)
    if data is None:
        return placeholder_graph
    if not data[0]['Finish']:
        for i in range(len(data)):
            data[i]['Finish'] = data[i]['Start'] + datetime.timedelta(days=1)
    else:
        pass
    df = pd.DataFrame.from_records(data)
    df['RecipeNum'] = selected_recipe
    df['UnitRecipeNum'] = selected_unit_recipe
    df['ChargeNr'] = selected_charge

    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000058.k2_vegyes_p_tunde_view")
    now = data[0]['Now']
    if not only_data:
        figure = viz.gantt_fig_running_muveletek(df, f'{selected_charge} / {recept_komment} / {unit_recept_komment}', now)
        return dcc.Graph(id='muvelet-varhato-kemia', figure=figure)
    else:
        df_copy = copy.deepcopy(df)
        for idx, row in df.iterrows():
            unit = row['Unit']
            recipe_no = row['RecipeNum']
            unit_recipe_no = row['UnitRecipeNum']
            operation_type = row['Op']
            op_inst = row['OpInst']
            query_params = {'recipe_no': recipe_no,
                            'unit_recipe_no': unit_recipe_no,
                            'op_type': operation_type,
                            'op_inst': op_inst,
                            'unit': unit}
            sql_query_operation_itemnr = sql.query_operation_itemnr_kemia.format(**query_params)
            item_nr = utils.run_query(sql_query_operation_itemnr)[0]['item_num']
            if api == 'kemiai':
                api2 = 'Vegyes'
            elif api == 'kemia2':
                api2 = 'Vegtermek'
            else:
                api2 = '24epfir'
            query_params.update({'item_nr': item_nr.strip(), 'api': api2})
            sql_query_columns_for_operation = sql.query_columns_for_operation_kemia.format(**query_params)
            data = utils.run_query(sql_query_columns_for_operation)
            if data is None:
                df_copy = df_copy.drop([idx])
        if df_copy.empty:
            return placeholder_graph
        figure = viz.gantt_fig_running_muveletek(df_copy, f'{selected_charge} / {recept_komment} / {unit_recept_komment}', now)
        return dcc.Graph(id='muvelet-varhato-kemia', figure=figure)


@app.callback(
    [Output('futtatasok-sensor-plot-dropdown-kemia', 'options'),
     Output('futtatasok-sensor-plot-dropdown-kemia', 'disabled'),
     Output('futtatasok-sensor-plot-dropdown-kemia', 'placeholder')],
    [Input('muvelet-varhato-kemia', 'clickData')],
    [State('muvelet-varhato-kemia', 'figure'),
     State('kemia_tab3', 'value')])
@utils.timeit
def update_dropdown_options(muvelet_click_data: utils.Click_data,
                            muvelet_fig: Figure, api: str
                            ) -> Tuple[utils.Options, bool, str]:
    if muvelet_fig == {} or muvelet_click_data is None:
        return [], True, "Válasszon ki egy műveletet"

    customdata = utils.get_clicked_attribute_from_gantt(muvelet_click_data, muvelet_fig, 'customdata')[0]
    unit = customdata['Unit']
    recipe_no = customdata['RecipeNum']
    unit_recipe_no = customdata['UnitRecipeNum']
    operation_type = customdata['Op']
    op_inst = customdata['OpInst']
    query_params = {'recipe_no': recipe_no,
                    'unit_recipe_no': unit_recipe_no,
                    'op_type': operation_type,
                    'op_inst': op_inst,
                    'unit': unit}
    sql_query_operation_itemnr = sql.query_operation_itemnr_kemia.format(**query_params)
    item_nr_sql = utils.run_query(sql_query_operation_itemnr)
    if item_nr_sql is None:
        missing_data_message = f'Nincs ilyen recept a recipe_datails2 táblában: {recipe_no}'
        return [], True, missing_data_message
    item_nr = item_nr_sql[0]['item_num']
    if api == 'kemiai':
        api2 = 'Vegyes'
    elif api == 'kemia2':
        api2 = 'Vegtermek'
    else:
        api2 = '24epfir'
    query_params.update({'item_nr': item_nr.strip(), 'api': api2})
    sql_query_columns_for_operation = sql.query_columns_for_operation_kemia.format(**query_params)
    data = utils.run_query(sql_query_columns_for_operation)
    missing_stat_message = f"Nincs idősoros statisztika a kiválasztott művelethez ({item_nr} cikkszám)"
    if data is None:
        return [], True, missing_stat_message
    columns = [row['quantity'] for row in data]
    options = [{'label': col, 'value': col} for col in columns]

    return options, False, "Válasszon ki egy mennyiséget"


@app.callback(
    Output('futtatasok-sensor-plot-kemia', 'figure'),
    [Input('futtatasok-sensor-plot-button-kemia', 'n_clicks'),
     Input('futtatasok-plot-deviation-kemia', 'value'),
     Input('futtatasok-plot-deviation-type-kemia', 'value'),
     Input('kemia_tab3', 'value')],
    [State('muvelet-varhato-kemia', 'clickData'),
     State('muvelet-varhato-kemia', 'figure'),
     State('futtatasok-sensor-plot-dropdown-kemia', 'value'),
     State('futtatasok-yaxis-scale-kemia', 'value'),
     State('futtatasok-yscale-min-kemia', 'value'),
     State('futtatasok-yscale-max-kemia', 'value')])
@utils.timeit
def update_figure_sensor_plot(n_clicks: int, plot_deviation: str, deviation_type: str, api: str,
                              muvelet_click_data: utils.Click_data,
                              muvelet_fig: Figure, chosen_col: Optional[str],
                              yscale_mode: str, y_min: int, y_max: int
                              ) -> Figure:
    placeholder_fig = Figure(layout={'height': 800})

    clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
    if clicked == 'kemia_tab3':
        return placeholder_fig

    if n_clicks == 0 or muvelet_fig == {} or muvelet_click_data is None or chosen_col is None or (
            yscale_mode == 'manual' and (y_min is None or y_max is None)):
        return placeholder_fig
    customdata = utils.get_clicked_attribute_from_gantt(muvelet_click_data, muvelet_fig, 'customdata')[0]
    autoclave_num = customdata['Unit']
    recipe_comment = customdata['Recept komment']
    unit_recipe_comment = customdata['Unit recept komment']
    operation_comment = customdata['Művelet komment']
    charge_no = customdata['ChargeNr']
    recipe_no = customdata['RecipeNum']
    unit_recipe_no = customdata['UnitRecipeNum']
    operation_type = customdata['Op']
    op_id = customdata['OpInst']

    query_params = {'charge_no': charge_no, 'recipe_no': recipe_no, 'unit_recipe_no': unit_recipe_no,
                    'op_type': operation_type.strip(), 'op_inst': op_id, 'unit': autoclave_num}
    sql_query_operation_itemnr = sql.query_operation_itemnr_kemia.format(**query_params)
    item_nr = utils.run_query(sql_query_operation_itemnr)[0]['item_num'].strip()
    if api == 'kemiai':
        api2 = 'Vegyes'
    elif api == 'kemia2':
        api2 = 'Vegtermek'
    else:
        api2 = '24epfir'
    sql_query_item_nm = sql.query_item_nm_kemia.format(item_nr=item_nr, api=api2)
    stat_name = utils.run_query(sql_query_item_nm)[0]['stat_name']
    # mysql 5.7-ben a '1' igaznak értékelődik ki, '0' hamisnak

    if api == 'kemiai':
        table = 'EFR_S0000058.k2_vegyes_p_tunde_view'
    elif api == 'kemia2':
        table = 'EFR_S0000057.k2_vegtermek_p_tunde_view'
    else:
        table = 'EFR_24EPFIR.k2_24epfir_p_tunde_view'

    query_params.update({'item_nr': item_nr.strip(), 'median_iqr': deviation_type, 'stat_name': stat_name,
                         'column_select': f'{chosen_col}', 'table':table})
    if api == 'kemia3':
        query = sql.query_when_calculate_kemia_24epfir.format(**query_params)
    else:
        query = sql.query_when_calculate_kemia.format(**query_params)
    data = utils.run_query(query)
    if data is None:
        return placeholder_fig
    df = pd.DataFrame.from_records(data)

    if plot_deviation == 'relative':
        deviation = True
    elif plot_deviation == 'absolute':
        deviation = False
    else:
        raise ValueError('Not a valid value for plot_deviation')
    fig = viz.figure_of_muvelet_stats_kemia(df, chosen_col, charge_no, 0,
                                      [recipe_comment, unit_recipe_comment, operation_comment], autoclave_num, api,
                                      deviation_plot=deviation)
    if yscale_mode == 'manual':
        fig['layout']['yaxis']['range'] = [y_min, y_max]
    return fig
