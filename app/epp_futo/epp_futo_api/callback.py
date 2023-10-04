import copy
from datetime import datetime as dt
from typing import Optional
from typing import Tuple

import pandas as pd
from dash.dash import no_update
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
    Output('futtatasok-varhato', 'figure'),
    [Input('url', 'pathname'),
     Input('api_tab3', 'value')])
def update_figure_running_plan_sarzsok(path: Optional[str], api: str) -> Figure:
    if path.lstrip('/') != epp_futo_path:
        return no_update
    placeholder_fig = Figure(layout={'height': 600})
    if api not in ['1', '2']:
        return placeholder_fig
    sql_usual_duration = sql.query_usual_duration.format(api=api)
    usual_duration = utils.run_query(sql_usual_duration)[0]['usual_duration']
    sql_max_ts = sql.query_max_ts
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    query = sql.query_running_charge_runs.format(api=api, usual_duration=usual_duration, max_ts=max_ts)
    data = utils.run_query(query)
    if data is None:
        return placeholder_fig
    df = pd.DataFrame.from_records(data)
    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000034.k1_apimppi_p_tunde_view")
    now: dt = data[0]['Now']
    return viz.gantt_fig_running_sarzsok(df, now)


@app.callback(
    Output('sarzs-varhato', 'figure'),
    [Input('url', 'pathname'),
     Input('api_tab3', 'value')])
@utils.timeit
def update_figure_running_plan_receptek(path: Optional[str], api: str) -> Figure:
    if path.lstrip('/') != epp_futo_path:
        return no_update
    placeholder_fig = Figure(layout={'height': 600})
    if api not in ['1', '2']:
        return placeholder_fig

    cfg = utils.parse_config()
    if cfg.has_option('parameters', 'maxdays'):
        maxdays = int(cfg['parameters']['maxdays'])
    else:
        maxdays = 8

    if api == '1':
        tabla = 'EFR_S0000034.k1_apimpp1_gb_batch_view_tunde_view'
    else:
        tabla = 'EFR_S0000067.k1_apimpp2_gb_batch_view_tunde_view'

    sql_max_ts = sql.query_max_ts
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    query = sql.query_all_running_recipes.format(api=api, max_ts=max_ts, maxdays=maxdays, tabla=tabla)
    data = utils.run_query(query)

    if data is None:
        return placeholder_fig
    df = pd.DataFrame.from_records(data)
    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000034.k1_apimppi_p_tunde_view")
    now = data[0]['Now']
    return viz.gantt_fig_running_receptek(df, now)


@app.callback(
    Output('recept-varhato', 'figure'),
    [Input('sarzs-varhato', 'clickData'),
     Input('api_tab3', 'value')],
    [State('sarzs-varhato', 'figure'),
     State('api_tab3', 'value')])
@utils.timeit
def update_figure_running_plan_unit_receptek(sarzs_click_data: utils.Click_data, click,
                                             sarzs_fig: Figure, api: str) -> Figure:
    placeholder_fig = Figure(layout={'height': 600})

    clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
    if clicked == 'api_tab3':
        return placeholder_fig

    if sarzs_fig == {} or sarzs_click_data is None:
        return placeholder_fig
    customdata = utils.get_clicked_attribute_from_gantt(sarzs_click_data, sarzs_fig, 'customdata')[0]
    selected_charge = customdata['ChargeNr']
    selected_recipe = customdata['RecipeNum']
    recept_komment = customdata['Recept komment']
    query_params = {'charge_no': selected_charge, 'recipe_no': selected_recipe, 'api': api}

    sql_max_ts = sql.query_max_ts
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    query_params.update({'max_ts': max_ts})
    sql_query_running_unit_recipes_in_charge = sql.query_running_unit_recipes_in_charge.format(**query_params)
    data = utils.run_query(sql_query_running_unit_recipes_in_charge)
    if data is None:
        return placeholder_fig
    df = pd.DataFrame.from_records(data)
    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000034.k1_apimppi_p_tunde_view")
    now = data[0]['Now']
    return viz.gantt_fig_running_unit_receptek(df, f'{selected_charge} / {recept_komment}', now)


@app.callback(
    Output('muvelet-varhato', 'figure'),
    [Input('recept-varhato', 'clickData'),
     Input('api_tab3', 'value'),
     Input('only-data', 'value')],
    [State('recept-varhato', 'figure'),
     State('api_tab3', 'value')])
@utils.timeit
def update_figure_running_plan_muveletek(recept_click_data: utils.Click_data, click, only_data,
                                         recept_fig: Figure, api: str) -> Figure:
    placeholder_fig = Figure(layout={'height': 600})

    clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
    if clicked == 'api_tab3':
        return placeholder_fig

    if recept_fig == {} or recept_click_data is None:
        return placeholder_fig
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
        return placeholder_fig
    query_params = {'charge_no': selected_charge,
                    'recipe_no': selected_recipe,
                    'unit_recipe_no': selected_unit_recipe,
                    'batch_id': batch_id.strip(),
                    'unit_start': start,
                    'unit': unit,
                    'api': api}

    sql_max_ts = sql.query_max_ts
    max_ts = utils.run_query(sql_max_ts)[0]['max_ts']
    if api == '1':
        table = 'EFR_S0000070.k1_apimppi_operation_p_tunde_view'
    else:
        table = 'EFR_S0000069.k1_apimpp2_operation_p_tunde_view'
    query_params.update({'max_ts': max_ts, 'table': table})
    sql_query_running_operations_in_charge = sql.query_running_operations_in_charge.format(**query_params)
    data = utils.run_query(sql_query_running_operations_in_charge)
    if data is None:
        return placeholder_fig
    df = pd.DataFrame.from_records(data)
    df['RecipeNum'] = selected_recipe
    df['UnitRecipeNum'] = selected_unit_recipe
    df['ChargeNr'] = selected_charge

    data = utils.run_query("select max(TimeStamp) as 'Now' from EFR_S0000034.k1_apimppi_p_tunde_view")
    now = data[0]['Now']
    if not only_data:
        return viz.gantt_fig_running_muveletek(df, f'{selected_charge} / {recept_komment} / {unit_recept_komment}', now)
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
            sql_query_operation_itemnr = sql.query_operation_itemnr.format(**query_params)
            item_nr = utils.run_query(sql_query_operation_itemnr)[0]['item_num']
            query_params.update({'item_nr': item_nr.strip()})
            sql_query_columns_for_operation = sql.query_columns_for_operation.format(**query_params)
            data = utils.run_query(sql_query_columns_for_operation)
            if data is None:
                df_copy = df_copy.drop([idx])
        if df_copy.empty:
            return placeholder_fig
        return viz.gantt_fig_running_muveletek(df_copy, f'{selected_charge} / {recept_komment} / {unit_recept_komment}', now)


@app.callback(
    [Output('futtatasok-sensor-plot-dropdown', 'options'),
     Output('futtatasok-sensor-plot-dropdown', 'disabled'),
     Output('futtatasok-sensor-plot-dropdown', 'placeholder')],
    [Input('muvelet-varhato', 'clickData')],
    [State('muvelet-varhato', 'figure'),
     State('api_tab3', 'value')])
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
    sql_query_operation_itemnr = sql.query_operation_itemnr.format(**query_params)
    item_nr = utils.run_query(sql_query_operation_itemnr)[0]['item_num']
    query_params.update({'item_nr': item_nr.strip()})
    sql_query_columns_for_operation = sql.query_columns_for_operation.format(**query_params)
    data = utils.run_query(sql_query_columns_for_operation)
    missing_stat_message = f"Nincs idősoros statisztika a kiválasztott művelethez ({item_nr} cikkszám)"
    if data is None:
        return [], True, missing_stat_message
    columns = [row['quantity'] for row in data]
    options = [{'label': col, 'value': col} for col in columns]

    return options, False, "Válasszon ki egy mennyiséget"


@app.callback(
    Output('futtatasok-sensor-plot', 'figure'),
    [Input('futtatasok-sensor-plot-button', 'n_clicks'),
     Input('futtatasok-plot-deviation', 'value'),
     Input('futtatasok-plot-deviation-type', 'value'),
     Input('api_tab3', 'value')],
    [State('muvelet-varhato', 'clickData'),
     State('muvelet-varhato', 'figure'),
     State('futtatasok-sensor-plot-dropdown', 'value'),
     State('futtatasok-yaxis-scale', 'value'),
     State('futtatasok-yscale-min', 'value'),
     State('futtatasok-yscale-max', 'value'),
     State('api_tab3', 'value')])
@utils.timeit
def update_figure_sensor_plot(n_clicks: int, plot_deviation: str, deviation_type: str, click,
                              muvelet_click_data: utils.Click_data,
                              muvelet_fig: Figure, chosen_col: Optional[str],
                              yscale_mode: str, y_min: int, y_max: int, api: str
                              ) -> Figure:
    placeholder_fig = Figure(layout={'height': 800})

    clicked = callback_context.triggered[0]['prop_id'].split('.')[0]
    if clicked == 'api_tab3':
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
    sql_query_operation_itemnr = sql.query_operation_itemnr.format(**query_params)
    item_nr = utils.run_query(sql_query_operation_itemnr)[0]['item_num']

    sql_query_item_nm = sql.query_item_nm.format(item_nr=item_nr)
    stat_name = utils.run_query(sql_query_item_nm)[0]['stat_name']
    # mysql 5.7-ben a '1' igaznak értékelődik ki, '0' hamisnak
    if api == '1':
        table = 'EFR_S0000034.k1_apimppi_p_tunde_view'
    else:
        table = 'EFR_S0000067.k1_apimpp2_p_tunde_view'
    query_params.update({'item_nr': item_nr.strip(), 'median_iqr': deviation_type, 'stat_name': stat_name,
                         'column_select': f'{chosen_col}', 'table':table})
    query = sql.query_when_calculate.format(**query_params)
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
    fig = viz.figure_of_muvelet_stats(df, chosen_col, charge_no, 0,
                                      [recipe_comment, unit_recipe_comment, operation_comment], autoclave_num, api,
                                      deviation_plot=deviation)
    if yscale_mode == 'manual':
        fig['layout']['yaxis']['range'] = [y_min, y_max]
    return fig
