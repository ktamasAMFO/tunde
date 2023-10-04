from typing import Dict, Tuple, Optional
import pandas as pd
import re

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Output, Input

import app.common.utils as utils
from app import app
from .. import sql
from .. import viz


@app.callback(
    Output('monitor-container-kemia', 'children'),
    [Input('monitor-interval-kemia', 'n_intervals'),
     Input('kemia_tab4', 'value'),
     Input('button-monitoring-refresh', 'n_clicks')])
@utils.timeit
def update_graphs(_n_intervals: Optional[int], api: str, _refresh):
    # TODO: sanitize api
    graph_rows = list()
    children_with_no_data = list()
    process_started = list()
    table_data = [html.Tr([html.Th('Unit szám'),
                            html.Th('Recept szám'),
                            html.Th('Unit recept szám'),
                            html.Th('Művelet típus'),
                            html.Th('Op_inst'),
                            html.Th('Ciklus szám'),
                            html.Th('Kezdet'),
                            html.Th('Rendelési szám'),
                            html.Th('Batch ID'),
                            html.Th('ItemNr'),
                            html.Th('ChargeNr')
                            ])]
    running_units = get_running_units(api=api)
    # running_units = [running_units[0]]
    print(api)
    print(running_units)
    if len(running_units) > 0:
        for u in running_units:
            df, stats = process_running_unit(u) if u['plot'] else (None, None)
            if df is not None:
                # Sorting is needed because the kind of time series scatterplots we will make
                # can get ruined if the timestamps are not in order
                df = df.sort_values('TimeStamp')
                # TODO consolidate monitoring
                # here we use the MR field as recipe_num, but in process_running_unit() we used RECIPE as recipe_comment
                op_details_params = {'recipe_num': u['ID']['MR'],
                                     'unit_recipe_num': u['ID']['PARTNO'],
                                     'op_inst': u['ID']['OPNR'],
                                     'op_type': u['ID']['OPNAME'].strip()}
                sql_query_details_for_operation = sql.query_details_for_operation_kemia.format(**op_details_params)
                details_data = utils.run_query(sql_query_details_for_operation)

                if details_data is None:
                    children_with_no_data.append(
                        html.Div(children=[
                            html.H4(f"{u['unit']}. Unit"),
                            html.Div("Nem található komment az adott művelethez, "
                                     "illetve a hozzá tartozó recepthez vagy unitrecepthez")
                        ])
                    )
                    continue

                details = details_data[0]
                try:
                    if api == 'kemia1':
                        table = f"EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view"
                    elif api == 'kemia2':
                        table = f"EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view"
                    else:
                        table = f"EFR_24EPFIR.k2_24epfir_batch_data_operation_p_tunde_view"
                    ur_query_params = {'recipe_num': u['ID']['MR'], 'unit_recipe_num': u['ID']['PARTNO'],
                                       'table': table}
                    sql_query_charge_running_unit_recipe = sql.query_charge_running_unit_recipe_kemia.format(**ur_query_params)
                    charge_no = utils.run_query(sql_query_charge_running_unit_recipe)[0]
                except TypeError:
                    app.logger.error(f"No running unit recipe found with "
                                     f"recipe_num {u['ID']['MR']}, "
                                     f"unit_recipe_num {u['ID']['PARTNO']}, "
                                     f"even though it is supposedly running on unit {u['unit']} now")
                    continue
                process_started.append(df['TimeStamp'].min())

                duration_query_params = dict(op_details_params, **{'item_nr': details['item_num'].strip(),
                                                                   'start': df['TimeStamp'].min()})
                sql_query_duration = sql.query_duration_kemia.format(**duration_query_params)
                duration = utils.run_query(sql_query_duration)
                small_graphs = list()
                for index, s in enumerate(stats):
                    last_sensor_ts = df[~df[s].isna()]['TimeStamp'].max()
                    # median_end = (dt.strptime(duration[0]['median_end'], '%Y-%m-%d %H:%M:%S')
                    #               if duration is not None else last_sensor_ts)
                    median_end = (duration[0]['median_end'] if duration is not None else last_sensor_ts)
                    x_axis_end = max(median_end, last_sensor_ts)
                    fig = viz.figure_monitoring(df, s)
                    # col_name_map = {k.replace('{num}', u['unit']): v for k, v in utils.column_map_autoklav.items()}
                    # readable_yaxis_title = col_name_map[fig['layout']['yaxis']['title_text']].replace('{num}',
                    #                                                                                   u['unit'])
                    fig['layout']['yaxis']['title_text'] = s
                    fig.update_xaxes(range=[df['TimeStamp'].min(), x_axis_end])
                    small_graphs.append(
                        html.Div(
                            dcc.Graph(
                                id=f'graph-{index + 1}',
                                figure=fig
                            ),
                            className='monitoring-graph-small'
                        )
                    )
                # az adott művelet futási statisztikáit lekérjük
                # {'recipe_num': 183.0, 'unit_recipe_num': 5, 'op_inst': 2.0, 'op_type': 'DESTA', 'item_nr': '70133010',
                #  'start': Timestamp('2019-09-28 06:51:00')}
                duration = duration[0]['median_end'] if duration is not None else 'Nem érhető el statisztika'

                graph_rows.append(
                    html.Div(className='monitoring-graph-row', children=[
                        html.Div(className='monitoring-graph-info', children=[
                            html.Table(
                                className='monitoring-graph-info-table',
                                children=[
                                    html.Tr([
                                        html.Th('Unit'),
                                        html.Td(f"{u['unit']}")]),
                                    html.Tr([
                                        html.Th('Sarzs'),
                                        html.Td(f"{charge_no['ChargeNr']}.sarzs")]),
                                    html.Tr(
                                        [html.Th('Recept'),
                                         html.Td(f"{details['recipe_comment']}")]),
                                    html.Tr([
                                        html.Th('Unit recept'),
                                        html.Td(f"{details['unit_recipe_comment']}")]),
                                    html.Tr([
                                        html.Th('Művelet'),
                                        html.Td(f"{details['op_comment']}")]),
                                    html.Tr([
                                        html.Th('Kezdet'),
                                        html.Td(f"{df['TimeStamp'].min()}")]),
                                    html.Tr([
                                        html.Th('Várható befejezés'),
                                        html.Td(f"{duration}")])
                                ]
                            )
                        ]),
                        html.Div(id='monitoring-graph-row-graphs', children=small_graphs)
                    ])
                )

            else:
                df1, stats1 = process_running_unit(u)

                if df1 is None:
                    kezdet= html.Td(f"üres")
                else:
                    kezdet = html.Td(f"{df1['TimeStamp'].min()}")
                table_data.append(html.Tr(
                         [html.Td(u['unit']),
                         html.Td(u['ID']['MR']),
                         html.Td(u['ID']['PARTNO']),
                         html.Td(u['ID']['OPNAME']),
                         html.Td(u['ID']['OPNR']),
                         html.Td(u['ID']['CYCCNT']),
                         kezdet,
                         html.Td(u['ID']['ORDERID']),
                         html.Td(u['ID']['BATCHID']),
                         html.Td(u['ID']['ITEMNR'].strip()),
                         html.Td(u['ID']['CHARGENR'])
                         ]))

        # order by start date of operation + add units with no valid data to the end
        children_with_no_data.append(html.Div(children=[
                    html.H4("Az alábbi műveletek futnak, de nincs beállítva a láthatóságuk"),
                    html.Table(table_data, className='monitoring-notshown-table')
                ]))
        graph_rows = [x for _, x in
                      sorted(zip(process_started, graph_rows), key=lambda pair: pair[0])] + children_with_no_data
        return html.Div(graph_rows)
    else:
        return html.Div([html.H4("Jelenleg nem fut ábrázolandó folyamat.")])


# @app.callback(
#     [Output('tabs-main-kemia', 'style'),
#      Output('mon-sub-pages', 'style'),
#      Output('navbar', 'style'),
#      Output('button-toggle-navbar-kemia', 'style'),
#      Output('button-toggle-navbar-kemia', 'children'),
#      Output('button-monitoring-refresh-kemia', 'style')],
#     [Input('button-toggle-navbar-kemia', 'n_clicks')]
# )
# def toggle_headers_monitoring(n_clicks: int) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str], Dict[str, str],
#                                                       str, Dict[str, str]]:
#     if n_clicks % 2:
#         hide = {'display': 'none'}
#         return hide, hide, hide, {'top': '0'}, 'Fejléc megjelenítés', {'top': '0'}
#     return {}, {}, {}, {}, 'Fejléc elrejtés', {}


def get_running_units(api, get_paused_suspended=False):
    """Return data on currently operating autoclaves.

    Currently operating autoclaves are defined as units that are operating in the most recent lines in the
    'operation_p' tables. Running units can be filtered on e.g. the contents of BatchID.

    :param api: Specifies the table to be used (possible values: 'i' and '2'
    :param get_paused_suspended: If true, operations in state PAUSED and SUSPEND are also listed along with RUNNING.

    :return operating_units: dictionary containing data about currently running units.
        Contains: DB table name, unit number, status (RUNNING, PAUSED, SUSPEND),
        values that identify the operation and timestamp.
    """
    if api == 'kemia1':
        api12 = 1
    elif api == 'kemia2':
        api12 = 2
    else:
        api12 = 3

    operating_units = list()

    if api12 == 1:
        table = f"EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view"
    elif api12 == 2:
        table = f"EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view"
    else:
        table = f"EFR_24EPFIR.k2_24epfir_batch_data_operation_p_tunde_view"

    # get most recent records in table
    sql_query_from_records_kemia = sql.query_from_records_kemia.format(table=table)
    data = utils.run_query(sql_query_from_records_kemia)
    if data is None:
        return []
    df = pd.DataFrame.from_records(data)

    # list of units (currently only 3 digit)
    # to include 2 digit units, add this: [c[1:3] for c in df.columns if re.match("^U[0-9][0-9]_BSTS", c)]

    # units = [c[1:4] for c in df.columns if re.match("^U[0-9][0-9][0-9]_BSTS", c)]

    # loop over units, and collect information on currently running operations
    if not get_paused_suspended:
        running_unit_data = [row['Unit'] for idx, row in df.iterrows() if row['State'] == 'Running']
    else:
        running_unit_data = [row['Unit'] for idx, row in df.iterrows() if row['State'] != "END"]

    running_unit_data = [c[1:4] for c in running_unit_data]

    # store data on units in a list of dictionaries
    for idx, unit in enumerate(running_unit_data):
        recipe_num = df['RecipeNr'].values[idx]
        unitrecipe_num =df['UnitRecipeNr'].values[idx]
        op_type = df['OperationName'].values[idx].strip()
        op_inst = df['OperationType'].values[idx]
        sql_query_dash_visible = sql.query_dash_visible_kemia.format(api=api12, recipe_num=recipe_num,
                                                                     unit_recipe_num=unitrecipe_num, op_type=op_inst,
                                                                     op_inst=op_type)
        dash_visible = utils.run_query(sql_query_dash_visible)
        operating_units.append(dict(unit=unit,
                                    status=df['State'].values[idx],
                                    source=table,
                                    ID=dict(BATCHID=df['BatchID'].values[idx],
                                            MR=recipe_num,
                                            PARTNO=unitrecipe_num,
                                            OPNAME=op_type,
                                            OPNR=op_inst,
                                            OPSTRDT=df['OperationStart'].values[idx],
                                            ORDERID=df['OrderID'].values[idx],
                                            CYCCNT=df['CycleCount'].values[idx],
                                            ITEMNR=df['ItemNr'].values[idx],
                                            CHARGENR=df['sarzsszám'].values[idx]),
                                    timestamp=df["TimeStamp"].values[idx],
                                    plot=False if dash_visible is None else True))
    return operating_units


def process_running_unit(u):
    """ Process data of a currently running unit. Return dataframe which contains three sensor data and
    status (RUNNING, PAUSED, SUSPEND) since the start of the operation.

    Practically: this function is used on elements of the list returned by get_running_units().

    :param u: dictionary containing data of currently running unit
        (an element of the list returned by get_running_units)

    :return df: dataframe containing unit status, timestamp and sensor values.
    :return four_stats: list with the names of the sensor value columns.
    """

    # df containing minutes (1 minute = 1 row) since start of operation and status (RUNNING, PAUSED, SUSPEND)
    # itt lehet, hogy a RECIPE oszlop helyett az MR oszlopot kéne használni mint recept szám
    span_query = sql.query_span_query_kemia.format(unit=u['unit'], source=u['source'], BATCHID=u['ID']['BATCHID'],
                                              PARTNO=u['ID']['PARTNO'],
                                              OPNAME=u['ID']['OPNAME'], OPNR=u['ID']['OPNR'],
                                              datetime=pd.to_datetime(str(u['timestamp'])).strftime('%Y-%m-%d %H:%M:%S'))

    # three stat names, substitute × character for unit number
    four_stats_query = sql.query_four_stats_query_kemia.format(MR=u['ID']['MR'], PARTNO=u['ID']['PARTNO'],
                                                          OPNAME=u['ID']['OPNAME'], OPNR=u['ID']['OPNR'])

    four_stats = utils.run_query(four_stats_query)

    if u['source'] == 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view':
        source = u['source'].replace('EFR_18VEGYES.k2_vegyes_batch_data', 'k2_vegyes')
    elif u['source'] == 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view':
        source = u['source'].replace('EFR_18VEGTERMEK.k2_vegtermek_batch_data', 'k2_vegtermek')
    else:
        source = u['source'].replace('EFR_24EPFIR.k2_24epfir_batch_data', 'k2_24epfir')

    available_columns_sql = sql.query_available_columns.format(source=source.replace('_operation', ''), unit=u['unit'])

    available_columns = utils.run_query(available_columns_sql)

    available_columns = [row['COLUMN_NAME'] for row in available_columns
                         if row['COLUMN_NAME'] is not None]

    # available_columns = [name.split('_', 2)[:2] for name in available_columns
    #                      if 'ActValue' in name]
    #
    # available_columns = ['_'.join(name) for name in available_columns]

    if four_stats is not None:
        stats1 = [col + '_ActValue' for col in four_stats[0].values() if
                 col is not None and col + '_ActValue' in available_columns]
        stats2 = [col + '_PresentValue' for col in four_stats[0].values() if
                 col is not None and col + '_PresentValue' in available_columns]
        stats = stats1 + stats2
        stats = list(set(stats))  # deduplikáció
    else:
        stats = []

    # select statistics from sensor table
    if u['source'] == 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view':
        source = u['source'].replace('EFR_18VEGYES.k2_vegyes_batch_data', 'EFR_S0000058.k2_vegyes')
        sensor_query = (sql.query_sensor_query.format(stats=', '.join([''] + stats),
                                                      source=source.replace('_operation', '')))
        # join span of operation with sensor data
        join_query = (sql.query_join_query_kemia.format(unit=u['unit'], stats=', '.join([''] + stats),
                                                        span_query=span_query, sensor_query=sensor_query))
    elif u['source'] == 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view':
        source = u['source'].replace('EFR_18VEGTERMEK.k2_vegtermek_batch_data', 'EFR_S0000057.k2_vegtermek')
        sensor_query = (sql.query_sensor_query.format(stats=', '.join([''] + stats),
                                                      source=source.replace('_operation', '')))
        # join span of operation with sensor data
        join_query = (sql.query_join_query_kemia.format(unit=u['unit'], stats=', '.join([''] + stats),
                                                        span_query=span_query, sensor_query=sensor_query))
    else:
        source = u['source'].replace('_batch_data', '')
        sensor_query = (sql.query_sensor_query_24epfir.format(stats=', '.join([''] + stats),
                                                      source=source.replace('_operation', '')))
        # join span of operation with sensor data
        join_query = (sql.query_join_query_kemia_24epfir.format(unit=u['unit'], stats=', '.join([''] + stats),
                                                        span_query=span_query, sensor_query=sensor_query))

    df = pd.DataFrame.from_records(utils.run_query(join_query))

    # add time series statistics:
    # first, generate an Offset column in df to use as key
    df['Offset'] = ((df['TimeStamp'] - min(df['TimeStamp'])).dt.total_seconds() / 60).astype(int)

    # az épp futó műveletekhez tartozó cikkszámot csak az sap-táblából tudjuk biztosan kivenni
    # mivel a műveleteket tartalmazó view-ban csak a befejeztük után jelennek meg a műveletek

    if u['source'] == 'EFR_18VEGYES.k2_vegyes_batch_data_operation_p_tunde_view':
        table_item = 'EFR_18VEGYES.k2_vegyes_gb_operation_mg_tunde_view'
    elif u['source'] == 'EFR_18VEGTERMEK.k2_vegtermek_batch_data_operation_p_tunde_view':
        table_item = 'EFR_18VEGTERMEK.k2_vegtermek_gb_operation_mg_tunde_view'
    else:
        table_item = 'EFR_24EPFIR.k2_24epfir_gb_operation_mg_tunde_view'

    query_params = {'recipe_no': u['ID']['MR'],
                    'unit_recipe_no': u['ID']['PARTNO'],
                    'op_type': u['ID']['OPNAME'].strip(),
                    'op_inst': u['ID']['OPNR'],
                    'unit': 'U'+u['unit'],
                    'table_item': table_item}

    try:
        itemnr_query = sql.query_itemnr_query_kemia.format(**query_params)
        itemnr = utils.run_query(itemnr_query)
        itemnr = itemnr[0]['ItemNr'].strip()
    except TypeError as e:
        print(f'Error in getting item_num for operation: {str(e)}')
        return None, None
    query_params.update({'item_nr': itemnr})

    # query of ts stat table, for each stat
    for stat_unit in stats:
        query_params.update({'stat_unit': stat_unit})
        ts_query = sql.query_ts_query_kemia.format(**query_params)
        ts_data = utils.run_query(ts_query)

        # merge ts data to df
        if ts_data is not None:
            ts_df = pd.DataFrame.from_records(ts_data)
            df = df.merge(ts_df, on='Offset', how='right')
            df['TimeStamp'] = min(df['TimeStamp']) + pd.to_timedelta(df['Offset'], unit='m')

    return df, stats
