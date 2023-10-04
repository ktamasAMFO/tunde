import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash_table import DataTable

from app.common import utils as utils
from .. import gen_stats_subpage_links


def gen_layout() -> html.Div:
    return html.Div([
        gen_stats_subpage_links(__name__),
        html.Div([
            html.Div([
                dbc.Row([
                    html.H2('Szenzor statisztikák', className='stats-edit-title'),
                    html.Img(src='/assets/info_button.svg', id='info-btn-ts_stat1', className= 'info_button'),
                    dbc.Tooltip(
                        target='info-btn-ts_stat1',
                        children='A szenzor statisztikák az egyes műveletek futása alatti szenzorértékekről '
                                 'nyújtanak információt. Az alábbi táblázatban láthatóak hogy milyen néven és milyen '
                                 'cikkszámra készültek statisztikák. Egy cikkszámhoz egyetlen statisztika tartozik, '
                                 'egy új statisztika készítésével felülíródik az előző azonos cikkszámra készített '
                                 'statisztika.',
                        style={'fontSize': 18})
                ]),
                html.Div([
                    html.Button(id='ts-stats-edit-refresh', children='Táblázat frissítése')
                ]),
                html.Div([
                    DataTable(
                        id='ts-stats-table',
                        columns=[
                            {'name': 'Id', 'id': 'id', 'type': 'numeric'},
                            {'name': 'Cikkszám', 'id': 'item_nr', 'type': 'text'},
                            {'name': 'Api', 'id': 'api', 'type': 'text'},
                            {'name': 'Név', 'id': 'stat_name', 'type': 'text'},
                            {'name': 'Default', 'id': 'default_stat', 'type': 'numeric'},
                            {'name': 'Állapot', 'id': 'state', 'type': 'text'},
                            {'name': 'Medián és IQR', 'id': 'use_median_iqr', 'type': 'numeric'},
                            {'name': 'IQR szorzó', 'id': 'deviation_multiplier_iqr', 'type': 'numeric'},
                            {'name': 'STD szorzó', 'id': 'deviation_multiplier_std', 'type': 'numeric'}
                        ],
                        data=utils.run_query(
                            'select id, stat_name, api, item_nr, state, default_stat, '
                            '       use_median_iqr, deviation_multiplier_iqr, deviation_multiplier_std from ts_stat') or [],
                        style_header=dict(utils.table_header_default_style, **{'padding': '12px 8px'}),
                        style_cell=dict(utils.table_cell_default_style, **{'padding': '12px 8px'}),
                        style_cell_conditional=utils.set_table_col_widths({
                            'id': '25px', 'item_nr': '55px', 'api': '25px', 'stat_name': '65px', 'default_stat': '55px',
                            'state': '115px'}),
                        fixed_rows={'headers': True, 'data': 0},
                        style_as_list_view=True,
                        filter_action='native',
                        page_size=50,
                        row_selectable='single'
                    )
                ], className='stats-edit-table')
            ]),
            html.Div([
                dbc.Row([
                    html.H2('Kiválasztott statisztika'),
                    html.Img(src='/assets/info_button.svg', id='info-btn-ts_stat2', className= 'info_button'),
                    dbc.Tooltip(
                        target='info-btn-ts_stat2',
                        children='Egy már létező statisztikát kiválasztva itt jelenik meg hogy milyen beállításokkal '
                                 'futott le. Ez a statisztika újra is futtatható ha szükséges, illetve néhány '
                                 'paraméter újrafuttatás nélkül megváltoztatható.',
                        style={'fontSize': 18})
                ]),
                html.Div([
                    html.Button(id='ts-stats-edit-delete', children='Törlés'),
                    html.Button(id='ts-stats-edit-rerun', children='Újrafuttatás'),
                    dbc.Tooltip(
                        target='ts-stats-edit-rerun',
                        children='Ha a details táblában megváltozott, hogy milyen szenzorokra legyenek statisztikák '
                                 'egy-egy műveletnél, manuálisan újra kell futtatni a statisztikát hogy a változások'
                                 'életbe lépjenek',
                        style={'fontSize': 18}
                    ),
                    html.Button(id='ts-stats-edit-default', children='Legyen alapértelmezett'),
                    dbc.Tooltip(
                        target='ts-stats-edit-default',
                        children='A kiválasztott statisztika legyen az alapértelmezett, amit a monitoring oldal használ '
                                 'amikor adott cikkszámú művelet fut',
                        style={'fontSize': 18}
                    ),
                    html.Button(id='ts-stats-edit-change', children='Változtatás'),
                    dbc.Tooltip(
                        target='ts-stats-edit-change',
                        children='A statisztika azon jellemzőinek megváltoztatása, amihez nem szükséges újra '
                                 'lefuttatni a statisztika számolást (mérőszám, szorzók)',
                        style={'fontSize': 18}
                    )
                ], id='ts-stat-edit-btns'),
                html.Div([
                    html.Fieldset([
                        html.Legend('ID', className='stats-legend'),
                        dcc.Input(id='existing-ts-stat-id', placeholder='ID', disabled=True, className='full-width')
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Név', className='stats-legend'),
                        dcc.Input(id='existing-ts-stat-name', placeholder='Elnevezés', disabled=True,
                                  className='full-width')
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('API', className='stats-legend'),
                        dcc.RadioItems(id='existing-ts-stat-api',
                                       className='create-stat-api',
                                       options=[{'label': 'API 1', 'value': '1', 'disabled': True},
                                                {'label': 'API 2', 'value': '2', 'disabled': True}]),
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Monitoring mérőszámok', className='stats-legend'),
                        dcc.RadioItems(id='existing-ts-stat-median_iqr',
                                       className='create-stat-api',
                                       options=[{'label': 'Medián és IQR', 'value': '1'},
                                                {'label': 'Átlag és szórás', 'value': '0'}]),
                    ], className='stats-fieldset col-sm-4'),
                    html.Fieldset([
                        html.Legend('IQR szorzó', className='stats-legend'),
                        dcc.Input(id='existing-ts-stat-multiplier-iqr', placeholder='Szorzó')
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('STD szorzó', className='stats-legend'),
                        dcc.Input(id='existing-ts-stat-multiplier-std', placeholder='Szorzó')
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Cikkszám/Termék', className='stats-legend'),
                        dcc.Dropdown(id='existing-ts-stat-itemnr-dropdown', placeholder='Cikkszám', disabled=True),
                        dcc.Input(id='existing-ts-stat-chosen-product', disabled=True)
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('Időszakok', className='stats-legend'),
                        dcc.Checklist(id='existing-ts-stat-time-checkboxes')
                    ], className='stats-fieldset col-sm-3'),
                    html.Fieldset([
                        html.Legend('Sarzsok', className='stats-legend'),
                        dcc.Checklist(id='existing-ts-stat-charge-checkboxes',
                                      className='create-stat-charge-checkboxes'),
                    ], className='stats-fieldset col-sm-10'),
                ], className='stats-edit-existing-fields')
            ], className='stats-edit-existing'),
            html.Div([
                dbc.Row([
                    html.H2('Új statisztika létrehozása'),
                    html.Img(src='/assets/info_button.svg', id='info-btn-ts_stat3', className= 'info_button'),
                    dbc.Tooltip(
                        target='info-btn-ts_stat3',
                        children='Új statisztika létrehozásához töltse ki az alábbi mezőket és kattintson a '
                                 '"Statisztika létrehozása" gombra.',
                        style={'fontSize': 18}
                    ),
                ]),
                html.Div([
                    html.Button(id='create-ts-stat-new', children='Statisztika létrehozása'),
                    html.Button(id='delete-ts-stat-new', children='Tartalom ürítése')
                ]),
                html.Div([
                    html.Div(id='alert_ts', children=[]),
                    html.Fieldset([
                        html.Legend('Név', className='stats-legend'),
                        dcc.Input(id='create-ts-stat-name', placeholder='Elnevezés', className='full-width')
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('API', className='stats-legend'),
                        dcc.RadioItems(id='create-ts-stat-api',
                                       className='create-stat-api',
                                       options=[{'label': 'API 1', 'value': '1'},
                                                {'label': 'API 2', 'value': '2'}]),
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Monitoring mérőszámok', className='stats-legend'),
                        dcc.RadioItems(id='create-ts-stat-median_iqr',
                                       className='create-stat-api',
                                       options=[{'label': 'Medián és IQR', 'value': '1'},
                                                {'label': 'Átlag és szórás', 'value': '0'}]),
                    ], className='stats-fieldset col-sm-4'),
                    html.Fieldset([
                        html.Legend('IQR szorzó', className='stats-legend'),
                        dcc.Input(id='create-ts-stat-multiplier-iqr', placeholder='Szorzó')
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('STD szorzó', className='stats-legend'),
                        dcc.Input(id='create-ts-stat-multiplier-std', placeholder='Szorzó')
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Cikkszám/Termék', className='stats-legend'),
                        dcc.Dropdown(id='create-ts-stat-itemnr-dropdown', placeholder='Cikkszám'),
                        dcc.Input(id='create-ts-stat-chosen-product', disabled=True)
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('Időszakok', className='stats-legend'),
                        dcc.Checklist(id='create-ts-stat-time-checkboxes')
                    ], className='stats-fieldset col-sm-3'),
                    html.Fieldset([
                        html.Legend('Sarzsok', className='stats-legend'),
                        html.Button(id='create-ts-stat-all-charges', children='Összes'),
                        dcc.Checklist(id='create-ts-stat-charge-checkboxes',
                                      className='create-stat-charge-checkboxes'),
                    ], className='stats-fieldset col-sm-10'),
                ], className='stats-edit-create-fields')
            ], className='stats-edit-create')
        ], className='stats-edit-content', id='ts_stats-container')
    ], className='stats_div')
