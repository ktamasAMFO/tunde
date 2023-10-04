import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash_table import DataTable

from app.common import utils as utils
from app.statisztikak import gen_stats_subpage_links


def gen_layout() -> html.Div:
    return html.Div([
        gen_stats_subpage_links(__name__),
        html.Div([
            html.Div([
                dbc.Row([
                    html.H2('Futási statisztikák', className='stats-edit-title'),
                    html.Img(src='/assets/info_button.svg', id='info-btn-run_stat1', className='info_button'),
                    dbc.Tooltip(
                        target='info-btn-run_stat1',
                        children='A futási statisztikák a receptek, unitreceptek és műveletek hosszáról és offsetjéről '
                                 'nyújtanak információt. Az alábbi táblázatban láthatóak hogy milyen néven és milyen '
                                 'cikkszámra készültek statisztikák. A default érték azt jelzi, hogy egy adott cikkszámnál '
                                 'mely statisztikát vesszük figyelembe az "Aktuális sarzsok áttekintése" illetve a '
                                 '"Termelési tervek" oldalakon.',
                        style={'fontSize': 18}
                    ),
                ]),
                html.Div([
                    html.Button(id='run-stats-edit-refresh', children='Táblázat frissítése')
                ]),
                html.Div([
                    DataTable(
                        id='run-stats-table',
                        columns=[
                            {'name': 'Id', 'id': 'id', 'type': 'numeric'},
                            {'name': 'Cikkszám', 'id': 'item_nr', 'type': 'text'},
                            {'name': 'Api', 'id': 'api', 'type': 'text'},
                            {'name': 'Név', 'id': 'stat_name', 'type': 'text'},
                            {'name': 'Default', 'id': 'default_stat', 'type': 'numeric'},
                            {'name': 'Állapot', 'id': 'state', 'type': 'text'}
                        ],
                        data=utils.run_query(
                            """select id, stat_name, api, item_nr, state, default_stat from run_stat where (api = '1' or api = '2')
                            """) or [],
                        style_header=utils.table_header_default_style,
                        style_cell=utils.table_cell_default_style,
                        fixed_rows={'headers': True, 'data': 0},
                        style_as_list_view=True,
                        filter_action='native',
                        page_size=50,
                        row_selectable='single'
                    )], className='stats-edit-table')
            ]),
            html.Div([
                dbc.Row([
                    html.H2('Kiválasztott statisztika'),
                    html.Img(src='/assets/info_button.svg', id='info-btn-run_stat2', className= 'info_button'),
                    dbc.Tooltip(
                        target='info-btn-run_stat2',
                        children='Egy már létező statisztikát kiválasztva itt jelenik meg hogy milyen beállításokkal '
                                 'futott le. Ez a statisztika újra is futtatható ha szükséges.',
                        style={'fontSize': 18}
                    )]),
                html.Div([
                    html.Button(id='run-stats-edit-delete', children='Törlés'),
                    html.Button(id='run-stats-edit-default', children='Legyen alapértelmezett'),
                    dbc.Tooltip(target='run-stats-edit-default', children='A kiválasztott statisztikát használ'),
                    html.Button(id='run-stats-edit-rerun', children='Újrafuttatás'),
                    dbc.Tooltip(
                        target='run-stats-edit-rerun',
                        children='Ha egy details tábla tartalma megváltozott (például új sorok kerültek bele), '
                                 'manuálisan újra kell futtatni a statisztikát hogy a változások életbe lépjenek.'
                    ),
                ]),
                html.Div([
                    html.Fieldset([
                        html.Legend('ID', className='stats-legend'),
                        dcc.Input(id='existing-run-stat-id', placeholder='ID', disabled=True, className='full-width')
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Név', className='stats-legend'),
                        dcc.Input(id='existing-run-stat-name', placeholder='Elnevezés', disabled=True,
                                  className='full-width')
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('API', className='stats-legend'),
                        dcc.RadioItems(id='existing-run-stat-api',
                                       className='create-stat-api',
                                       options=[{'label': 'API 1', 'value': '1', 'disabled': True},
                                                {'label': 'API 2', 'value': '2', 'disabled': True}]),
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Cikkszám/Termék', className='stats-legend'),
                        dcc.Dropdown(id='existing-run-stat-itemnr-dropdown', placeholder='Cikkszám',
                                     disabled=True),
                        dcc.Input(id='existing-run-stat-chosen-product', disabled=True)
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('Időszakok', className='stats-legend'),
                        dcc.Checklist(id='existing-run-stat-time-checkboxes')
                    ], className='stats-fieldset col-sm-3'),
                    html.Fieldset([
                        html.Legend('Sarzsok', className='stats-legend'),
                        dcc.Checklist(id='existing-run-stat-charge-checkboxes',
                                      className='create-stat-charge-checkboxes'),
                    ], className='stats-fieldset col-sm-10'),
                ], className='stats-edit-existing-fields')
            ], className='stats-edit-existing'),
            html.Div([
                dbc.Row([
                    html.H2('Új statisztika létrehozása'),
                    html.Img(src='/assets/info_button.svg', id='info-btn-run_stat3', className= 'info_button'),
                    dbc.Tooltip(
                        target='info-btn-run_stat3',
                        children='Új statisztika létrehozásához töltse ki az alábbi mezőket és kattintson a '
                                 '"Statisztika létrehozása" gombra.',
                        style={'fontSize': 18}
                    ),
                ]),
                html.Div([
                    html.Button(id='create-run-stat-new', children='Statisztika létrehozása'),
                    html.Button(id='delete-run-stat-new', children='Tartalom ürítése')
                ]),
                html.Div([
                    html.Div(id='alert', children=[]),
                    html.Fieldset([
                        html.Legend('Név', className='stats-legend'),
                        dcc.Input(id='create-run-stat-name', placeholder='Elnevezés', className='full-width')
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('API', className='stats-legend'),
                        dcc.RadioItems(id='create-run-stat-api',
                                       className='create-stat-api',
                                       options=[{'label': 'API 1', 'value': '1'},
                                                {'label': 'API 2', 'value': '2'}],
                                       ),
                    ], className='stats-fieldset col-sm-2'),
                    html.Fieldset([
                        html.Legend('Cikkszám/Termék', className='stats-legend'),
                        dcc.Dropdown(id='create-run-stat-itemnr-dropdown', placeholder='Cikkszám'),
                        dcc.Input(id='create-run-stat-chosen-product', disabled=True)
                    ], className='stats-fieldset col-sm-5'),
                    html.Fieldset([
                        html.Legend('Időszakok', className='stats-legend'),
                        dcc.Checklist(id='create-run-stat-time-checkboxes')
                    ], className='stats-fieldset col-sm-3'),
                    html.Fieldset([
                        html.Legend('Sarzsok', className='stats-legend'),
                        html.Button(id='create-run-stat-all-charges', children='Összes'),
                        dcc.Checklist(id='create-run-stat-charge-checkboxes',
                                      className='create-stat-charge-checkboxes'),
                    ], className='stats-fieldset col-sm-10'),
                ], className='stats-edit-create-fields')
            ], className='stats-edit-create')
        ], className='stats-edit-content', id='run_stats-container')
    ], className='stats_div')
