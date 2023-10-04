import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from .. import gen_now_running_data_subpage_links


def gen_layout() -> html.Div:
    return html.Div([
        gen_now_running_data_subpage_links(__name__),
        dbc.Row([
            dbc.Col([
                html.Img(src='/assets/info_button.svg', id='info-btn-epp-futo')],
                width='auto'),
            dbc.Col(
                dcc.RadioItems(id='kemia_tab3',
                               options=[{'label': 'Vegyes', 'value': 'kemiai'},
                                        {'label': 'Végtermék', 'value': 'kemia2'},
                                        {'label': '24epfir', 'value': 'kemia3'}],
                               value='kemiai',
                               labelStyle={'display': 'inline-block'}),
                width='auto')
        ]),
        dbc.Row([
            html.Div(
                dcc.Loading([
                    dcc.Graph(id='futtatasok-varhato-kemia')
                ])
                , className="six columns graph-container-md", id='futtatasok-varhato-box'),
            html.Div(
                dcc.Loading([
                    dcc.Graph(id='sarzs-varhato-kemia',
                              figure={'data': [{'name': 'Dummy'}]})
                ], id='sarzs-varhato-kemia-container')
                , className="six columns graph-container-md", id='sarzs-varhato-box')
        ]),
        html.Div([
            html.Br(), dcc.Checklist(id='only-data-kemia', options=[
                {'label': 'Csak a szenzoradatokkal rendelkező műveletek mutatása', 'value': 'custom'}])
        ], style={'text-align': 'center'}),
        dbc.Row([
            html.Div(
                dcc.Loading([
                    dcc.Graph(id='recept-varhato-kemia',
                              figure={'data': [{'name': 'Dummy'}]})
                ], id='recept-varhato-kemia-container')
                , className="six columns graph-container-md", id='recept-varhato-box'),
            html.Div(
                dcc.Loading([
                    dcc.Graph(id='muvelet-varhato-kemia')
                ], id='muvelet-varhato-kemia-container')
                , className="six columns graph-container-md", id='muvelet-varhato-box')
        ], className="row"),
        dbc.Row(
            [dbc.Col([
                html.Div([
                    dcc.Dropdown(
                        id='futtatasok-sensor-plot-dropdown-kemia',
                        disabled=True,
                        placeholder="Válasszon ki egy műveletet",
                        clearable=False,
                        className='Dropdown'
                    )
                ]),
                html.Div([
                    html.H4(['Y skála ', html.Img(src='/assets/info_button.svg', id='futtatasok-yscale-info',
                                                  className='info-img-sm')]),
                    dcc.RadioItems(id='futtatasok-yaxis-scale-kemia',
                                   options=[{'label': 'Automatikus', 'value': 'auto'},
                                            {'label': 'Manuális', 'value': 'manual'}],
                                   value='auto',
                                   labelStyle={'display': 'inline-block'}),
                    html.Label(['Min ', dcc.Input(id='futtatasok-yscale-min-kemia', type='number', placeholder='min')]),
                    html.Label(['Max ', dcc.Input(id="futtatasok-yscale-max-kemia", type='number', placeholder='max')])
                ]),
                html.Div([
                    html.Br(),
                    html.Button(id='futtatasok-sensor-plot-button-kemia', children='Plot')
                ], style={'position': 'relative'}),
                html.Div([
                    html.H4('Függőleges tengelyen ábrázolandó mennyiség'),
                    dcc.RadioItems(id='futtatasok-plot-deviation-kemia',
                                   options=[{'label': 'Valódi érték', 'value': 'absolute'},
                                            {'label': 'Eltérés a mediántól', 'value': 'relative'}],
                                   value='absolute',
                                   labelStyle={'display': 'inline-block'})]),
                html.Div([
                    html.H4('Szórás típusa'),
                    dcc.RadioItems(id='futtatasok-plot-deviation-type-kemia',
                                   options=[{'label': 'Medián és IQR', 'value': '1'},
                                            {'label': 'Átlag és szórás', 'value': '0'}],
                                   value='1',
                                   labelStyle={'display': 'inline-block'})

                ]),
            ], width=2),
                dbc.Col([
                    dcc.Loading([
                        dcc.Graph(id='futtatasok-sensor-plot-kemia', figure={'data': [{'name': 'Dummy'}]})
                    ], style={
                        'display': 'flex',
                        'align-items': 'center',
                        'justify-content': 'center'}),
                ], className='graph-container-full', width=10)
            ]),
        dbc.Tooltip("Y skála beállítása. "
                    "Alapértelmezetten automatikusan állítja be. "
                    "Manuális esetben adja meg a min és max értékeket!", target='futtatasok-yscale-info',
                    style={'fontSize': 18}),
        dbc.Tooltip("Ezen az oldalon négy Gantt-diagramot láthatunk sarzs, recept, unit recept, művelet sorrendben "
                    "Az első két ábra automatikusan kirajzolódik és megmutatja az épp futó összes sarzsot és "
                    "receptet a Kémia1-ből, Kémia2-ből vagy Kémia3-ból.",
                    target='info-btn-epp-futo',
                    style={'fontSize': 18}),
        dbc.Tooltip('Az épp futó sarzsok Gantt-diagramja', target='futtatasok-varhato-box', style={'fontSize': 18}),
        dbc.Tooltip("A kiválasztott sarzsszámmal rendelkező receptek. "
                    "Kattintson az ábra valamely receptjére!", target='sarzs-varhato-box', style={'fontSize': 18}),
        dbc.Tooltip("A kiválasztott recepthez tartozó unit receptek. "
                    "Kattintson az ábra valamely unit receptjére!", target='recept-varhato-box',
                    style={'fontSize': 18}),
        dbc.Tooltip("A kiválasztott unit recepthez tartozó műveletek. ", target='muvelet-varhato-box',
                    style={'fontSize': 18}),
    ], className='container-fluid', id='epp_futo-container')
