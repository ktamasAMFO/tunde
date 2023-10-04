import dash_core_components as dcc
import dash_html_components as html

from .. import gen_monitoring_subpage_links


def gen_layout() -> html.Div:
    return html.Div([
        gen_monitoring_subpage_links(__name__),
        html.Div([
            dcc.RadioItems(
                id='kemia_tab4',
                options=[{'label': 'Vegyes', 'value': 'kemia1'},
                         {'label': 'Végtermék', 'value': 'kemia2'},
                         {'label': '24epfir', 'value': 'kemia3'}],
                value='kemia1',
                labelStyle={'display': 'inline-block'}),
            html.Div(className='flex-filler'),
            html.Button(id='button-monitoring-refresh', children='Frissítés', n_clicks=0),
            html.Button(id='button-toggle-navbar', children='Fejléc elrejtés', n_clicks=0),
        ], className='monitoring-header'),
        dcc.Loading(html.Div(id='monitor-container-kemia'), className='monitoring_loading'),
        dcc.Interval(
            id='monitor-interval-kemia',
            interval=2 * 6000000 * 2,  # 60000: update every minute
            n_intervals=0
        )
    ], className='tab-content')
