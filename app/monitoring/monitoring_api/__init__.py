import dash_core_components as dcc
import dash_html_components as html

from .. import gen_monitoring_subpage_links


def gen_layout() -> html.Div:
    return html.Div([
        gen_monitoring_subpage_links(__name__),
        html.Div([
            dcc.RadioItems(
                id='api_tab4',
                options=[{'label': 'API 1', 'value': 'i'},
                         {'label': 'API 2', 'value': '2'}],
                value='i',
                labelStyle={'display': 'inline-block'}),
            html.Div(className='flex-filler'),
            html.Button(id='button-monitoring-refresh', children='Frissítés', n_clicks=0),
            html.Button(id='button-toggle-navbar', children='Fejléc elrejtés', n_clicks=0),
        ], className='monitoring-header'),
        dcc.Loading(html.Div(id='monitor-container'), className='monitoring_loading'),
        dcc.Interval(
            id='monitor-interval',
            interval=2 * 6000000 * 2,  # 60000: update every minute
            n_intervals=0
        )
    ], className='tab-content')
