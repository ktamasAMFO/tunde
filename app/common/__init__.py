import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

from . import utils


def gen_layout_header(env):
    title = 'Termelő Üzemek Numerikus aDatainak Elemzése' + ('' if env == 'prod' else ' - teszt környezet')
    return [
        dcc.Location(id='url', refresh=False),
        html.Nav(id='navbar', className='navbar' if env == 'prod' else 'navbar test', children=[

            html.Div(id='pageTitle', children=title),
            html.Div(id='pageVersion', children='v0.14.0')
        ])
    ]


def prepend_main_navlinks(layout_gen_func, selected_tab):
    return [utils.module_subpage_links('app', selected_tab), layout_gen_func()]
