from datetime import datetime as dt

import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from .. import gen_monitoring_subpage_links

def gen_layout() -> html.Div:
    return html.Div([
         gen_monitoring_subpage_links(__name__),
         html.Div('Épp futó műveletek - Karbantart'
                  ' Az oldal nem elérhető')
    ])