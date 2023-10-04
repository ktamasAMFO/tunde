from importlib import import_module

import dash_html_components as html
from dash.dependencies import Output, Input
from dash.exceptions import PreventUpdate
from flask_login import current_user

from app import app
from app import gen_layout as gen_home_layout
from app.common import utils
from app.common import prepend_main_navlinks


@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')])
def display_page(pathname: str):
    if pathname is None:
        raise PreventUpdate

    df = utils.pages_with_abs_path()
    valid_pages = df[df['page_path_absolute'] == pathname.lstrip('/')]
    if not valid_pages.empty:
        valid_page = valid_pages.iloc[0]

        # check for permission
        access_pages = []
        access_pages.append({'page_id': 1})  # a főoldalt mindenki láthatja
        access_pages.append({'page_id': 4})
        access_pages.append({'page_id': 5})
        access_pages.append({'page_id': 23})
        access_pages.append({'page_id': 24})
        access_pages.append({'page_id': 25})
        access_pages.append({'page_id': 26})
        access_pages.append({'page_id': 27})
        access_pages.append({'page_id': 7})
        access_pages.append({'page_id': 11})
        access_pages.append({'page_id': 12})
        access_pages.append({'page_id': 28})
        access_pages.append({'page_id': 29})
        access_page_ids = [page['page_id'] for page in access_pages]
        if valid_page['page_id'] in access_page_ids:
            m = import_module(valid_page['layout_module'])
            layout_fn = getattr(m, 'gen_layout')
            return prepend_main_navlinks(layout_fn, valid_page['layout_module'])

    if pathname in ['/']:
        return prepend_main_navlinks(gen_home_layout, '')

    # else show 404 page


@app.callback(
    Output('user-name', 'children'),
    [Input('page-content', 'children')])
def display_cur_user(_):
    if current_user.is_authenticated:
        return current_user.id
    else:
        return ''


@app.callback(
    [Output('logout', 'children'),
     Output('logout', 'style')],
    [Input('page-content', 'children')])
def display_user_logout_btn(_):
    if current_user.is_authenticated:
        return html.I(className='ti-power-off logout-icon'), {}
    else:
        return '', {'display': 'none'}
