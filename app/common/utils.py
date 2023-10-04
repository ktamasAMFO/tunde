import configparser
import logging.handlers
import re
import time
from datetime import datetime as dt
from pathlib import Path
from typing import Any, Optional, Dict, List, Union, Callable, Tuple

import pandas as pd
from dash import callback_context
from plotly.graph_objs import Figure
import dash_core_components as dcc
import dash_html_components as html
from flask_login import current_user
import pyodbc


# custom types
Click_data = Optional[Dict[str, List[Dict[str, int]]]]
Table_data = List[Dict[str, Union[str, int]]]
Options = List[Dict[str, str]]

column_map_autoklav = {'Dátum': 'Dátum',
                       'TIT{num}CU_11': '{num} belső, felső hőmérséklet [C]',
                       'TT{num}CU_12': '{num} belső, alsó hőmérséklet [C]',
                       'TT{num}CU_13': '{num} belső, középső hőmérséklet [C]',
                       'WY{num}CU_61N': '{num} töltet tömeg [kg]',
                       'TT{num}JM_11': '{num} köpeny be hőmérséklet [C]',
                       'TT{num}JM_12': '{num} köpeny ki hőmérséklet [C]',
                       'TV{num}JM_11': '{num} szabályozó szelep [%]',
                       'LIT{num}CU_61': '{num} töltet szint [l]',
                       'VSZ{num}JM_31': '{num} fűtési állapot',
                       'TIT{num}CU_13': '{num} TIT{num}CU_13',
                       'VSZ{num}JM_11': '{num} VSZ{num}JM_11',
                       'VSZ{num}JM_21': '{num} VSZ{num}JM_21',
                       'k1_apimppi_p.ramp': '{num} k1_apimppi_p.ramp'
                       }

column_map_calculated = {'CF{num}JM': '{num} Térfogatáram [m^3/s]',
                         'CQB{num}CU': '{num} Töltet hőteljesítmény [kW]',
                         'CQM{num}JM': '{num} Monofluid hőteljesítmény [kW]',
                         'CT{num}JM_MIN': '{num} Min hőmérséklet max térfogatáram [m^3/s]',
                         'CT{num}JM_IC': '{num} tengelymetszet',
                         'CT{num}JM_SL': '{num} meredekség'}

column_map_fluid = {'Dátum': 'Dátum',
                    'TIT863_11': 'meleg olaj bemenő hőmérséklet [C]',  # most ez kell
                    'TIT864_11': 'közepes olaj bemenő hőmérséklet [C]',
                    'TIT864_12': 'közepes olaj bemenő hőmérséklet 2 [C]',
                    'TIT865_11': 'hideg olaj  hőmérséklet [C]'}

autoklav_nums = [171, 172, 174, 175, 182, 183, 271, 272, 273, 274]
calculated_column_names = ['{num} Térfogatáram [m^3/s]',
                           '{num} Töltet hőteljesítmény [kW]',
                           '{num} Monofluid hőteljesítmény [kW]',
                           '{num} Min hőmérséklet max térfogatáram [m^3/s]']
debug = True

table_cell_default_style = {
    'padding': '12px 16px',
    'borderBottom': '1px solid #dee2e6',
    'borderTop': '1px solid #dee2e6',
    'textAlign': 'center',
    'fontFamily': 'Open Sans',
    'fontSize': '13px',
    'color': '#525f7f',
    'backgroundColor': 'rgba(0, 0, 0, 0)',
    'minWidth': '0px',
    'maxWidth': '150px'
}

table_header_default_style = {
    'padding': '12px 16px',
    'textTransform': 'uppercase',
    'borderBottom': '1px solid #dee2e6',
    'borderTop': '1px solid #dee2e6',
    'fontFamily': 'Open Sans',
    'fontSize': '10px',
    'fontWeight': 600,
    'letterSpacing': '1px',
    'color': '#525f7f',
    'backgroundColor': 'rgba(0, 0, 0, 0.05)',
    'whiteSpace': 'normal'
}


def style_cell_conditional(col_id: str, attrs: Dict[str, str]):
    style_dict = {'if': {'column_id': col_id}}
    style_dict.update(attrs)
    return style_dict


def set_table_col_widths(id_width: Dict[str, str]):
    styles = [style_cell_conditional(id, {'width': width}) for
              id, width in id_width.items()]
    return styles


def timeit(method: Callable) -> Callable:
    def timed(*args, **kw):  # type: ignore
        if not debug:
            return method(*args, **kw)
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result

    return timed


def parse_config() -> configparser.RawConfigParser:
    config = configparser.ConfigParser()
    config.read('app/config.ini')
    if config['environment']['env'] not in ('prod', 'test'):
        config['database']['enet-schema'] = 'enet_dev'
    return config


# def connect_to_db() -> MySQLdb.Connection:
#     cfg = parse_config()
#     db_cfg = cfg['database']
#     return MySQLdb.Connect(host=db_cfg['host'],
#                            port=int(db_cfg.get('port', '3306')),
#                            user=db_cfg['user'],
#                            password=db_cfg['password'],
#                            db=db_cfg['enet-schema'],
#                            charset='utf8',
#                            cursorclass=DictCursor)

def connect_to_db():
    server = 'hun-dev-claudia-psql01.database.windows.net'
    database = 'HUN-dev-claudia-Pdb01'
    username = 'tunde_dev_app'
    password = '{70afrwot6KGthd3T2QWbzwkE}'
    driver = '{ODBC Driver 18 for SQL Server}'

    with pyodbc.connect(
            'DRIVER=' + driver + ';SERVER=tcp:' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password) as conn:
        with conn.cursor() as cursor:
            return conn, cursor
          
def run_query_new(query: str,
              query_params: Optional[Union[Dict[str, Union[str, int]], List[Union[str, int]]]] = None
              ) -> Optional[List[Dict[str, Any]]]:
    conn, cursor = connect_to_db()
    try:
        if query_params is not None:
            cursor.execute(query, query_params)
        else:
            cursor.execute(query)
        # Check if the query is a SELECT statement
        if query.strip().upper().startswith("SELECT"):
            columnNames = [column[0] for column in cursor.description]
            results = [] 

            for record in cursor.fetchall():
                results.append(dict(zip(columnNames, record)))

            conn.commit()

            if results:
                return results
            else:
                return None
        else:
            conn.commit()
            return None  # or some other appropriate value/feedback for non-SELECT queries

    finally:
        cursor.close()
        conn.close()
        

def run_query(query: str,
              query_params: Optional[Union[Dict[str, Union[str, int]], List[Union[str, int]]]] = None
              ) -> Optional[List[Dict[str, Any]]]:
    # conn = connect_to_db()
    # cursor = conn.cursor()
    conn, cursor = connect_to_db()
    try:
        if query_params is not None:
            cursor.execute(query, query_params)
        else:
            cursor.execute(query)

        columnNames = [column[0] for column in cursor.description]
        results = []

        for record in cursor.fetchall():
            results.append(dict(zip(columnNames, record)))
        # print(results)
        conn.commit()
        if results:
            return results  # for now we only return the last result set
        else:
            return None
    finally:
        cursor.close()
        conn.close()


# def run_query(query: str,
#               query_params: Optional[Union[Dict[str, Union[str, int]], List[Union[str, int]]]] = None
#               ) -> Optional[List[Dict[str, Any]]]:
#     conn = connect_to_db()
#     cursor = conn.cursor()
#     try:
#         cursor.execute(query, query_params)
#     except MySQLdb.MySQLError as e:
#         conn.rollback()
#         # from ... import app
#         # app.app.logger.error(f'database error during query execution: {str(e)} - query: {query} - query_params: {query_params}')
#         raise
#     else:
#         # néha a cursor.nextset() hívásnál száll el a query
#         # például inserteknél, amikor kulcs duplikáció lép fel
#         try:
#             results = [cursor.fetchall()]
#             while cursor.nextset():
#                 results.append(cursor.fetchall())
#             results = [res for res in results if res != ()]  # discard the not empty results
#
#             conn.commit()
#             if results:
#                 return list(results[-1])  # for now we only return the last result set
#             else:
#                 return None
#         except MySQLdb.MySQLError as e:
#             conn.rollback()
#             query_oneline = query.replace('\n', ' ')
#             message = (f'database error during query execution: {str(e)} - '
#                        f'query: {query_oneline} - '
#                        f'query_params: {query_params}')
#             # from app import app
#             # app.logger.error(message)
#             raise
#     finally:
#         cursor.close()
#         conn.close()


def insert_df(df: pd.DataFrame, delete: bool = False) -> None:
    # the columns of the dataframe are the columns of the DB table
    # the name of the dataframe is the table name
    # handles null values well, but too many (>1000) rows can cause problems
    df_str_cols = df.select_dtypes(include='object')
    # kell az astype(str), mert annak ellenére, hogy az object típusú oszlopokkal dolgozunk, lehetnek nem string értékek
    df.loc[:, df_str_cols.columns] = df_str_cols.astype(str).apply(lambda x: "'" + x + "'", axis=1)
    df_str = df.astype(str)
    df_str.name = df.name
    insert = f'insert into {df_str.name} ' + '(' + ', '.join(df_str.columns) + ')' + ' values '
    insert_rows_values = '(' + df_str.apply(', '.join, axis=1) + ')'
    insert_vals = insert_rows_values.str.cat(sep=', ')
    insert_vals = insert_vals.replace("'NULL'", "NULL")
    if delete:
        delete_insert = f'delete from {df_str.name};' + insert + insert_vals
        run_query(delete_insert)
    else:
        run_query(insert + insert_vals)


def load_formula_data_from_db(table, start_date, end_date):
    query = f'select * from {table} where Start > %(start_date)s and End < %(end_date)s'
    query_params = {'start_date': start_date,
                    'end_date': end_date}
    # conn = connect_to_db('localhost', 'root', 'autoklav', 'autoklav', 'utf8', DictCursor)
    data = run_query(query, query_params)
    return pd.DataFrame.from_records(data)


def find_cols(dataframe, regex):
    """Return a list of columns that match the given regex."""
    r = re.compile(regex, )
    return list(filter(r.match, dataframe.columns))


def drop_unnecessary_cols(dataframe, needed_cols):
    dataframe.drop(labels=set(dataframe.columns) - set(needed_cols), axis=1, inplace=True)


def load_quantity_column(column_names, autoclave_num):
    """Depending on the equipment, sometimes different sensors are measuring the same quantity"""
    if f'{autoclave_num} töltet szint [l]' in column_names:
        return f'{autoclave_num} töltet szint [l]'
    elif f'{autoclave_num} töltet tömeg [kg]' in column_names:
        return f'{autoclave_num} töltet tömeg [kg]'
    elif f'LIT{autoclave_num}CU_61' in column_names:
        return f'LIT{autoclave_num}CU_61'
    elif f'WY{autoclave_num}CU_61N' in column_names:
        return f'WY{autoclave_num}CU_61N'
    else:
        raise ValueError


def internal_temp_column(column_names, autoclave_num):
    """Depending on the equipment, sometimes different sensors are measuring the same quantity"""
    if f'{autoclave_num} belső, felső hőmérséklet [C]' in column_names:
        return f'{autoclave_num} belső, felső hőmérséklet [C]'
    elif f'{autoclave_num} belső, középső hőmérséklet [C]' in column_names:
        return f'{autoclave_num} belső, középső hőmérséklet [C]'
    elif f'{autoclave_num} belső, alsó hőmérséklet [C]' in column_names:
        return f'{autoclave_num} belső, alsó hőmérséklet [C]'
    elif f'TIT{autoclave_num}CU_11' in column_names:
        return f'TIT{autoclave_num}CU_11'
    elif f'TT{autoclave_num}CU_13' in column_names:
        return f'TT{autoclave_num}CU_13'
    elif f'TT{autoclave_num}CU_12' in column_names:
        return f'TT{autoclave_num}CU_12'
    else:
        raise ValueError


def reverse_map(mapping: dict):
    if len(set(mapping.values())) != len(mapping.values()):
        raise ValueError("The values in the map are not unique!")
    return {v: k for k, v in mapping.items()}


def get_text_from_gantt_click(click_data: Click_data, fig: Figure):
    """Get text attribute of the trace that you clicked on in a Gantt diagram"""
    muvelet_curve_num = click_data['points'][0]['curveNumber']
    legendgroup = fig['data'][muvelet_curve_num]['legendgroup']

    # in Gantt diagrams, the edges and the middle part of a bar is a bit different object
    # so first we need to find the edge's curve number
    if fig['data'][muvelet_curve_num]['name'] == '':  # did we click on the edge?
        muvelet_edge_curve_num = muvelet_curve_num
        # muvelet_middle_curve_num = search_trace_by_mode(muvelet_fig['data'], legendgroup, 'none')
    else:  # we clicked in the middle
        muvelet_edge_curve_num = find_trace_by_attr_values(fig['data'], {'legendgroup': legendgroup, 'mode': 'markers'})
        # muvelet_middle_curve_num = muvelet_curve_num
    text = fig['data'][muvelet_edge_curve_num]['text']
    return text


def find_trace_by_attr_values(figdata, attr_value_pairs):
    """Return the index of the trace that has matching attribute values

    :param figdata: the 'data' attribute of a plotly figure
    :param attr_value_pairs: a dictionary like {attribute1: value_of_attribute1, ...}
    """
    for idx, trace in enumerate(figdata):
        if all([trace[attr] == value for attr, value in attr_value_pairs.items()]):
            return idx


def get_clicked_attribute_from_gantt(click_data: Click_data,
                                     figure: Figure, attribute: str) -> Any:
    return figure['data'][click_data['points'][0]['curveNumber']][attribute]


def gen_start_end_query_params(start_date: str, end_date: str) -> Dict[str, Union[str, int, dt]]:
    start_date_dt = dt.strptime(start_date, "%Y-%m-%d")
    end_date_dt = dt.strptime(end_date, "%Y-%m-%d")
    query_params: Dict[str, Union[str, int, dt]] = {'start_date': start_date_dt,
                                                    'end_date': end_date_dt}
    return query_params


def get_sensor_table_columns(table):
    """Get names of columns in a table."""

    columns = pd.DataFrame.from_records(run_query(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA='efr_data' 
        AND TABLE_NAME='{table}'""")).iloc[:, 0].values.tolist()

    return columns


def triggered() -> Tuple[Optional[str], Optional[str]]:
    if len(callback_context.triggered) > 0:
        return callback_context.triggered[0]['prop_id'].split('.')
    return None, None


def generate_utilization_manual_order_dropdown() -> Options:
    data = run_query('select distinct order_name from manual_unit_order')
    orderings = [{'label': row['order_name'], 'value': row['order_name']} for row in data] if data is not None else []
    return orderings


def utilization_manual_order_default() -> str:
    data = run_query('select order_name from manual_unit_order limit 1')
    return data[0]['order_name'] if data is not None else ''


def generate_options_distinct(table_name, column_name, null=True):
    filter_nulls = f"where {column_name} is not null" if not null else ""
    query = f'select distinct {column_name} as col from {table_name} {filter_nulls}'
    data = run_query(query)
    options = [{'label': row['col'], 'value': row['col']} for row in data] if data is not None else []
    return options


def abs_path(df):
    def path(row: pd.Series):
        if pd.isna(row['parent_page_id']):
            return row['page_path_relative']
        return '/'.join([path(df[df['page_id'] == row['parent_page_id']].iloc[0]), row['page_path_relative']])

    return path


def module_subpage_links(module_name, selected_link, mon=0):
    # print(len(selected_link.split('.')))
    df = pages_with_abs_path()
    parent_page_id = df[df['layout_module'] == module_name]['page_id'].iloc[0]
    subpages_df = df[df['parent_page_id'] == parent_page_id]
    pages = subpages_df.to_dict('records')

    access_pages = []
    access_pages.append({'page_id': 1})
    access_pages.append({'page_id': 5})
    access_pages.append({'page_id': 4})
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
    # access_page_ids = [page['page_id'] for page in access_pages]  # TODO: ez már nem kell az összes aloldalhoz
    visible_page_ids = [4, 5, 7, 11, 12, 23, 24, 25, 26, 27, 28, 29]  # TODO: ezeket az aolodalakat használjuk
    links = []
    for page in pages:
        if page['page_id'] in visible_page_ids:
            link = dcc.Link([
                html.Div(page['page_name'],
                         className='custom-tab--selected' if page['layout_module'] in selected_link else 'custom-tab')
            ], href='/' + page['page_path_absolute'])
        else:
            # link = dummy_tab_link(page['page_name']) # TODO: csak a 3 oldalt és azok aloldalait jelenítjük meg
            link = []  # only a few options are visible
        links.append(link)

    return html.Div(links, className='tabs', id='tabs-main' if not mon else 'mon-sub-pages')


def pages_with_abs_path():
    pages = run_query('select page_id, page_name, parent_page_id, page_path_relative, layout_module from pages')
    df = pd.DataFrame(pages)
    df['page_path_absolute'] = df.apply(abs_path(df), axis=1)
    return df


def dummy_tab_link(text):
    return html.Div(text, className='custom-tab--disabled')


def rotating_log_handler(logfile_path):
    Path(logfile_path).parent.mkdir(parents=True, exist_ok=True)
    handler = logging.handlers.TimedRotatingFileHandler(
        logfile_path,
        when='d',
        interval=1,
        backupCount=8)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(message)s'))
    return handler
