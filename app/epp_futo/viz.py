from datetime import datetime as dt
from typing import List
import pandas as pd
from plotly.graph_objs import Figure, Scatter
from . import sql
import app.common.utils as utils
from app.common.viz import generate_gantt_figure, fig_layout


@utils.timeit
def gantt_fig_running_sarzsok(df: pd.DataFrame, now: dt) -> Figure:
    fig = generate_gantt_figure(df, start='Start', end='Finish', category='Sarzs', name='Sarzs', hovertexts=['Start', 'Finish', 'Sarzs'],
                                color_column='color', color_by='Sarzs', customdata=['Sarzs'])
    fig = fig_layout(fig)
    fig.update_layout({'title': 'Sarzsok'})
    y = list(fig['layout']['yaxis']['range'])
    now_line = {'x': [now, now],
                'y': y,
                'showlegend': False,
                'type': 'scatter',
                'mode': 'lines'}
    fig.add_trace(now_line)
    return fig


@utils.timeit
def gantt_fig_running_receptek(df: pd.DataFrame, now: dt) -> Figure:
    fig = generate_gantt_figure(df, start='Start', end='Finish', category='sarzs-recept', name='Recept komment',
                                hovertexts=['Start', 'Finish', 'Recept komment'], customdata=['ChargeNr', 'RecipeNum', 'Recept komment'],
                                color_column='color', color_by='sarzs-recept', ord_by='sarzs-recept')
    fig = fig_layout(fig)
    fig.update_layout({'title': 'Receptek'})
    y = list(fig['layout']['yaxis']['range'])
    now_line = {'x': [now, now],
                'y': y,
                'showlegend': False,
                'type': 'scatter',
                'mode': 'lines'}
    fig.add_trace(now_line)
    return fig


@utils.timeit
def gantt_fig_running_unit_receptek(df: pd.DataFrame, title: str, now: dt) -> Figure:
    fig = generate_gantt_figure(df, start='Start', end='Finish', category='Unit recept komment', color_column='color',
                                name='Unit recept komment', hovertexts=['Start', 'Finish', 'Unit recept komment', 'Unit'],
                                customdata=['ChargeNr', 'RecipeNum', 'UnitRecipeNum', 'Recept komment',
                                            'Unit recept komment', 'BatchID', 'Start', 'Unit'], color_by='ChargeNr')
    fig = fig_layout(fig)
    fig.update_layout({'title': title})
    y = list(fig['layout']['yaxis']['range'])
    now_line = {'x': [now, now],
                'y': y,
                'showlegend': False,
                'type': 'scatter',
                'mode': 'lines'}
    fig.add_trace(now_line)
    return fig


@utils.timeit
def gantt_fig_running_muveletek(df: pd.DataFrame, title: str, now: dt) -> Figure:
    fig = generate_gantt_figure(df, start='Start', end='Finish', category='Művelet komment', name='Művelet komment',
                                color_by='Start', hovertexts=['Művelet komment', 'Unit'], color_column='color',
                                customdata=['Unit', 'Recept komment', 'Unit recept komment', 'Művelet komment',
                                            'ChargeNr', 'RecipeNum', 'UnitRecipeNum', 'Op', 'OpInst'])
    fig = fig_layout(fig)
    fig.update_layout({'title': title})
    y = list(fig['layout']['yaxis']['range'])
    now_line = {'x': [now, now],
                'y': y,
                'showlegend': False,
                'type': 'scatter',
                'mode': 'lines'}
    fig.add_trace(now_line)
    return fig


def figure_of_muvelet_stats(df: pd.DataFrame, chosen_col: str, charge_no: str, cycle: int,
                            muvelet_id_list: List[str], unit: str, api: str, deviation_plot: bool = False) -> Figure:
    fig = Figure()

    pressure_stat = ((chosen_col[:2] in ('PT', 'PD')) or
                     (chosen_col[:3] == 'PIT'))

    fig.update_layout(title=f'{" / ".join(muvelet_id_list)} / {chosen_col} ({unit} autokláv)', height=800)
    fig.update_xaxes(title='Művelet kezdetétől eltelt idő (perc)')
    query_param = {'col': chosen_col}
    if api == '1':
        table = 'EFR_S0000034.k1_apimppi_lp_tunde_view'
        query_param.update({'table': table})
        sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
        tagname_data = utils.run_query(sql_query_tagname_single.format(table=table))[0]
        chosen_tagname = tagname_data['TagName'] or tagname_data['TagID']
        chosen_dimension = tagname_data['Dimension']
    elif api == '2':
        table = 'EFR_S0000067.k1_apimpp2_lp_tunde_view'
        query_param.update({'table': table})
        sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
        tagname_data = utils.run_query(sql_query_tagname_single.format(table=table))[0]
        chosen_tagname = tagname_data['TagName'] or tagname_data['TagID']
        sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
        chosen_dimension = utils.run_query(sql_query_tagname_single.format(table=table))[0]['Dimension']
    if deviation_plot:
        fig.update_yaxes(title=f'{chosen_tagname.strip()} átlagostól való eltérése ({chosen_dimension.strip()})')
    else:
        fig.update_yaxes(title=f'{chosen_tagname.strip()} ({chosen_dimension.strip()})')

    if deviation_plot:
        df[chosen_col] -= df['MID']
        df['MIN'] -= df['MID']
        df['MAX'] -= df['MID']
        df['up'] -= df['MID']
        df['low'] -= df['MID']
        df['MID'] -= df['MID']

    mean = Scatter(x=df['offset'],
                   y=df['MID'],
                   name='medián',
                   mode='lines',
                   line=dict(color='rgb(31, 119, 180)'),
                   legendgroup='stats_middle')
    min_bound = Scatter(x=df['offset'],
                        y=df['MIN'],
                        name='minimum',
                        mode='lines',
                        line=dict(color='rgba(0, 0, 0, 0.3)', dash='dot'),
                        legendgroup='stats_minmax')
    max_bound = Scatter(x=df['offset'],
                        y=df['MAX'],
                        name='maximum',
                        mode='lines',
                        line=dict(color='rgba(0, 0, 0, 0.3)', dash='dot'),
                        legendgroup='stats_minmax')
    upper_bound = Scatter(name='átlag + szórás',
                          x=df['offset'],
                          y=df['up'],
                          mode='lines',
                          marker=dict(color="#444"),
                          line=dict(width=0),
                          fillcolor='rgba(68, 68, 68, 0.3)',
                          fill='tonexty',
                          legendgroup='stats_DEV')
    lower_bound = Scatter(name='átlag - szórás',
                          x=df['offset'],
                          y=df['low'],
                          mode='lines',
                          marker=dict(color="#444"),
                          line=dict(width=0),
                          legendgroup='stats_DEV')
    trace = Scatter(x=df['offset'],
                    y=df[chosen_col],
                    mode="lines",
                    name=f'{charge_no}-{cycle}',
                    line=dict(color='rgb(238, 60, 60)'))
    # fontos, hogy milyen sorrendben adjuk hozzá a trace-eket (mivel pl az upper_boundban a 'fill' attribútum 'tonexty')
    fig.add_traces([min_bound, max_bound, lower_bound, upper_bound, mean, trace])

    min_yrange = 1 if pressure_stat else 10
    max_y = max(df[chosen_col].max(), df['up'].max(), df['MAX'].max())
    min_y = min(df[chosen_col].min(), df['low'].min(), df['MIN'].min())
    if (float(max_y) - float(min_y)) < min_yrange:
        mid = float(min_y) + (float(max_y) - float(min_y)) / 2
        fig.update_yaxes(range=[mid - min_yrange/2, mid + min_yrange/2])

    return fig


def figure_of_muvelet_stats_kemia(df: pd.DataFrame, chosen_col: str, charge_no: str, cycle: int,
                            muvelet_id_list: List[str], unit: str, api: str, deviation_plot: bool = False) -> Figure:
    fig = Figure()
    
    chosen_col_basic = chosen_col[:-9] if 'ActValue' in chosen_col else chosen_col[:-13]
    
    pressure_stat = ((chosen_col[:2] in ('PT', 'PD')) or
                     (chosen_col[:3] == 'PIT'))

    fig.update_layout(title=f'{" / ".join(muvelet_id_list)} / {chosen_col_basic} ({unit} autokláv)', height=800)
    fig.update_xaxes(title='Művelet kezdetétől eltelt idő (perc)')
    query_param = {'col': chosen_col_basic}
    if api == 'kemiai':
      table = 'EFR_S0000058.k2_vegyes_lp_tunde_view'
      query_param.update({'table': table})
      sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
      tagname_data = utils.run_query(sql_query_tagname_single.format(table=table))[0]
      chosen_tagname = tagname_data['TagName'] or tagname_data['TagID']
      chosen_dimension = tagname_data['Dimension']
    elif api == 'kemia2':
      table = 'EFR_S0000057.k2_vegtermek_lp_tunde_view'
      query_param.update({'table': table})
      sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
      tagname_data = utils.run_query(sql_query_tagname_single.format(table=table))[0]
      chosen_tagname = tagname_data['TagName'] or tagname_data['TagID']
      sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
      chosen_dimension = utils.run_query(sql_query_tagname_single.format(table=table))[0]['Dimension']
    else:
      table = 'EFR_24EPFIR.k2_24epfir_lp_tunde_view'
      query_param.update({'table': table})
      sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
      tagname_data = utils.run_query(sql_query_tagname_single.format(table=table))[0]
      chosen_tagname = tagname_data['TagName'] or tagname_data['TagID']
      sql_query_tagname_single = sql.query_tagname_single.format(**query_param)
      chosen_dimension = utils.run_query(sql_query_tagname_single.format(table=table))[0]['Dimension']

    if deviation_plot:
        fig.update_yaxes(title=f'{chosen_tagname.strip()} átlagostól való eltérése ({chosen_dimension.strip()})')
    else:
        fig.update_yaxes(title=f'{chosen_tagname.strip()} ({chosen_dimension.strip()})')

    if deviation_plot:
        df[chosen_col] -= df['MID'].astype(float)
        df['MIN'] -= df['MID']
        df['MAX'] -= df['MID']
        df['up'] -= df['MID']
        df['low'] -= df['MID']
        df['MID'] -= df['MID']

    mean = Scatter(x=df['offset'],
                   y=df['MID'],
                   name='medián',
                   mode='lines',
                   line=dict(color='rgb(31, 119, 180)'),
                   legendgroup='stats_middle')
    min_bound = Scatter(x=df['offset'],
                        y=df['MIN'],
                        name='minimum',
                        mode='lines',
                        line=dict(color='rgba(0, 0, 0, 0.3)', dash='dot'),
                        legendgroup='stats_minmax')
    max_bound = Scatter(x=df['offset'],
                        y=df['MAX'],
                        name='maximum',
                        mode='lines',
                        line=dict(color='rgba(0, 0, 0, 0.3)', dash='dot'),
                        legendgroup='stats_minmax')
    upper_bound = Scatter(name='átlag + szórás',
                          x=df['offset'],
                          y=df['up'],
                          mode='lines',
                          marker=dict(color="#444"),
                          line=dict(width=0),
                          fillcolor='rgba(68, 68, 68, 0.3)',
                          fill='tonexty',
                          legendgroup='stats_DEV')
    lower_bound = Scatter(name='átlag - szórás',
                          x=df['offset'],
                          y=df['low'],
                          mode='lines',
                          marker=dict(color="#444"),
                          line=dict(width=0),
                          legendgroup='stats_DEV')
    trace = Scatter(x=df['offset'],
                    y=df[chosen_col],
                    mode="lines",
                    name=f'{charge_no}-{cycle}',
                    line=dict(color='rgb(238, 60, 60)'))
    # fontos, hogy milyen sorrendben adjuk hozzá a trace-eket (mivel pl az upper_boundban a 'fill' attribútum 'tonexty')
    fig.add_traces([min_bound, max_bound, lower_bound, upper_bound, mean, trace])

    min_yrange = 1 if pressure_stat else 10
    max_y = max(df[chosen_col].max(), df['up'].max(), df['MAX'].max())
    min_y = min(df[chosen_col].min(), df['low'].min(), df['MIN'].min())
    if (float(max_y) - float(min_y)) < min_yrange:
        mid = float(min_y) + (float(max_y) - float(min_y)) / 2
        fig.update_yaxes(range=[mid - min_yrange/2, mid + min_yrange/2])

    return fig
