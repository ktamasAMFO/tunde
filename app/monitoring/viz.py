import pandas as pd
from plotly.graph_objs import Figure, Scatter


def figure_monitoring(df: pd.DataFrame, stat: str):
    fig = Figure()

    pressure_stat = ((stat[:2] in ('PT', 'PD')) or
                     (stat[:3] == 'PIT'))

    traces = list()
    has_stats = all([stat + postfix in df.columns for postfix in ['MID', ' up', ' low']])
    if has_stats:
        traces.append(Scatter(x=df['TimeStamp'],
                              y=df[stat + ' up'],
                              mode="lines",
                              fill=None,
                              marker=dict(color="#444"),
                              line=dict(width=0),
                              fillcolor='rgba(68, 68, 68, 0.3)',
                              legendgroup='stats_deviation'))

        traces.append(Scatter(x=df['TimeStamp'],
                              y=df[stat + ' low'],
                              mode="lines",
                              fill="tonexty",
                              marker=dict(color="#444"),
                              line=dict(width=0),
                              fillcolor='rgba(105,105,105, 0.6)',
                              legendgroup='stats_deviation'))

        traces.append(Scatter(x=df['TimeStamp'],
                              y=df[stat + 'MIN'],
                              mode='lines',
                              line=dict(color='rgba(169,169, 169, 0.5)', width=1)))
        traces.append(Scatter(x=df['TimeStamp'],
                              y=df[stat + 'MAX'],
                              mode='lines',
                              fill="tonexty",
                              line=dict(color='rgba(169,169,169, 0.5)', width=1)))

    traces.append(Scatter(x=df['TimeStamp'],
                          y=df[stat],
                          mode="markers",
                          marker=dict(color='rgb(238, 60, 60)', size=3)))
    fig.add_traces(traces)

    fig['layout']['yaxis']['title_text'] = stat
    fig['layout']['showlegend'] = False
    fig.update_layout(margin=dict(l=20, r=20, t=10, b=10))
    fig.update_layout(height=225)
    fig.update_yaxes(nticks=20)

    min_yrange = 1 if pressure_stat else 10
    if has_stats:
        max_y = max(df[stat].max(), df[stat + ' up'].max())
        min_y = min(df[stat].min(), df[stat + ' low'].min())
    else:
        max_y = df[stat].max()
        min_y = df[stat].min()
    if (float(max_y) - float(min_y)) < min_yrange:
        mid = float(min_y) + (float(max_y) - float(min_y)) / 2
        fig.update_yaxes(range=[mid - min_yrange / 2, mid + min_yrange / 2])

    return fig
