from typing import List

import colorlover as cl
from plotly.graph_objects import Figure
from .utils import timeit


def fig_layout(figure: Figure) -> Figure:
    figure['layout'].update(dict(margin={'l': 50},
                                 autosize=True,
                                 plot_bgcolor='rgba(0,0,0,0)',
                                 height=600))
    figure['layout'].pop('width', None)
    figure.update_xaxes(hoverformat="%Y-%m-%d %H:%M:%S")
    return figure


def color_maker(num):
    """Egy színskálából választott RGB színekből álló listát ad vissza.
    Annyi színt ad vissza, ami a paraméterrel meghívott len() függvény visszatérési értéke.
    FIGYELEM: az így generált színek nem garantált, hogy teljesen egyediek lesznek bizonyos szám felett.
    """
    ylgnbu = cl.flipper()['seq']['9']['YlGnBu']
    # mivel a színskálák elejei általában nagyon beleolvadnak a háttérbe, több színt generálunk,
    # de az első pár színt nem használjuk fel
    ylgnbu_n = cl.to_rgb(cl.interp(ylgnbu, num + 2))[2:]
    return ylgnbu_n


def exact_distinct_colors(num: int) -> List[str]:
    distinct_colors = [
        'rgb(60, 180, 75)',  # green
        'rgb(70, 240, 240)',  # cyan
        'rgb(255, 225, 25)',  # yellow
        'rgb(0, 130, 200)',  # blue
        'rgb(230, 25, 75)',  # red
        'rgb(245, 130, 48)',  # orange
        'rgb(240, 50, 230)',  # magenta
        'rgb(250, 190, 190)',  # pink
        'rgb(0, 128, 128)',  # teal
        'rgb(230, 190, 255)',  # lavender
        'rgb(170, 110, 40)',  # brown
        'rgb(128, 0, 0)',  # maroon
        'rgb(170, 255, 195)',  # mint
        'rgb(0, 0, 128)',  # navy
        'rgb(128, 128, 128)',  # grey
        # 'rgb(255, 250, 200)'  # beige
    ]
    repeated_colors = distinct_colors * (num // len(distinct_colors) + 1)
    return repeated_colors[:num]


def mdm(series):
    """Egy számsorozat mediántól való abszolút eltéréseknek mediánját adja vissza."""
    return (series.median() - series).abs().median()


@timeit
def generate_gantt_figure(df, start, end, category, name, color_by=None, hovertexts=None, hovertext_sep='<br>',
                          hovertext_colnames=True, ord_by='start', ord_asc=True, customdata=None,
                          distinct_colors=False, color_column=None):
    """Plotly-s Gantt-diagram generáló függvény.

    :param df: A pandas dataframe, amiben levő adatokat ábrázolni akarjuk
    :param start: Azon df-beli oszlop neve, ami a kezdeti pontot jelenti. Azt feltételezi, hogy datetime típusú,
    de akár float vagy int is lehet, csak ekkor át kell írni a függvény által elkészített figure
    ['layout']['xaxis']['type'] változóját None-ra vagy 'linear'-ra, valamint a ['layout']['xaxis']['hoverformat']
    értékét megfelelő értékre (lásd https://plot.ly/python/reference/#layout-xaxis-hoverformat)
    :param end: Ugyanaz vonatkozik rá mint a start paraméterre, csak épp a befejező időpontot jelöli
    az itt megadott oszlopnév
    :param category: Az oszlop neve ami szerint külön sorba kell rendezni az egyes rekordokat. Az ebben az oszlopban
    levő értékek megjelennek az y tengelytől balra minden sor mellett
    :param name: Oszlopnév, aminek a tartalmát a 'name' attribútuma tartalmazza majd minden objektumnak.
    Ez nem jelenik meg a felhasználó részére sehol, de akkor hasznos lehet, ha mondjuk interaktív ábrát csinálunk
    és ki akarunk egyszerűen szedni valamilyen értéket clickData-ból vagy hoverData-ból, stb.
    (Tipikusan mondjuk az adott rekord azonosítóját.)
    :param color_by: Az oszlop neve ami alapján ugyanolyan színűre színezze az egyes rekordokat, kötelező paraméter.
    FIGYELEM: Ha túl sok egyedi érték van ebben az oszlopban, akkor előfordulhat, hogy különböző értékek is
    ugyanolyan színt kapnak.
    :param hovertexts: Egy vagy több oszlopnév listája. Ezeknek a tartalma kerül ki a hovertext-be
    :param hovertext_sep: String. Ez választja el a különböző hovertexts-ben megadott oszlop értékét a hovertextben
    :param hovertext_colnames: Bool. Ha True, akkor az oszlopnevek is megjelennek hovertextben
    "Oszlopnév: érték" mintára, idézőjelek nélkül
    :param ord_by: String: Mi alapján kövessék egymást a különböző kategóriák/sorok. Vagy egy oszlop a df-ből,
    vagy 'start' vagy 'category' (ilyen oszlopnevek szerint nem lehet sorbarendezni).
    'start': minden kategória legkorábbi kezdeti időpontja alapján rendezi el a sorokat
    'category': ABC-sorrendben szedi egymás alá a kategóriákat
    :param ord_asc: bool, emelkedő sorrendet használjunk-e
    :param customdata: Egy name-hez hasonló paraméter, viszont itt akár több oszlopnév listáját is megadhatjuk.
    Nem látható a felhasználó számára, interaktív funkcióknál lehet hasznos, hogy még több információt kinyerhessünk
    pl clickData-n és a figure objektumon keresztül.
    :param distinct_colors: Boolean, ami azt határozza meg, hogy milyen színeket használjon a függvény.
    True esetén igyekszik jól megkülönböztethető színeket használni, False esetén egy lineáris színskálát használ.
    :param color_column: Az oszlop neve, ami az adott adatponthoz tartozó színt jelöli stringgel. Formátum: rgb(R, G, B)
    Ha egy sorban üres string vagy None érték van, akkor annak a sornak a default módon generált színt adja (ami a
    distinct_color paramétertől is függ)

    :return: plotly.graph_objs.Figure objektum, ami a gantt-diagramot testesíti meg
    """

    if distinct_colors:
        colors = exact_distinct_colors(len(df[color_by].unique()))
    else:
        colors = color_maker(len(df[color_by].unique()))
    order_color = list(df[color_by].sort_values().unique())[::-1]

    if ord_by == 'category':
        order_task = list(df[category].sort_values(ascending=ord_asc).unique()[::-1])
    elif ord_by == 'start':
        order_task = list(df.groupby(category)[start].min().sort_values(ascending=ord_asc).index[::-1])
        if df[category].isna().sum() > 0:
            order_task.append(None)
    elif ord_by in df.columns:
        order_task = list(df.groupby(category)[ord_by].min().sort_values(ascending=ord_asc).index[::-1])
    else:
        raise ValueError(
            f"Not a valid 'ord_by' parameter: {ord_by}. Possible values are 'category', 'start' or a column of df.")

    if not type(hovertexts) == list:
        hovertexts = [hovertexts]
    if hovertexts == [None]:
        df['__concat_text__'] = ''
    elif hovertext_colnames:
        df['__concat_text__'] = df[hovertexts].astype(str).apply(
            lambda x: "<br>".join([': '.join(pair) for pair in zip(x.index, x.values)]), axis=1)
    else:
        df['__concat_text__'] = df[hovertexts].astype(str).apply(lambda x: hovertext_sep.join(x), axis=1)

    if not type(customdata) == list:
        customdata = [customdata]
    if customdata == [None]:
        df['__customdata__'] = None
    else:
        df['__customdata__'] = df[customdata].apply(lambda x: x.to_dict(), axis=1)

    traces = []
    for idx, row in df.iterrows():
        text = row['__concat_text__']
        y = order_task.index(row[category])
        color = colors[order_color.index(row[color_by])] if (not color_column or row[color_column] is None or row[color_column] == '') else row[color_column]

        middle_scatter = {
            'x': [row[start], row[end], row[end], row[start]],
            'y': [y - 0.2, y - 0.2, y + 0.2, y + 0.2],
            'mode': 'none',
            'fill': 'toself',
            'fillcolor': color,
            'hoverinfo': 'text',
            'text': text,
            'showlegend': False,
            'name': row[name],
            'customdata': [row['__customdata__']],
            'type': 'scatter'  # needed if we want to add traces as dicts
        }
        traces.append(middle_scatter)

    fig = Figure()
    fig.add_traces(traces)
    fig['layout']['yaxis'] = {
        'range': [-1, len(df[category].unique())],
        'autorange': False,
        'showgrid': True,
        'ticktext': order_task,
        'tickvals': list(range(len(order_task))),
        'gridcolor': 'rgb(181, 181, 181)',
        'zeroline': False
    }
    df.drop(['__concat_text__', '__customdata__'], axis=1, inplace=True)

    return fig
