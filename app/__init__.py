"""
Az app kódja oldalak mentén tagolódik subpackage-ekre (akár több szinten is, például a `statisztikak` alatti aloldalak
esetében). Minden subpackage `__init__.py` fájljában a `gen_layout` függvény kell hogy visszaadja az adott oldal
layoutját. A callback.py fájljlokban vannak a callback függvények, a viz.py fájlokban pedig a plotly-ábrák készítéséhez
tartozó függvények.

### Routing
Az adatbázis oldalon a `pages` táblában kell megjelölni hogy milyen oldalakat akarunk kiszolgálni.
Fontos, hogy minden szint megjelenjen itt, még ha a köztes szinteken nem is kell tartalmat megjeleníteni
(hogy az együtt megjelenítendő aloldal-linkeket össze lehessen gyűjteni).

Példa: a `statisztikak/__init__.py`-ban kell, hogy legyen `gen_layout` függvény, és a `pages` táblában is kell olyan sor,
ami az `app.statisztikak` modulra mutat, aminek a leszármazottai az egyes statisztikai aloldalak.
"""

import secrets

import dash
import dash_html_components as html
from flask_login import LoginManager
from flask_login import current_user

from .common import gen_layout_header
from .common.utils import rotating_log_handler

from .common.utils import parse_config

handler = rotating_log_handler('logs/error_log.txt')

cfg = parse_config()
env = cfg['environment']['env']
schema = cfg['database']['enet-schema']
if env != 'prod' and schema == 'enet':
    raise ValueError('Az "enet" adatbázis schema nem használható nem production környezetben.')

app = dash.Dash(__name__)
app.logger.addHandler(handler)
app.config.update({'port': '8050' if env == 'prod' else '8051'})
app.config['suppress_callback_exceptions'] = True

app.server.config.update(SECRET_KEY=secrets.token_urlsafe(16) if env == 'prod' else 'dev')
login_manager = LoginManager()
login_manager.init_app(app.server)

app.layout = html.Div(gen_layout_header(env) + [html.Div(id='page-content')])


def gen_layout():
    return html.Div(f'Üdvözöljük, a Tünde felületén!')
