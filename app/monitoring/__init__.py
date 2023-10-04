from . import sql
from app.common.utils import module_subpage_links
import dash_html_components as html

def gen_monitoring_subpage_links(selected_tab=''):
    subpage_links = module_subpage_links(__name__, selected_tab, 1)
    return subpage_links


def gen_layout():
    return gen_monitoring_subpage_links()