from . import sql
from app.common.utils import module_subpage_links


def gen_now_running_data_subpage_links(selected_tab=''):
    subpage_links = module_subpage_links(__name__, selected_tab)
    return subpage_links


def gen_layout():
    return gen_now_running_data_subpage_links()
