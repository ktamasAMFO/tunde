import os
os.chdir('/home/cdsw/cloudera_kemia')

import app.statisztikak.ts_stats_api.callback
import app.statisztikak.run_stats_api.callback
import app.statisztikak.ts_stats_kemia.callback
import app.statisztikak.run_stats_kemia.callback
import app.epp_futo.epp_futo_api.callback
import app.monitoring.monitoring_api.callback
import app.epp_futo.epp_futo_kemia.callback
import app.monitoring.monitoring_kemia.callback
import app.common.callback


from app import app

if __name__ == '__main__':
    app.run_server(dev_tools_ui=False, port=os.getenv("CDSW_READONLY_PORT"))