from typing import Optional, List, Dict, Any

import pandas as pd
import numpy as np
import pyodbc

query_calc = '''
set @max_terfogataram = 50 * 0.8;

# térfogatáram együtthatók
SET @coef4 = -0.000001155889569;
SET @coef3 = 0.00016649;
SET @coef2 = -0.00215652;
SET @coef1 = 0.11733987;

# milyen hőmérséklet értékekkel számoljuk a lineáris regressziót
set @x1 = 170;
set @x2 = 175;
set @x3 = 180;
set @x4 = 185;
set @x5 = 190;
set @x6 = 195;
set @x7 = 200;


select `Dátum` as Datum,
       `Térfogatáram [m^3/s]` as `CF{unit}JM`,
       `Töltet hőteljesítmény [kW]` as `CQB{unit}CU`,
       `Monofluid hőteljesítmény [kW]` as `CQM{unit}JM`,
       `Monofluid hőteljesítmény [kW]` / 0.45 / @max_terfogataram + {kopeny_ki} as 'CT{unit}JM_MIN',
       ((Sy * Sxx) - (Sx * Sxy)) / ((7 * (Sxx)) - (Sx * Sx))                   AS 'CT{unit}JM_IC',  # tengelymetszet
       ((7 * Sxy) - (Sx * Sy)) / ((7 * Sxx) - (Sx * Sx))                       AS 'CT{unit}JM_SL'  # meredekség
from (
         # segédmennyiségek a lineáris regresszióhoz
         select y1 + y2 + y3 + y4 + y5 + y6 + y7                                           as Sy,
                @x1 + @x2 + @x3 + @x4 + @x5 + @x6 + @x7                                    as Sx,
                y1 * @x1 + y2 * @x2 + y3 * @x3 + y4 * @x4 + y5 * @x5 + y6 * @x6 + y7 * @x7 as Sxy,
                power(@x1, 2) + power(@x2, 2) + power(@x3, 2) + power(@x4, 2) + power(@x5, 2) + power(@x6, 2) +
                power(@x7, 2)                                                              as Sxx,
                `Dátum`,
                `Térfogatáram [m^3/s]`,
                `Töltet hőteljesítmény [kW]`,
                `Monofluid hőteljesítmény [kW]`,
                {kopeny_ki}
         from (
                  # térfogatáram szükségletek
                  select `Dátum`,
                         `Térfogatáram [m^3/s]`,
                         `Töltet hőteljesítmény [kW]`,
                         `Monofluid hőteljesítmény [kW]`,
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x1 - {kopeny_ki}) as y1, # térfogatáram szükséglet 170C-os monofluid esetén
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x2 - {kopeny_ki}) as y2,
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x3 - {kopeny_ki}) as y3,
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x4 - {kopeny_ki}) as y4,
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x5 - {kopeny_ki}) as y5,
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x6 - {kopeny_ki}) as y6,
                         `Monofluid hőteljesítmény [kW]` / 0.45 / (@x7 - {kopeny_ki}) as y7,
                         {kopeny_ki}
                  from (
                           # monofluid hőteljesítmény
                           select `Dátum`,
                                  case
                                      when {futesi_allapot} = 2
                                          then 0.45 * `Térfogatáram [m^3/s]` * (TIT863_11 - {kopeny_ki})
                                      else 0 end as `Monofluid hőteljesítmény [kW]`,
                                  `Térfogatáram [m^3/s]`,
                                  `Töltet hőteljesítmény [kW]`,
                                  {kopeny_ki}
                           from (
                                    # térfogatáram
                                    select terfogataram.TimeStamp as 'Dátum',
                                           {futesi_allapot},
                                           {kopeny_ki},
                                           TIT863_11,
                                           case
                                               when {futesi_allapot} = 2
                                                   then round(
                                                           @coef4 * power({szelep}, 4) +
                                                           @coef3 * power({szelep}, 3) +
                                                           @coef2 * power({szelep}, 2) +
                                                           @coef1 * {szelep}, 15)
                                               else 0 end     as 'Térfogatáram [m^3/s]',
                                           `Töltet hőteljesítmény [kW]`
                                    from (select k1.TimeStamp,
                                                 {futesi_allapot},
                                                 {kopeny_ki},
                                                 {szelep}
                                          from efr_data.k1_apimppi_p k1
                                          join efr_data.k1_apimpp2_p k2
                                              on k1.TimeStamp = k2.TimeStamp) terfogataram
                                             join (
                                        # töltet hőteljesítmény
                                        select 4.18 * present.{toltet_mennyiseg} / 600 *
                                               (future.{belso_homerseklet} - past.{belso_homerseklet}) as 'Töltet hőteljesítmény [kW]',
                                               present.TimeStamp,
                                               TIT863_11
                                        from (select k1.TimeStamp,
                                                     TIT863_11,
                                                     {toltet_mennyiseg}
                                              from efr_data.k1_apimppi_p k1
                                              join efr_data.k1_apimpp2_p k2
                                                  on k1.TimeStamp = k2.TimeStamp) present
                                                 left join (select {belso_homerseklet},
                                                                   k1.TimeStamp
                                                            from efr_data.k1_apimppi_p k1
                                                            join efr_data.k1_apimpp2_p k2
                                                                on k1.TimeStamp = k2.TimeStamp) past
                                                           on past.TimeStamp = date_add(present.TimeStamp, interval -5 minute)
                                                 left join (select {belso_homerseklet},
                                                                   k1.TimeStamp
                                                            from efr_data.k1_apimppi_p k1
                                                            join efr_data.k1_apimpp2_p k2
                                                                on k1.TimeStamp = k2.TimeStamp) future
                                                           on future.TimeStamp = date_add(present.TimeStamp, interval 5 minute)
                                    ) felvett_hotelj
                                                  on felvett_hotelj.TimeStamp = terfogataram.TimeStamp
                                ) a
                       ) b
              ) c
     ) d
where `Dátum` between '{start}' and '{end}' '''


query_cols_in_table = '''
select COLUMN_NAME as 'Column'
from information_schema.columns
where COLUMN_TYPE <> 'datetime'
  and (table_name = 'EFR_S0000034.k1_apimppi_p_tunde_view' 
    or table_name = 'EFR_S0000067.k1_apimpp2_p_tunde_view')'''


def cols_to_strings(df):
    for col, dtype in df.dtypes.iteritems():
        if dtype == 'timedelta64[ns]':
            df[col] = df[col] / np.timedelta64(1, 'm')
            df[col] = df[col].astype(str)
        elif dtype == 'datetime64[ns]':
            df[col] = df[col].dt.strftime("'%Y-%m-%d %H:%M:%S'")
        elif dtype == 'object':
            df[col] = "'" + df[col].astype(str) + "'"
        else:
            df[col] = df[col].astype(str)


def truncate_insert_stats(df, table_name):
    insert = f'insert into {table_name} values '
    insert_rows_values = '(' + df.apply(', '.join, axis=1) + ')'
    step = 1000
    for i in range(0, len(insert_rows_values), step):
        insert_vals = insert_rows_values[i:i + step].str.cat(sep=', ').replace('nan', 'Null').replace("'None'", 'Null')
        run_query(insert + insert_vals)


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


def run_query(query: str) -> Optional[List[Dict[str, Any]]]:
    conn, cursor = connect_to_db()
    try:
        cursor.execute(query)

        columnNames = [column[0] for column in cursor.description]
        results = []

        for record in cursor.fetchall():
            results.append(dict(zip(columnNames, record)))
        # print(results)
        conn.commit()
        if results:
            return results
        else:
            return None
    finally:
        cursor.close()
        conn.close()


def calc_ts():
    start = run_query('''select {fn TIMESTAMPADD(SQL_TSI_MINUTE, 1, max(Datum))} as start from calculated''')[0]['start']
    end = str(pd.datetime.now().year + 1) + '-12-31 23:59:59'
    # start = '2019-09-01'
    # end = '2019-12-31 23:59:00'

    df_full = pd.DataFrame()

    # for unit in autoklav_nums:
    for unit in [171, 172, 174, 175, 182, 183, 271, 272, 273, 274]:
        print(unit)
        data = run_query(query_cols_in_table)
        columns = [row['Column'] for row in data]

        columns_in_query = {'kopeny_ki': f'TT{unit}JM_12' if f'TT{unit}JM_12' in columns else f'TIT{unit}JM_12',
                            'futesi_allapot': f'VSZ{unit}JM_31',
                            'szelep': f'TV{unit}JM_11',
                            'toltet_mennyiseg': f'LIT{unit}CU_61' if f'LIT{unit}CU_61' in columns else f'WY{unit}CU_61N',
                            'belso_homerseklet': f'TIT{unit}CU_11' if f'TIT{unit}CU_11' in columns else (
                                f'TT{unit}CU_13' if f'TT{unit}CU_13' in columns else f'TT{unit}CU_12'),
                            'unit': unit,
                            'start': start, 'end': end}
        if f'TT{unit}JM_12' in columns and (
                f'VSZ{unit}JM_31' in columns) and (
                f'TV{unit}JM_11' in columns) and (
                f'LIT{unit}CU_61' in columns or f'WY{unit}CU_61N' in columns) and (
                f'TIT{unit}CU_11' in columns or f'TT{unit}CU_13' in columns or f'TT{unit}CU_12' in columns):
            sql_query_calc = query_calc.format_map(columns_in_query)
            data = run_query(sql_query_calc)
            if data is None:
                # there are no new timestamps since last update
                return True
            df = pd.DataFrame.from_records(data, index='Datum')
            # TODO: handle daylight savings better
            # remove duplicated timestamp indices when daylight savings time duplicates them
            df = df.loc[~df.index.duplicated(keep='first')]
            df_full = pd.merge(df_full, df, how='outer', left_index=True, right_index=True)

    df_full.reset_index(inplace=True)
    cols_to_strings(df_full)
    truncate_insert_stats(df_full, 'calculated')
    return True


if __name__ == '__main__':
    # calculated.py-t futtatja
    calc_ts()
