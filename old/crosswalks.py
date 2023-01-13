from helpers import *
def get_crosswalks(year=2010, overwrite=False):
    dec = decade_(year)
    tbl = f'transformers.crosswalks_block{dec}_block2020'
    if not check_exists(tbl, overwrite):
        if dec == 2010:
            print(f'getting {tbl}')
            zip_file = pq_(tbl).parent / f'TAB2010_TAB2020_ST{STATE.fips}.zip'
            url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
            download(zip_file, url)

            txt = zip_file.with_name(f'{zip_file.stem}_{STATE.abbr}.txt'.lower())
            df = prep(pd.read_csv(txt, sep='|')).rename(columns={'arealand_int': 'aland'})#.query('aland > 0')
            for yr in [2010, 2020]:
                df[f'block{yr}'] = (
                    df[f'state_{yr}' ].astype(str).str.rjust(LEVELS['state' ], '0') +
                    df[f'county_{yr}'].astype(str).str.rjust(LEVELS['county'], '0') +
                    df[f'tract_{yr}' ].astype(str).str.rjust(LEVELS['tract' ], '0') +
                    df[f'blk_{yr}'   ].astype(str).str.rjust(LEVELS['block' ], '0'))
                df[f'prop{yr}'] = df['aland'] / np.fmax(df.groupby(f'block{yr}')['aland'].transform('sum'), 1)
            df = prep(df[['block2010', 'block2020', 'aland', 'prop2010', 'prop2020']])
            df_to_table(df, tbl)
        else:
            cross = get_crosswalks(2010)
            print(f'getting {tbl}')
            qry = f"""
select
    block2020,
    sum(aland) as aland,
    sum(prop2020) as prop2020,
from
    {cross}
group by
    1
"""
            query_to_table(qry, tbl)
    return tbl