from transformers import *
def get_acs5(year=2018, overwrite=False):
    tbl = f'acs5.{year}'
    if not check_exists(tbl, overwrite):
        level = 'tract'
        geoid = f'{level}{decade_(year)}'
        tbl_raw = tbl+'_raw'
        if not check_exists(tbl_raw):
            print(f'getting {tbl_raw}')
            base = set().union(*FEATURES['cols'])
            survey = {x for x in base if x[0].lower()=='s'}
            base = base.difference(survey)
            df = get(base, 'acs5', year, level).merge(get(survey, 'acs5st', year, level), on=['year', geoid])
            for k, row in FEATURES.iterrows():
                df[row['name']] = df[row['cols']].sum(axis=1)

            df = df[[geoid, 'year', *FEATURES['name']]].copy()
            for var in FEATURES['age_var'].unique():
                compute_other(df, var)
            df_to_table(df, tbl_raw)
            df_to_parquet(df, pq_(tbl_raw))

        trans = get_transformer(year=year, level=level)['vtd']
        cols = {x: x[:x.find("_", x.find("_")+1)] for x in get_columns(tbl_raw)[2:]}
        sums = [f'cast(round(sum(A.{key} * B.{val}_prop)) as int) as {key}' for key, val in cols.items()]
        qry = f"""
select
    B.vtd2020,
    A.year,
    {make_select(sums)},
from
    {tbl_raw} as A
join
    {trans} as B
using
    ({geoid})
group by
    1, 2
"""
        print(f'getting {tbl}')
        query_to_table(qry, tbl)
    return tbl