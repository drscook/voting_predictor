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

        sums = []
        feat = []
        for col in get_columns(tbl_raw)[2:]:
            i = col.find('_') + 1
            j = col.find('_', i)
            if col[:i] != 'all_':
                subpop = col[:j]
                f = col[i:]
                sums.append(f'sum(A.{col} * B.{subpop}_prop) as {col}')
                feat.append(f)
        feat = pd.value_counts(feat)
        feat = [f'white_{f} + hisp_{f} + other_{f} as all_{f}' for f in feat[feat >= 3].index]
        qry = f"""
select
    *,
    {make_select(feat)},
from (
    select
        B.vtd2020,
        A.year,
        {make_select(sums, 2)},
    from
        {tbl_raw} as A
    join
        {trans} as B
    using
        ({geoid})
    group by
        1, 2
    )
"""
        print(f'getting {tbl}')
        query_to_table(qry, tbl)
    return tbl