from shapes import *
def get_elections(overwrite=False):
    level = 'vtd'
    year = 2022
    geoid = f'{level}{decade_(year)}'
    tbl = f'elections.all'
    if not check_exists(tbl, overwrite):
        tbl_raw = f'elections.raw'
        if not check_exists(tbl_raw):
            print(f'getting {tbl_raw}')
            pq = pq_(tbl_raw)
            zip_file = pq.parent / f'2020-general-vtd-election-data-2020.zip'
            url = f'https://data.capitol.texas.gov/dataset/35b16aee-0bb0-4866-b1ec-859f1f044241/resource/5af9f5e2-ca14-4e5d-880e-3c3cd891d3ed/download/{zip_file.name}'
            download(zip_file, url)
            L = []
            for f in sorted(zip_file.parent.iterdir()):
                a = f.stem.split('_')
                if a[-1] == 'Returns':
                    df = prep(pd.read_csv(f)).query('votes > 0 & party in ["R", "D", "L", "G"]').copy()
                    if len(df) == 0:
                        continue
                    df['year'] = int(a[0])
                    df['election'] = '_'.join(a[1:-2]).lower()
                    df['incumbent'] = df['incumbent'] == 'Y'
                    L.append(df)
            cols = ['year', 'county', 'fips', 'vtd', 'election', 'office', 'name', 'party', 'incumbent', 'votes']
            df = prep(pd.concat(L, axis=0).reset_index(drop=True)[cols])
            df['vtd'] = df['vtd'].astype('string')
            df_to_table(df, tbl_raw)
        tbl_vtd = get_shapes(level='vtd')
        print(f'getting {tbl}')
        qry = f"""
select
    coalesce(B.vtd2020, C.vtd2020, A.fips || A.vtd) as vtd2020,
    A.year,
    A.fips,
    A.county,
    A.election,
    A.office,
    A.party,
    A.name,
    A.incumbent,
    A.year||'_'||A.election||'_'||A.office||'_'||A.name||'_'||A.party as campaign,
    sum(A.votes) as votes,
    sum(coalesce(B.all_tot_pop, C.all_tot_pop)) as all_tot_pop,
from (
    select
        lpad(A.vtd, 6, '0') as vtd,
        A.year,
        {STATE.fips} || lpad(cast(A.fips as string), 3, '0') as fips,
        A.county,
        A.election,
        replace(replace(replace(A.office, ' ', ''), '.', ''), ',', '') as office,
        upper(A.party) as party,
        replace(A.name, ' ', '') as name,
        A.incumbent,
        A.votes,
    from
        {tbl_raw} as A
    ) as A
left join
    {tbl_vtd} as B
on
    A.fips || A.vtd = B.vtd2020
left join
    {tbl_vtd} as C
on
    A.fips || '0' || left(A.vtd, 5) = C.vtd2020
group by
    1,2,3,4,5,6,7,8,9,10
"""
        query_to_table(qry, tbl)
    return tbl