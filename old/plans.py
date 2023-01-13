from helpers import *
import mechanicalsoup

def get_plans(overwrite=False):
    level = 'block'
    year = 2021
    geoid = f'{level}{decade_(year)}'
    tbl = f'plans.all'
    pq = pq_(tbl)
    if not check_exists(tbl, overwrite):
        print(f'getting {tbl}')
        browser = mechanicalsoup.Browser()
        district_types = {'s':31, 'h':150, 'c':38}
        for dt in district_types.keys():
            not_found = 0
            for k in range(1000):
                not_found += 1
                proposal = f'plan{dt}{2100+k}'.lower()
                root_url = f'https://data.capitol.texas.gov/dataset/{proposal}#'
                login_page = browser.get(root_url)
                tag = login_page.soup.select('a')
                if len(tag) >= 10:
                    not_found = 0
                    for t in tag:
                        url = t['href']
                        if 'blk.zip' in url:
                            zip_file = pq.parent / url.split('/')[-1]
                            download(zip_file, url)
                if not_found > 15:
                    break
        for file in pq.parent.iterdir():
            if file.suffix == '.zip':
                unzipper(file)

        L = []
        for file in pq.parent.iterdir():
            if file.suffix == '.csv':
                plan = file.stem.lower()
                dt = plan[4]
                df = prep(pd.read_csv(file))
                df.columns = ['block2020', 'district']
                # df['year'] = 2020
                df['plan'] = plan

                if df['district'].nunique() == district_types[dt]:
                    tbl_raw = f'plans.{plan}'
                    df_to_table(df, tbl_raw)
                    L.append(tbl_raw)
        L = sorted(L)
        sep = '\nunion all\n'
        qry = f"""
select
    *
from (
    {subquery(join([f'select * from {x}' for x in L], sep))}
    )
pivot(max(district) for plan in {tuple(x.split('.')[1] for x in L)})
"""
        query_to_table(qry, tbl, overwrite=True)
    return tbl
    #     df = prep(pd.concat(L, axis=0).reset_index(drop=True))
    #     df_to_parquet(pq)
    #     df_to_table(tbl)
    # return tbl
