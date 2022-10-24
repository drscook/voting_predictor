from acs5 import *
import torch, matplotlib.pyplot as plt, pandas_bokeh
device = "cuda" if torch.cuda.is_available() else "cpu"

def features(year, election='general', office='President', level='tract', overwrite=False):
    get_acs5(year=year, overwrite=overwrite)
    pq = data_path / f'features/{year}_{election}_{office}.parquet'
    if pq.is_file() and not overwrite:
        df = pd.read_parquet(pq)
    else:
        qry = f"""
        select
            vtd2020,
            election,
            office,
            lower(party) as party,
            campaign,
            -- party,
            votes,
        from
            elections.all
        where
            election = '{election}'
            and office = '{office}'
            and year = {year}
            and party in ('R', 'D')
        """
        elections = query_to_df(qry).pivot(index=['vtd2020', 'campaign'], columns='party', values='votes').fillna(0)#.reset_index('campaign')#.set_index('vtd2020')

        qry = f"""
        select
            A.vtd2020,
            A.county,
            cast(round(A.aland)             as int) as aland,
            cast(round(A.awater)            as int) as awater,
            cast(round(A.atot)              as int) as atot,
            cast(round(A.perim)             as int) as perim,
            cast(round(A.polsby_popper*100) as int) as polsby_popper,
            cast(round(A.dist_to_border)    as int) as dist_to_border,
        from
            shapes.vtd2020 as A
        """
        shapes = query_to_df(qry).set_index('vtd2020')

        qry = f"""select
            *
        from
            acs5.{year}
        """
        acs = query_to_df(qry).set_index('vtd2020')

        vtd = prep(elections.join(shapes, how='outer').join(acs, how='outer'))
        df = vtd

        for race in ['all', 'white', 'hisp', 'other']:
            df[race+'_vap_density'] = (df[race+'_vap_pop'] / np.fmax(df['aland'], 1) * 1609.34**2)
        df['votes'] = df[elections.columns].sum(axis=1)
        for candidate in elections.columns:
            df[candidate+'_pct'] = df[candidate] / np.fmax(df['all_tot_pop'], 1) * 100

        for col in df.columns:
            a = col.find('_')+1
            b = col[a:].find('_')+a+1
            subpop = col[:b] + 'pop'
            if col[b:] != 'pop' and subpop in df.columns:
                df[col] /= np.fmax(df[subpop], 1)
        df_to_parquet(df, pq)
    
    return pq


