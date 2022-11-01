from acs5 import *
import torch, matplotlib.pyplot as plt, pandas_bokeh
device = "cuda" if torch.cuda.is_available() else "cpu"
warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')

def tensorify(T):
    if not torch.is_tensor(T):
        T = torch.FloatTensor(np.array(T)).to(device)
    return T

def extract_features(elec, overwrite=False):
# def extract_features(year, election='general', office='President', level='tract', overwrite=False):
    year, election, office = elec.split('_')
    m_per_mi = 1609.34
    year = int(year)
    get_acs5(year=year, overwrite=overwrite)
    pq = DATA_PATH / f'features/{year}_{election}_{office}.parquet'
    if pq.is_file() and not overwrite:
        df = gpd.read_parquet(pq)
    else:
        qry = f"""
        select
            vtd2020,
            year || "_" || election || "_" || office as election,
            lower(party) as party,
            votes,
        from
            elections.all
        where
            election = '{election}'
            and office = '{office}'
            and year = {year}
            and party in ('R', 'D')
        """
        elections = query_to_df(qry).pivot(index=['vtd2020', 'election'], columns='party', values='votes').fillna(0)

        qry = f"""
        select
            A.vtd2020,
            A.county,
            A.dist_to_border / {m_per_mi} as dist_to_border,
            A.aland / {m_per_mi**2} as aland,
            A.polsby_popper as polsby_popper,
            A.geometry,
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
        vtd = prep(gpd.GeoDataFrame(elections.join(shapes, how='inner').join(acs, how='inner')))
        vtd.geometry = vtd.geometry.simplify(0.004)
        df = vtd

        for race in ['all', 'white', 'hisp', 'other']:
            df[f'{race}_vap_density'] = (df[f'{race}_vap_pop'] / df['aland']).fillna(0)
        # df['votes'] = df[elections.columns].sum(axis=1)
        # for party in elections.columns:
        #     col = party+'_prop'
        #     df[col] = (df[party] / df['all_vap_pop'])#.clip(0, 1)
        #     df[col].fillna(df[col].median(), inplace=True)

        for col in df.columns:
            a = col.find('_')+1
            b = col.find('_', a)+1
            subpop = col[:b] + 'pop'
            if col[b:] not in ['pop', 'density'] and subpop in df.columns:
                df[col] = (df[col] / df[subpop])#.clip(0, 1)
                df[col].fillna(df[col].median(), inplace=True)                
        col = 'hisp_vap_spanish_at_home_english_well'
        df[col] = df[col] / df['hisp_vap_spanish_at_home']
        df[col].fillna(df[col].median(), inplace=True)
        df_to_parquet(df, pq)
    return df

def extract_dataset(elections, feat, targ, weig):
    D = [extract_features(elec) for elec in listify(elections)]
    df = gpd.GeoDataFrame(pd.concat(D).sample(frac=1, random_state=42)).to_crs(D[0].crs)
    W = df[weig].astype(float)
    X = df[feat].astype(float)
    Y = df[targ].astype(float)
    W = W.join(W, lsuffix='_d', rsuffix='_r')
    X = (X - X.min()) / (X.max() - X.min())
    return df, W, X, Y
