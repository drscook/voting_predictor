from constants import *
import json, os, sys, subprocess, pathlib, shutil, warnings, itertools as it, us, census
import numpy as np, pandas as pd, geopandas as gpd
from google.colab import auth, drive
from google.cloud import bigquery
from codetiming import Timer

STATE = us.states.TX

project_id = 'redistricting-361203'
repo = 'voting_predictor'
MOUNT_PATH = pathlib.Path('/content/drive')
ROOT_PATH = MOUNT_PATH / 'MyDrive/gerrymandering/2022-10'
REPO_PATH = ROOT_PATH / repo
DATA_PATH = ROOT_PATH / f'data/{STATE.abbr}'
MODEL_PATH= ROOT_PATH / f'models'

auth.authenticate_user()
# drive.mount(str(mount_path))
client = bigquery.Client(project=project_id)

api_key = 'dccb7bb4b7df5dff59d2d99c859016f973197e4e'
census_session = census.Census(api_key)

pd.set_option('plotting.backend', 'pandas_bokeh')
pd.plotting.output_notebook()
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.simplefilter(action='ignore', category=FutureWarning)

def mkdir(path, overwrite=False):
    if overwrite:
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(exist_ok=True, parents=True)

def listify(X):
    """Turns almost anything into a list"""
    if isinstance(X, list):
        return X
    elif (X is None) or (X is np.nan) or (X==''):
        return []
    elif isinstance(X, str):
        return [X]
    else:
        try:
            return list(X)
        except:
            return [X]

def cartesian(D):
    D = {key: listify(val) for key, val in D.items()}
    return [dict(zip(D.keys(), x)) for x in it.product(*D.values())]

def to_numeric(ser):
    """converts columns to small numeric dtypes when possible"""
    dt = str(ser.dtype)
    if  dt in ['geometry', 'timestamp'] or dt[0].isupper():
        return ser
    else:
        return pd.to_numeric(ser, errors='ignore', downcast='integer')  # cast to numeric datatypes where possible

def prep(df, fix_names=True):
    """prep dataframes"""
    idx = len(df.index.names)
    df = df.reset_index()
    if fix_names:
        df.columns = [c.strip().lower() for c in df.columns]
    return df.apply(to_numeric).set_index(df.columns[:idx].tolist()).copy()
    # return df.apply(to_numeric).convert_dtypes().set_index(df.columns[:idx].tolist()).squeeze().copy()

def run_query(qry):
    res = client.query(qry).result()
    if res.total_rows > 0:
        res = prep(res.to_dataframe())
        if 'geometry' in res.columns:
            geo = gpd.GeoSeries.from_wkt(res['geometry'], crs=CRS['bigquery'])
            res = gpd.GeoDataFrame(res, geometry=geo)
    return res

def delete_table(tbl):
    client.delete_table(tbl, not_found_ok=True)

def copy_dataset(curr, targ):
    curr = 'redistricting-361203.blocks'
    targ = 'redistricting-361203.shapes'
    client.delete_dataset(targ, not_found_ok=True)
    client.create_dataset(targ)
    for t in client.list_tables(curr):
        client.copy_table(t, f'{targ}.{t.table_id}')

def check_table(tbl, overwrite=False):
    if overwrite:
        delete_table(tbl)
    try:
        client.get_table(tbl)
        return True
    except:
        return False

def get_columns(tbl):
    if check_table(tbl):
        return [s.name for s in client.get_table(tbl).schema]

def subquery(qry, indents=1):
    """indent query for inclusion as subquery"""
    s = '\n' + indents * '    '
    return qry.strip(';\n ').replace('\n', s)  # strip leading/trailing whitespace and indent all but first line

def join(parts, sep=', '):
    """ join list into single string """
    return sep.join([str(p) for p in listify(parts)])

def make_select(cols, indents=1, sep=',\n', tbl=None):
    """ useful for select statements """
    cols = listify(cols)
    if tbl is not None:
        cols = [f'{tbl}.{x}' for x in cols]
    qry = join(cols, sep)
    return subquery(qry, indents)

def unzipper(file):
    subprocess.run(['unzip', '-u', '-qq', '-n', file, '-d', file.parent], capture_output=True)

def download(file, url, unzip=True, overwrite=False):
    """Help download data from internet sources"""
    if overwrite:
        file.unlink(missing_ok=True)
    if not file.is_file():  # check if file already exists
        print(f'downloading from {url}', end=elipsis)
        mkdir(file.parent)
        subprocess.run(['wget', '-O', file, url], capture_output=True)
        print('done!')
    if file.suffix == '.zip' and unzip:
        unzipper(file)
    return file

def pq_(tbl):
    subdir, filename = tbl.split('.')[-2:]
    return DATA_PATH / f'{subdir}/{filename}.parquet'

def tbl_(pq):
    dataset, tbl = pq.parts[-2:]
    return f'{dataset}.{tbl}'

def df_to_parquet(df, pq, overwrite=False):
    if overwrite:
        pq.unlink(missing_ok=True)
    if not pq.is_file():
        mkdir(pq.parent)
        prep(df).to_parquet(pq)
    return pq

def parquet_to_df(pq, **kwargs):
    try:
        df = gpd.read_parquet(pq, **kwargs)
    except ValueError:
        df = pd.read_parquet(pq, **kwargs)
    return prep(df)

def query_to_df(qry):
    return run_query(qry)

def query_to_parquet(qry, pq, overwrite=False):
    if not pq.is_file() or overwrite:
        df = query_to_df(qry)
        df_to_parquet(df, pq, overwrite)
    return pq

def query_to_table(qry, tbl, overwrite=False):
    if not check_table(tbl, overwrite=overwrite):
        qry = f"""
create table {tbl} as (
    {subquery(qry)}
)"""
        client.create_dataset(tbl.split('.')[0], exists_ok=True)
        run_query(qry)
    return tbl

def table_to_df(tbl, rows=-1):
    qry = f'select * from {tbl}'
    if rows > 0:
        qry += f' limit {rows}'
    return query_to_df(qry)

def df_to_table(df, tbl, overwrite=False):
    if not check_table(tbl, overwrite=overwrite):
        X = prep(df).reset_index().drop(columns=['index', 'level_0'], errors='ignore')
        client.create_dataset(tbl.split('.')[0], exists_ok=True)
        client.load_table_from_dataframe(X, tbl).result()
    return tbl

def table_to_parquet(tbl, pq=None, overwrite=False):
    pq = pq_(tbl) if pq is None else pq
    if not pq.is_file() or overwrite:
        df = table_to_df(tbl)
        df_to_parquet(df, pq, overwrite)
    return pq

def parquet_to_table(pq, tbl=None, overwrite=False):
    tbl = tbl_(pq) if tbl is None else tbl
    if not check_table(tbl, overwrite):
        df = parquet_to_df(pq)
        df_to_table(df, tbl)
    return tbl

def check_exists(tbl, overwrite=False):
    pq = pq_(tbl)
    if overwrite:
        delete_table(tbl)
        pq.unlink(missing_ok=True)
    if check_table(tbl, overwrite):
        return True
    elif pq.is_file() and not overwrite:
        parquet_to_table(pq, tbl, overwrite)
        return True
    else:
        return False


@Timer()
def get(cols, dataset='acs5', year=2020, level='tract'):
    print(f'fetching data from {dataset}', end=elipsis)
    conn = getattr(census_session, dataset)
    level_alt = level.replace('_',' ')  # census uses space rather then underscore in block_group here - we must handle and replace
    cols = listify(cols)
    df = prep(pd.DataFrame(conn.get([x.upper() for x in cols], year=year,
        geo={'for': level_alt+':*', 'in': f'state:{STATE.fips} county:*'})
        )).rename(columns={level_alt: level})
        
    df['year'] = year
    if year >= 2020:
        geoid = level+'2020'
    else:
        geoid = level+'2010'
    df[geoid] = ''
    for level, k in LEVELS.items():
        if level in df:
            df[geoid] += df[level].astype(str).str.rjust(k, '0')
    # assert not df.isnull().any().any(), 'null values detected'
    print('done!')
    return prep(df[['year', geoid, *cols]])


def compute_other(df, feat):
    try:
        df['other_'+feat] = df['all_'+feat] - df['white_'+feat] - df['hisp_'+feat]
    except KeyError:
        print(f'Can not compute other_{feat}')


def decade_(year):
    return int(year) // 10 * 10


def level_changer(targ, curr='block'):
    d = {'state':2, 'county':5, 'tract':11, 'block_group':12, 'block':15}
    e = d[curr] - d[targ]
    assert e >= 0, f'can not change to small geographic unit'
    return 10 ** e

def transform_labeled(trans, df):
    """apply scikit-learn tranformation and return dataframe with appropriate column names and index"""
    return prep(pd.DataFrame(trans.fit_transform(df), columns=trans.get_feature_names_out(), index=df.index))