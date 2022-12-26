from helpers.common_imports import *
from helpers import utilities as ut
import census, us, geopandas as gpd
from shapely.ops import orient
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.simplefilter(action='ignore', category=FutureWarning)

elipsis = ' ... '
levels = {
    'state':2,
    'county':3,
    'tract':6,
    'block_group':1,
    'block':4,
}
crs = {
    'census'  : 'EPSG:4269'  , # degrees - used by Census
    'bigquery': 'EPSG:4326'  , # WSG84 - used by Bigquery
    'area'    : 'ESRI:102003', # meters
    'length'  : 'ESRI:102005', # meters
}
subpops = {
    'p2_001n': 'all_tot_pop',
    'p4_001n': 'all_vap_pop',
    'p2_005n': 'white_tot_pop',
    'p4_005n': 'white_vap_pop',
    'p2_002n': 'hisp_tot_pop',
    'p4_002n': 'hisp_vap_pop',
}

def get_decade(year):
    return int(year) // 10 * 10

def unzipper(file):
    os.system(f'unzip -u -qq -n {file} -d {file.parent}')

def download(file, url, unzip=True, overwrite=False):
    """Help download data from internet sources"""
    if overwrite:
        file.unlink(missing_ok=True)
    if not file.is_file():  # check if file already exists
        print(f'downloading from {url}', end=elipsis)
        ut.mkdir(file.parent)
        os.system(f'wget -O {file} {url}')
        print('done!')
    if file.suffix == '.zip' and unzip:
        unzipper(file)
    return file

def get_geoid(df, year=2020):
    dec = str(get_decade(year))
    geoid = 'block'+dec
    df[geoid] = ''
    for level, k in levels.items():
        for col in [level, level+dec, level+'_'+dec]:
            if col in df:
                df[geoid] += ut.rjust(df[col], k)
                break
    return df, geoid

@dataclasses.dataclass
class Redistricter():
    census_api_key: str
    bg_project_id: str
    state: str = 'TX'
    root_path:str = '/content/'
    
    def __post_init__(self):
        self.root_path = pathlib.Path(self.root_path)
        self.data_path = self.root_path / 'data'
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.census_session = census.Census(self.census_api_key)
        self.bq = ut.BigQuery(project_id=self.bg_project_id)
        self.state = us.states.lookup(self.state)
        self.tbls = dict()
        
    def get_census(self, fields, dataset='acs5', year=2020, level='tract'):
        conn = getattr(self.census_session, dataset)
        fields = ut.prep(ut.listify(fields), mode='upper')
        if not 'NAME' in fields:
            fields.insert(0, 'NAME')
        level_alt = level.replace('_', ' ')  # census uses space rather then underscore in block_group here - we must handle and replace
        df = ut.prep(pd.DataFrame(
            conn.get(fields=fields, year=year, geo={'for': level_alt+':*', 'in': f'state:{self.state.fips} county:*'})
        )).rename(columns={level_alt: level})
        display(df.head(3))
        df['year'] = year
        df, geoid = get_geoid(df)
        display(df.head(3))

        # if year >= 2020:
        #     geoid = level+'2020'
        # else:
        #     geoid = level+'2010'
        # df[geoid] = ''
        # for level, k in levels.items():
        #     if level in df:
        #         # df[geoid] += df[level].astype(str).str.rjust(k, '0')
        #         df[geoid] += ut.rjust(df[level], k)
        # assert not df.isnull().any().any(), 'null values detected'
        return ut.prep(df[['year', geoid, *ut.prep(fields)]])

    @codetiming.Timer()
    def get_pl(self, year=2020, overwrite=False):
        dec = get_decade(year)
        geoid = f'block{dec}'
        tbl = f'pl.{self.state.abbr}{dec}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        print(f'{tbl}', end=elipsis)
        if not self.bq.get_tbl(tbl, overwrite):
            print('fetching', end=elipsis)
            df = self.get_census(
                fields = ['name', *subpops.keys()],
                dataset = 'pl',
                year = 2020,
                level = 'block')
            df['county'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
            df = df.rename(columns=subpops)[[geoid, 'county', *subpops.values()]]
        return tbl, df


    @codetiming.Timer()
    def get_crosswalks(self, overwrite=False):
        tbl = f'crosswalks.{self.state.abbr}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        print(f'{tbl}', end=elipsis)
        if not self.bq.get_tbl(tbl, overwrite):
            print('fetching', end=elipsis)
            zip_file = self.data_path / f'{attr}/TAB2010_TAB2020_ST{self.state.fips}.zip'
            url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
            download(zip_file, url)
            
            txt = zip_file.with_name(f'{zip_file.stem}_{self.state.abbr}.txt'.lower())
            df = ut.prep(pd.read_csv(txt, sep='|')).rename(columns={'arealand_int': 'aland', 'blk_2010': 'block_2010', 'blk_2020': 'block_2020'})
            for dec in [2010, 2020]:
                df, geoid = get_geoid(df, dec)

                # L = [ut.rjust(df[f'{l}_{yr}'], d) for l, d in levels.items() if l != 'block_group']
                # df[f'block{yr}'] = L[0] + L[1] + L[2] + L[3]
                df[f'prop{dec}'] = df['aland'] / np.fmax(df.groupby(geoid)['aland'].transform('sum'), 1)
            df = ut.prep(df[['block2010', 'block2020', 'aland', 'prop2010', 'prop2020']])
            self.bq.df_to_tbl(df, tbl)
        self.tbls[tbl.split('.')[0]] = tbl
        return tbl

    @codetiming.Timer()
    def get_assignments(self, year=2020, overwrite=False):
        dec = get_decade(year)
        geoid = f'block{dec}'
        tbl = f'assignments.{self.state.abbr}{dec}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        print(f'{tbl}', end=elipsis)
        if not self.bq.get_tbl(tbl, overwrite):
            print('fetching', end=elipsis)
            zip_file = self.data_path / f'{attr}/BlockAssign_ST{self.state.fips}_{self.state.abbr}.zip'
            if dec == 2010:
                url = f'https://www2.census.gov/geo/docs/maps-data/data/baf/{zip_file.name}'
            elif dec == 2020:
                url = f'https://www2.census.gov/geo/docs/maps-data/data/baf{dec}/{zip_file.name}'
            download(zip_file, url)
            dist = {'VTD':f'vtd{dec}', 'CD':f'congress{dec-10}', 'SLDU':f'senate{dec-10}', 'SLDL':f'house{dec-10}'}
            L = []
            for abbr, name in dist.items():
                f = zip_file.parent / f'{zip_file.stem}_{abbr}.txt'
                df = ut.prep(pd.read_csv(f, sep='|'))
                if abbr == 'VTD':
                    # create vtd id using 3 fips + 6 vtd, pad on left with 0 as needed
                    df['district'] = self.state.fips + ut.rjust(df['countyfp'], 3) + ut.rjust(df['district'], 6)
                repl = {'blockid': geoid, 'district':name}
                L.append(df.rename(columns=repl)[repl.values()].set_index(geoid))
            df = pd.concat(L, axis=1)
            self.bq.df_to_tbl(df, tbl)
        return tbl

    @codetiming.Timer()
    def get_shapes(self, year=2020, overwrite=False):
        dec = get_decade(year)
        geoid = f'block{dec}'
        tbl = f'shapes.{self.state.abbr}{dec}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        print(f'{tbl}', end=elipsis)
        if not self.bq.get_tbl(tbl, overwrite):
            print('fetching', end=elipsis)
            d = dec % 100
            zip_file = self.data_path / f'tl_{dec}_{self.state.fips}_tabblock{d}.zip'
            if dec == 2010:
                url = f'https://www2.census.gov/geo/tiger/TIGER{dec}/TABBLOCK/{dec}/{zip_file.name}'
            elif dec == 2020:
                url = f'https://www2.census.gov/geo/tiger/TIGER{dec}/TABBLOCK{d}/{zip_file.name}'
            repl = {'geoid{d}':geoid, 'aland{d}': 'aland', 'awater{d}': 'awater', 'geometry':'geometry',}
            download(zip_file, url, unzip=False)
            repl = {'geoid20':geoid, 'aland20': 'aland', 'awater20': 'awater', 'geometry':'geometry',}
            df = ut.prep(gpd.read_file(zip_file))[repl.keys()].rename(columns=repl).to_crs(crs['bigquery'])
            df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
            self.bq.df_to_tbl(df, tbl)
        return tbl




    @codetiming.Timer()
    def combine(self, overwrite=False):
        tbl = f'combine.{self.state.abbr}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        if not self.bq.get_tbl(tbl, overwrite):
            self.get_assignments()
            self.get_shapes()
            print(f'getting {tbl}')
            qry = f"""
select
    *
--    A.*,
    --B.* (except {geoid}),
from
    {self.tbls['assignments']} as A
inner join
    {self.tbls['shapes']} as S
using
    {geoid}
"""
            self.bq.qry_to_tbl(qry, tbl)
        return tbl


    # def get_shapes(self, overwrite=False):
    #     attr = 'shapes'
    #     tbl = f'{attr}.{self.state.abbr}'
    #     self.tbls[attr] = tbl
    #     if not self.bq.get_tbl(tbl, overwrite):
