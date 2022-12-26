from helpers.common_imports import *
from helpers import utilities as ut
import census, us, geopandas as gpd
from shapely.ops import orient
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.simplefilter(action='ignore', category=FutureWarning)

geoid = 'block2020'
elipsis = ' ... '
levels = {
    'state':2,
    'county':3,
    'tract':6,
    'block_group':1,
    'block':4,
}
CRS = {
    'census'  : 'EPSG:4269'  , # degrees - used by Census
    'bigquery': 'EPSG:4326'  , # WSG84 - used by Bigquery
    'area'    : 'ESRI:102003', # meters
    'length'  : 'ESRI:102005', # meters
}

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
        self.census = census.Census(self.census_api_key)
        self.bq = ut.BigQuery(project_id=self.bg_project_id)
        self.state = us.states.lookup(self.state)
        self.tbls = dict()
        
    def get_crosswalks(self, overwrite=False):
        tbl = f'crosswalks.{self.state.abbr}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'getting {tbl}')
            zip_file = self.data_path / f'{attr}/TAB2010_TAB2020_ST{self.state.fips}.zip'
            url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
            download(zip_file, url)
            txt = zip_file.with_name(f'{zip_file.stem}_{self.state.abbr}.txt'.lower())
            df = ut.prep(pd.read_csv(txt, sep='|')).rename(columns={'arealand_int': 'aland', 'blk_2010': 'block_2010', 'blk_2020': 'block_2020'})
            for yr in [2010, 2020]:
                L = [ut.rjust(df[f'{l}_{yr}'], d) for l, d in levels.items() if l != 'block_group']
                df[f'block{yr}'] = L[0] + L[1] + L[2] + L[3]
                df[f'prop{yr}'] = df['aland'] / np.fmax(df.groupby(f'block{yr}')['aland'].transform('sum'), 1)
            df = ut.prep(df[['block2010', 'block2020', 'aland', 'prop2010', 'prop2020']])
            self.bq.df_to_tbl(df, tbl)
        self.tbls[tbl.split('.')[0]] = tbl
        return tbl

    def get_assignments(self, overwrite=False):
        tbl = f'assignments.{self.state.abbr}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'getting {tbl}')
            zip_file = self.data_path / f'{attr}/BlockAssign_ST{self.state.fips}_{self.state.abbr}.zip'
            url = f'https://www2.census.gov/geo/docs/maps-data/data/baf2020/{zip_file.name}'
            download(zip_file, url)
            d = {'VTD':'vtd2020', 'CD':'congress2010', 'SLDU':'senate2010', 'SLDL':'house2010'}
            L = []
            for abbr, name in d.items():
                f = zip_file.parent / f'{zip_file.stem}_{abbr}.txt'
                df = ut.prep(pd.read_csv(f, sep='|'))
                if abbr == 'VTD':
                    # create vtd id using 3 fips + 6 vtd, pad on left with 0 as needed
                    df['district'] = self.state.fips + ut.rjust(df['countyfp'], 3) + ut.rjust(df['district'], 6)
                repl = {'blockid': geoid, 'district':name}
                L.append(df[repl.keys()].rename(columns=repl).set_index(geoid))
            df = pd.concat(L, axis=1)
            self.bq.df_to_tbl(df, tbl)
        return tbl

    def get_shapes(self, overwrite=False):
        tbl = f'shapes.{self.state.abbr}'
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'getting {tbl}')
            zip_file = self.data_path / f'tl_2020_{self.state.fips}_tabblock20.zip'
            url = f'https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/{zip_file.name}'
            download(zip_file, url, unzip=False)
            repl = {'geoid20':geoid, 'aland20': 'aland', 'awater20': 'awater', 'geometry':'geometry',}
            df = ut.prep(gpd.read_file(zip_file))[repl.keys()].rename(columns=repl).to_crs(CRS['bigquery'])
            df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
            # self.bq.df_to_tbl(df, tbl)
        return tbl, df
    



    # def get_shapes(self, overwrite=False):
    #     attr = 'shapes'
    #     tbl = f'{attr}.{self.state.abbr}'
    #     self.tbls[attr] = tbl
    #     if not self.bq.get_tbl(tbl, overwrite):
