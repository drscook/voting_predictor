from . import *
import census, us
elipsis = ' ... '
levels = {
    'state':2,
    'county':3,
    'tract':6,
    'block_group':1,
    'block':4,
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
        
    def get_crosswalks(self, overwrite=False):
        tbl = f'crosswalks.{self.state.abbr}'
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'getting {tbl}')
            zip_file = self.data_path / f'crosswalks/TAB2010_TAB2020_ST{self.state.fips}.zip'
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
        return tbl