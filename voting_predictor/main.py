from . import *
import census, us
elipsis = ' ... '

def unzipper(file):
#     subprocess.run(['unzip', '-u', '-qq', '-n', file, '-d', file.parent], capture_output=True)
    os.system(f'unzip -u -qq -n {file} -d {file.parent}')

def download(file, url, unzip=True, overwrite=False):
    """Help download data from internet sources"""
    if overwrite:
        file.unlink(missing_ok=True)
    if not file.is_file():  # check if file already exists
        print(f'downloading from {url}', end=elipsis)
        ut.mkdir(file.parent)
        subprocess.run(['wget', '-O', file, url], capture_output=True)
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
        tbl = f'crosswalks.{STATE.abbr}'
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'getting {tbl}')
            zip_file = self.data_path / f'TAB2010_TAB2020_ST{STATE.fips}.zip'
            url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
            download(zip_file, url)
            
            
            
        
