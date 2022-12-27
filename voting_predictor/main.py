from helpers.common_imports import *
from helpers import utilities as ut
import census, us, geopandas as gpd
from .constants import *
from shapely.ops import orient
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.simplefilter(action='ignore', category=FutureWarning)


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

def get_geoid(df, year=2020, level='block'):
    dec = str(get_decade(year))
    geoid = level+dec
    df[geoid] = ''
    for level, k in levels.items():
        for col in [level, level+'_'+dec]:
            if col in df.columns:
                df[geoid] += ut.rjust(df[col], k)
                break
    # df[geoid] = ut.prep(df[geoid])
    return geoid

def compute_other(df, feat):
    try:
        df['other_'+feat] = df['all_'+feat] - df['white_'+feat] - df['hisp_'+feat]
    except KeyError:
        print(f'Can not compute other_{feat}')

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
        self.features = ut.prep(features)

        
    def fetch_census(self, fields, dataset='acs5', year=2020, level='tract'):
        conn = getattr(self.census_session, dataset)
        fields = ut.prep(ut.listify(fields), mode='upper')
        if not 'NAME' in fields:
            fields.insert(0, 'NAME')
        level_alt = level.replace('_', ' ')  # census uses space rather then underscore in block_group here - we must handle and replace
        
        # opts = {'fields':fields, 'year':year, 'geo':{'for': level_alt+':*', 'in': f'state:{self.state.fips} county:*'}}
        # print(opts)

        # df = ut.prep(pd.DataFrame(conn.get(**opts))).rename(columns={level_alt: level})


        df = ut.prep(pd.DataFrame(
            conn.get(fields=fields, year=year, geo={'for': level_alt+':*', 'in': f'state:{self.state.fips} county:*'})
        )).rename(columns={level_alt: level}).copy()
        df['year'] = year
        geoid = get_geoid(df, year=year, level=level)
        return ut.prep(df[['year', geoid, *ut.prep(fields)]])

    def get_path(self, tbl):
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        return self.data_path / attr


    def get_acs5(self, year=2018, level='tract', overwrite=False):
        dec = get_decade(year)
        geoid = f'{level}{dec}'
        tbl = f'acs5.{self.state.abbr}{year}'
        path = self.get_path(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            # print(f'{tbl} fetching', end=elipsis)

            tbl_raw = tbl+'_raw'
            if not self.bq.get_tbl(tbl_raw, overwrite):
                print(f'{tbl_raw} fetching', end=elipsis)

                base = set().union(*self.features.values())
                survey = {x for x in base if x[0]=='s'}
                base = base.difference(survey)
                B = self.fetch_census(fields=base  , dataset='acs5'  , year=year, level=level)
                S = self.fetch_census(fields=survey, dataset='acs5st', year=year, level=level)
                self.B = B
                self.S = S
                df = B.merge(S, on=['year', geoid])
                for name, fields in self.features.items():
                    df[name] = df[fields].sum(axis=1)
                df = ut.prep(df[[geoid, 'year', *self.features.keys()]])
                for var in {name[name.find('_')+1:] for name in self.features.keys()}:
                    compute_other(df, var)
                self.bq.df_to_tbl(df, tbl_raw)
        #     self.bq.qry_to_tbl(qry, tbl)
        # print(tbl, end=elipsis)
        return tbl


    @codetiming.Timer()
    def get_crosswalks(self, overwrite=False):
        tbl = f'crosswalks.{self.state.abbr}'
        geoid = f'block2020'
        path = self.get_path(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            tbl_raw = tbl+'_raw'
            if not self.bq.get_tbl(tbl_raw, overwrite):
                print(f'{tbl_raw} fetching', end=elipsis)
                zip_file = path / f'TAB2010_TAB2020_ST{self.state.fips}.zip'
                url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
                download(zip_file, url)
            
                txt = zip_file.with_name(f'{zip_file.stem}_{self.state.abbr}.txt'.lower())
                df = ut.prep(pd.read_csv(txt, sep='|')).rename(columns={'arealand_int': 'aland', 'blk_2010': 'block_2010', 'blk_2020': 'block_2020'})
                for dec in [2010, 2020]:
                    geoid = get_geoid(df, dec)
                    df[f'aprop{dec}'] = df['aland'] / np.fmax(df.groupby(geoid)['aland'].transform('sum'), 1)
                df = ut.prep(df[['block2010', 'block2020', 'aland', 'aprop2010', 'aprop2020']])
                self.bq.df_to_tbl(df, tbl_raw)
            self.get_geo()
            print(f'{tbl} building', end=elipsis)
            qry = f"""
select
    A.*,
    {ut.make_select([f'aprop2020 * {subpop} as {subpop}' for subpop in [*subpops.values(), 'other_tot_pop', 'other_vap_pop']])}
from
    {tbl_raw} as A
inner join
    {self.tbls['geo']} as G
using
    ({geoid})
"""
            self.bq.qry_to_tbl(qry, tbl)
        print(tbl, end=elipsis)
        return tbl


    @codetiming.Timer()
    def get_geo(self, overwrite=False):
        dec = 2020
        geoid = f'block{dec}'
        tbl = f'geo.{self.state.abbr}{dec}'
        path = self.get_path(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            self.get_pl()
            self.get_assignments()
            self.get_shapes()
            print(f'{tbl} fetching', end=elipsis)
            qry = f"""
select
    * except (geometry),
    case when perim < 0.1 then 0 else 4 * {np.pi} * atot / (perim * perim) end as polsby_popper,
    geometry,
from
    {self.tbls['pl']} as P
inner join
    {self.tbls['assignments']} as A
using
    ({geoid})
inner join (
    select
        *,
        st_distance(geometry, (select st_boundary(us_outline_geom) from bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
        st_area(geometry) as atot,
        st_perimeter(geometry) as perim,
    from
        {self.tbls['shapes']}
    ) as S
using
    ({geoid})
"""
            self.bq.qry_to_tbl(qry, tbl)
        return tbl


    @codetiming.Timer()
    def get_shapes(self, overwrite=False):
        dec = 2020
        geoid = f'block{dec}'
        tbl = f'shapes.{self.state.abbr}{dec}'
        path = self.get_path(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'{tbl} fetching', end=elipsis)
            d = dec % 100
            zip_file = path / f'tl_{dec}_{self.state.fips}_tabblock{d}.zip'
            if dec == 2010:
                url = f'https://www2.census.gov/geo/tiger/TIGER{dec}/TABBLOCK/{dec}/{zip_file.name}'
            elif dec == 2020:
                url = f'https://www2.census.gov/geo/tiger/TIGER{dec}/TABBLOCK{d}/{zip_file.name}'
            download(zip_file, url, unzip=False)

            repl = {'geoid{d}':geoid, 'aland{d}': 'aland', 'awater{d}': 'awater', 'geometry':'geometry',}
            df = ut.prep(gpd.read_file(zip_file)).rename(columns=repl)[repl.values()].to_crs(crs['bigquery'])
            df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
            self.bq.df_to_tbl(df, tbl)
        print(tbl, end=elipsis)
        return tbl


    @codetiming.Timer()
    def get_assignments(self, overwrite=False):
        dec = 2020
        geoid = f'block{dec}'
        tbl = f'assignments.{self.state.abbr}{dec}'
        path = self.get_path(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'{tbl} fetching', end=elipsis)
            zip_file = path / f'BlockAssign_ST{self.state.fips}_{self.state.abbr}.zip'
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
        print(tbl, end=elipsis)
        return tbl


    @codetiming.Timer()
    def get_pl(self, overwrite=False):
        dec = 2020
        geoid = f'block{dec}'
        tbl = f'pl.{self.state.abbr}{dec}'
        path = self.get_path(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            print(f'{tbl} fetching', end=elipsis)
            df = self.fetch_census(fields=['name', *subpops.keys()], dataset='pl', year=dec, level='block')
            df['county'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
            df = df.rename(columns=subpops)[[geoid, 'county', *subpops.values()]]
            compute_other(df, 'tot_pop')
            compute_other(df, 'vap_pop')
            self.bq.df_to_tbl(df, tbl)
        print(tbl, end=elipsis)
        return tbl