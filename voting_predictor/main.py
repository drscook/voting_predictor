from helpers.common_imports import *
from helpers import utilities as ut
import census, us, mechanicalsoup, geopandas as gpd
from .constants import *
from shapely.ops import orient
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.filterwarnings('ignore', message='.*DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.*')
warnings.simplefilter(action='ignore', category=FutureWarning)
 

def get_decade(year):
    return int(year) // 10 * 10

def download(file, url, unzip=True, overwrite=False):
    """Help download data from internet sources"""
    if overwrite:
        file.unlink(missing_ok=True)
    if not file.is_file():  # check if file already exists
        print(f'downloading from {url}', end=ellipsis)
        ut.mkdir(file.parent)
        os.system(f'wget -O {file} {url}')
        print('done!')
    if unzip:
        ut.unzipper(file)
    return file

def get_geoid(df, year=2020, level='block'):
    decade = str(get_decade(year))
    geoid = level+decade
    df[geoid] = ''
    for level, k in levels.items():
        for col in [level, level+'_'+decade]:
            if col in df.columns:
                df[geoid] += ut.rjust(df[col], k)
                break
    # df[geoid] = ut.prep(df[geoid])
    return geoid

def compute_other(df, feat):
    try:
        df['other_'+feat] = df['all_'+feat] - df['white_'+feat] - df['hisp_'+feat]
    except KeyError:
        print(f'Can not compute other_{feat}', end=ellipsis)

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

        
    def fetch_census(self, fields, dataset='acs5', year=2020, level='tract'):
        conn = getattr(self.census_session, dataset)
        fields = ut.prep(ut.listify(fields), mode='upper')
        if not 'NAME' in fields:
            fields.insert(0, 'NAME')
        level_alt = level.replace('_', ' ')  # census uses space rather then underscore in block_group here - we must handle and replace
        df = ut.prep(pd.DataFrame(
            conn.get(fields=fields, year=year, geo={'for': level_alt+':*', 'in': f'state:{self.state.fips} county:*'})
        )).rename(columns={level_alt: level})
        df['year'] = year
        geoid = get_geoid(df, year=year, level=level)
        return ut.prep(df[['year', geoid, *ut.prep(fields)]])


    def parse(self, tbl):
        attr = tbl.split('.')[0]
        self.tbls[attr] = tbl
        path = self.data_path / attr

        g = tbl.split('_')[-1]
        level, year = g[:-4], int(g[-4:])
        decade = get_decade(year)
        geoid = f'{level}{decade}'
        
        return path, geoid, level, year, decade
        

    @codetiming.Timer()
    def transform_acs5(self, level='tract', year=2018, targ='vtd2020', overwrite=False):
        tbl_raw = self.get_acs5(level=level, year=year)
        path, geoid, level, year, decade = self.parse(tbl_raw)
        tbl = tbl_raw + '_' + targ
        if not self.bq.get_tbl(tbl, overwrite):
            qry = f"""
select
    year,
    {targ},
    county2020,
    {ut.make_select([f'sum({x}) over (partition by {targ}) as {x}' for x in features.keys()])},
    row_number() over (partition by {targ} order by all_tot_pop desc) as r,
from (
    select
        A.year,
        T.{targ},
        T.county2020,
        {ut.make_select([f'sum(A.{x} * T.{x[:x.rfind("_")]}_pop) as {x}' for x in features.keys()], 2)},
    from
        {tbl_raw} as A
    inner join
        {self.get_transformer(level=level, year=year)} as T
    using
        ({geoid})
    group by
        1, 2, 3
    ) as B
qualify
    r = 1
"""
            print(qry)
            self.bq.qry_to_tbl(qry, tbl)
        print(tbl, end=ellipsis)
        return tbl


    @codetiming.Timer()
    def get_acs5(self, level='tract', year=2018, overwrite=False):
        tbl = f'acs5.{self.state.abbr}_{level}{year}'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            base = set().union(*features.values())
            survey = {x for x in base if x[0]=='s'}
            base = base.difference(survey)
            B = self.fetch_census(fields=base  , dataset='acs5'  , year=year, level=level)
            S = self.fetch_census(fields=survey, dataset='acs5st', year=year, level=level)
            df = ut.prep(B.merge(S, on=['year', geoid]))
            for name, fields in features.items():
                df[name] = df[fields].sum(axis=1)
            df = df[['year', geoid, *features.keys()]]
            for var in {name[name.find('_')+1:] for name in features.keys()}:
                compute_other(df, var)
            self.bq.df_to_tbl(df, tbl)
        print(tbl, end=ellipsis)
        return tbl

    @codetiming.Timer()
    def get_transformer(self, level='tract', year=2018, overwrite=False):
        tbl = f'transformers.{self.state.abbr}_{level}{year}'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            qry = f"""
select
    A.{geoid},
    A.block2020,
    A.block_group2020,
    A.tract2020,
    A.vtd2020,
    A.county2020,
    {ut.make_select([f'case when B.{x} = 0 then 0 else A.{x} / B.{x} end as {x}' for x in subpops.keys()])},
from
    {self.get_crosswalks()} as A
inner join (
    select
        {geoid},
        {ut.make_select([f'sum({x}) as {x}' for x in subpops.keys()], 2)},
    from
        {self.get_crosswalks()}
    group by
        {geoid}
    ) as B
using
    ({geoid})
"""
            # print(qry)
            self.bq.qry_to_tbl(qry, tbl)
        print(tbl, end=ellipsis)
        return tbl


    @codetiming.Timer()
    def get_crosswalks(self, overwrite=False):
        tbl = f'crosswalks.{self.state.abbr}_block2020'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            tbl_raw = tbl+'_raw'
            if not self.bq.get_tbl(tbl_raw, overwrite):
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
            qry = f"""
select
    C.*,
    div(C.block2010, 1000) as block_group2010,
    div(C.block2020, 1000) as block_group2020,
    div(C.block2010, 10000) as tract2010,
    div(C.block2020, 10000) as tract2020,
    A.vtd2010,
    G.vtd2020,
    G.county2020,
    {ut.make_select([f'C.aprop2020 * G.{subpop} as {subpop}' for subpop in subpops.keys()])}
from
    {tbl_raw} as C
left join
    {self.get_geo()} as G
using
    (block2020)
left join
    {self.get_assignments(year=2010)} as A
using
    (block2010)
"""
            # print(qry)
            self.bq.qry_to_tbl(qry, tbl)
        print(tbl, end=ellipsis)
        return tbl


    @codetiming.Timer()
    def get_geo(self, overwrite=False):
        tbl = f'geo.{self.state.abbr}_block2020'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            qry = f"""
select
    * except (geometry),
    case when S.perim < 0.1 then 0 else 4 * {np.pi} * S.atot / (S.perim * S.perim) end as polsby_popper,
    S.geometry,
from
    {self.get_pl()} as P
left join
    {self.get_assignments()} as A
using
    ({geoid})
left join (
    select
        *,
        st_distance(geometry, (select st_boundary(us_outline_geom) from bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
        st_area(geometry) as atot,
        st_perimeter(geometry) as perim,
    from
        {self.get_shapes()}
    ) as S
using
    ({geoid})
"""
            # print(qry)
            self.bq.qry_to_tbl(qry, tbl)
        print(tbl, end=ellipsis)
        return tbl


    @codetiming.Timer()
    def get_plans(self, overwrite=False):
        if self.state.abbr != 'TX':
            return False
        tbl = f'plans.{self.state.abbr}_block{year}'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
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
                                zip_file = path / url.split('/')[-1]
                                download(zip_file, url)
                    if not_found > 15:
                        break
            for file in path.iterdir():
                if file.suffix == '.zip':
                    unzipper(file)
            L = []
            for file in path.iterdir():
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





    @codetiming.Timer()
    def get_assignments(self, year=2020, overwrite=False):
        tbl = f'assignments.{self.state.abbr}_block{year}'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            zip_file = path / f'BlockAssign_ST{self.state.fips}_{self.state.abbr}.zip'
            if decade == 2010:
                url = f'https://www2.census.gov/geo/docs/maps-data/data/baf/{zip_file.name}'
            elif decade == 2020:
                url = f'https://www2.census.gov/geo/docs/maps-data/data/baf{decade}/{zip_file.name}'
            download(zip_file, url)

            dist = {'VTD':f'vtd{decade}', 'CD':f'congress{decade-10}', 'SLDU':f'senate{decade-10}', 'SLDL':f'house{decade-10}'}
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
        print(tbl, end=ellipsis)
        return tbl


    @codetiming.Timer()
    def get_pl(self, overwrite=False):
        tbl = f'pl.{self.state.abbr}_block2020'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            repl = {v:k for k,v in subpops.items() if v}
            df = self.fetch_census(fields=['name', *repl.keys()], dataset='pl', year=year, level='block')
            df['county2020'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
            df = df.rename(columns=repl)[[geoid, *repl.values(), 'county2020']]
            compute_other(df, 'tot_pop')
            compute_other(df, 'vap_pop')
            self.bq.df_to_tbl(df, tbl)
        print(tbl, end=ellipsis)
        return tbl


    @codetiming.Timer()
    def get_shapes(self, overwrite=False):
        tbl = f'shapes.{self.state.abbr}_block2020'
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite):
            d = decade % 100
            zip_file = path / f'tl_{decade}_{self.state.fips}_tabblock{d}.zip'
            if decade == 2010:
                url = f'https://www2.census.gov/geo/tiger/TIGER{decade}/TABBLOCK/{decade}/{zip_file.name}'
            elif decade == 2020:
                url = f'https://www2.census.gov/geo/tiger/TIGER{decade}/TABBLOCK{d}/{zip_file.name}'
            download(zip_file, url, unzip=False)

            repl = {f'geoid{d}':geoid, f'aland{d}': 'aland', f'awater{d}': 'awater', 'geometry':'geometry',}
            df = ut.prep(gpd.read_file(zip_file)).rename(columns=repl)[repl.values()].to_crs(crs['bigquery'])
            df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
            self.bq.df_to_tbl(df, tbl)
        print(tbl, end=ellipsis)
        return tbl