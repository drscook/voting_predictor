from helpers.common_imports import *
from helpers import utilities as ut
import census, us, mechanicalsoup, geopandas as gpd
from .constants import *
from shapely.ops import orient
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.filterwarnings('ignore', message='.*DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.*')
warnings.simplefilter(action='ignore', category=FutureWarning)

def download(file, url, unzip=True, overwrite=False):
    """Help download data from internet sources"""
    if overwrite:
        file.unlink(missing_ok=True)
    if not file.is_file():  # check if file already exists
        print(f'downloading from {url}', end=ellipsis)
        ut.mkdir(file.parent)
        os.system(f'wget -O {file} {url}')
        if unzip:
            ut.unzipper(file)
        print('done', end=ellipsis)
    return file


@dataclasses.dataclass
class Voting():
    census_api_key: str
    bq_project_id: str
    state: str = 'TX'
    geoid: str = 'vtd2022'
    root_path:str = '/content/'
    refresh: tuple = () 
    
    def split_geoid(self, geoid):
        return geoid[:-4], int(geoid[-4:])

    def __post_init__(self):
        self.level, self.year = self.split_geoid(self.geoid)
        self.root_path = pathlib.Path(self.root_path)
        self.data_path = self.root_path / 'data'
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.census_session = census.Census(self.census_api_key)
        self.bq = ut.BigQuery(project_id=self.bq_project_id)
        self.state = us.states.lookup(self.state)
        self.tbls = set()
        self.levels = {'state':2, 'county':5, 'tract':11, 'blockgroup':12, 'block':15}
        dependencies = {
            'plan':'intersection',
            'census':'intersection',
            'shape':'intersection',
            'crosswalk':'intersection',
            'intersection':{'geo', 'acs'},
            'geo':'acs',
            'acs_src':'acs',
            'acs':'final',
            'election':'final',
            'final':set()}
        self.refresh = ut.setify(self.refresh)
        l = 0
        while l < len(self.refresh):
            l = len(self.refresh)
            for attr in self.refresh.copy():
                self.refresh.add(attr)
                try:
                    self.refresh.update(ut.setify(dependencies[attr]))
                except KeyError:
                    print(f'unknown attribute {attr} ... must be {list(dependencies.keys())}')

    def rpt(self, tbl):
        print('creating', tbl, end=ellipsis)
    
    def get_decade(self, x):
        try:
            return int(x) // 10 * 10
        except:
            level, year = self.split_geoid(x)
            if level == 'vtd':
                return x
            else:
                return f'{level}{self.get_decade(year)}'
    
    def parse(self, tbl):
        attr = tbl.split('.')[0]
        path = self.data_path / attr
        geoid = tbl.split('_')[-1]
        level, year = self.split_geoid(geoid)
        decade = self.get_decade(year)
        return path, level, year, decade

    def get_geoid(self, df, level='block', year=2020):
        decade = self.get_decade(year)
        geoid = f'{level}{decade}'
        k = self.levels[level]
        df[geoid] = ''
        i = 0
        for lev, j in self.levels.items():
            for col in [lev, f'{lev}_{decade}']:
                if (col in df.columns) & (j <= k):
                    df[geoid] += ut.rjust(df[col], j-i)
                    break
            i = j
        return geoid

    def fetch_census(self, fields, dataset='acs5', level='tract', year=2020):
        conn = getattr(self.census_session, dataset)
        fields = ut.prep(ut.listify(fields), mode='upper')
        if not 'NAME' in fields:
            fields.insert(0, 'NAME')
        level_alt = level.replace('_', ' ')  # census uses space rather then underscore in block_group here - we must handle and replace
        df = ut.prep(pd.DataFrame(
            conn.get(fields=fields, year=year, geo={'for': level_alt+':*', 'in': f'state:{self.state.fips} county:*'})
        )).rename(columns={level_alt: level})
        df['year'] = year
        geoid = self.get_geoid(df, level=level, year=year)
        return ut.prep(df[['year', geoid, *ut.prep(fields)]])

    def compute_other(self, df, feat):
        feat = feat.replace('all', '').replace('hisp', '').replace('white', '').replace('other', '')
        try:
            df[feat+'other'] = df[feat+'all'] - df[feat+'hisp'] - df[feat+'white'] 
        except KeyError:
            print(f'Can not compute other{feat}', end=ellipsis)

    def qry_to_tbl(self, qry, tbl, show=False):
        with Timer():
            self.rpt(tbl)
            if show:
                print(qry)
            self.bq.qry_to_tbl(qry, tbl)
            self.tbls.add(tbl)
    
    def df_to_tbl(self, df, tbl, cols=None):
        cols = cols if cols else df.columns
        cols = [x for x in ut.listify(cols) if x in df.columns]
        self.bq.df_to_tbl(ut.prep(df[cols]), tbl)
        self.tbls.add(tbl)


    def get_final(self):
        if (self.state.abbr != 'TX') or (self.level != 'vtd'):
            return False
        attr = 'final'
        geoid = self.geoid
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            qry = f"""
select campaign, string_agg(candidate, " v " order by party) as candidates,
from (
    select distinct
        office||"_"||year as campaign,
        name||"_"||party as candidate,
        party,
    from {self.get_election()}
    where party in ("D", "R") and year >= 2015 and office in ("Governor", "USSen", "President", "Comptroller", "AttorneyGen", "LtGovernor") and election = "general"
) group by campaign"""
            campaigns = ut.listify(self.bq.qry_to_df(qry))
            L = []
            for campaign, candidates in campaigns:
                office, year = campaign.split('_')
                year = min(int(year), datetime.date.today().year-2)
                qry = f"""
select
    *,
    dem_votes / greatest(1, tot_votes) as dem_prop,
    rep_votes / greatest(1, tot_votes) as rep_prop,
from (
    select 
        * except (D, R),
        "{campaign}" as campaign,
        "{candidates}" as candidates,
        coalesce(B.D, 0) as dem_votes,
        coalesce(B.R, 0) as rep_votes,
        coalesce(B.D, 0) + coalesce(B.R, 0) as tot_votes,
    from {self.get_acs(year)} as A
    left join (
        select {geoid}, party, votes,
        from {self.get_election()}
        where office = "{office}" and year = {year})
        pivot(sum(votes) for party in ("D", "R")
    ) as B using ({geoid}))"""
                L.append(qry)   
            qry = ut.join(L, '\nunion all\n')
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_election(self):
        if (self.state.abbr != 'TX') or (self.level != 'vtd'):
            return False
        attr = 'election'
        tbl = f'{attr}.{self.state.abbr}_{self.geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, level, year, decade = self.parse(tbl)
            with Timer():
                self.rpt(tbl)
                zip_file = path / f'2022-general-vtds-election-data.zip'                
                url = f'https://data.capitol.texas.gov/dataset/35b16aee-0bb0-4866-b1ec-859f1f044241/resource/b9ebdbdb-3e31-4c98-b158-0e2993b05efc/download/{zip_file.name}'
                download(zip_file, url)
                L = []
                cols = [self.geoid, 'office', 'year', 'election', 'name', 'party', 'incumbent', 'votes']                    
                for file in path.iterdir():
                    a = ut.prep(file.stem.split('_'))
                    if ('general' in a) & ('returns' in a):
                        df = ut.prep(pd.read_csv(file)).rename(columns={'vtdkeyvalue':self.geoid})
                        mask = (df['votes'] > 0) & (df['party'].isin(('R', 'D', 'L', 'G')))
                        if mask.any():
                            repl = {(' ', '.', ','): ''}
                            df['office'] = ut.replace(df['office'], repl)
                            df['year'] = int(a[0])
                            df['election'] = ut.join(a[1:-2], '_')
                            df['name'] = ut.replace(df['name'], repl)
                            df['incumbent'] = df['incumbent'] == 'Y'
                            L.append(df.loc[mask, cols])
                df = ut.prep(pd.concat(L, axis=0)).reset_index(drop=True)
                self.df_to_tbl(df, tbl)
        return tbl


    def get_acs(self, year=2018, level='tract', geoid='vtd2022'):
        attr = 'acs'
        attr_src = f'{attr}_src'
        tbl_src  = f'{attr}.{self.state.abbr}_{level}{year}'
        tbl = f'{tbl_src}_{geoid}'
        path, level, year, decade = self.parse(tbl_src)
        geoid_src = f'{level}{decade}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            if not self.bq.get_tbl(tbl_src, overwrite=(attr_src in self.refresh) & (tbl_src not in self.tbls)):
                with Timer():
                    self.rpt(tbl_src)
                    base = set().union(*features.values())
                    survey = {x for x in base if x[0]=='s'}
                    base = base.difference(survey)
                    B = self.fetch_census(fields=base  , dataset='acs5'  , year=year, level=level)
                    S = self.fetch_census(fields=survey, dataset='acs5st', year=year, level=level)
                    df = ut.prep(B.merge(S, on=['year', geoid_src]))
                    for name, fields in features.items():
                        df[name] = df[fields].sum(axis=1)
                    df = df[['year', geoid_src, *sorted(features.keys())]]
                    for x in features.keys():
                        if 'all' in x:
                            self.compute_other(df, x)
                    self.df_to_tbl(df, tbl_src)
            feat = self.bq.get_cols(tbl_src)[2:]
#             feat_all = [x for x in feat if x[:3] == 'all']
#             feat_grp = [x for x in feat if x not in feat_all]
#             feat_grp = [x for x in feat if x[:3] != 'all']
#             feat_all = [x[4:] for x in feat if x not in feat_grp]
            
#             feat_geo = ['aland', 'awater', 'atot', 'perim', 'polsby_popper']
#             f = lambda x: x[:ut.findn(x, '_', 2)]+'_pop'
            f = lambda x: 'pop'+x[x.find('_'):]
            sel_grp = ut.make_select([f'sum(A.{x} * B.{f(x)} / greatest(1, C.{f(x)})) as {x}' for x in feat if not "all" in x])
#             sel_geo  = ut.make_select([f'min(C.{x}) as {x}' for x in feat_geo])
            sel_geo  = ut.make_select([f'min(C.{x}) as {x}' for x in ['aland', 'awater', 'atot', 'perim', 'polsby_popper']])
            qry = f"""
select
    A.year,
    B.{geoid},
    B.county,
    {sel_grp},
    {sel_geo},
from {tbl_src} as A
join {self.get_intersection()} as B using ({geoid_src})
join {self.get_geo(geoid_src)} as C using ({geoid_src})
group by 1,2,3"""
            sel_all = ut.make_select([f'{x.replace("all", "hisp")} + {x.replace("all", "other")} + {x.replace("all", "white")} as {x}' for x in feat if "all" in x])
            qry = f"""
select
    *,
    {sel_all},
from (
    {ut.subquery(qry)})"""
# select
#     *,
#     {sel_all},
# from (
#     {ut.subquery(qry)})"""

            
# #             qry = f"""
# select
#     year,
#     {geoid},
#     county,
#     {sel_all},
#     * except (year, {geoid}, county),
# from (
#     {ut.subquery(qry)})"""
            
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_geo(self, geoid='tract2010'):
        attr = 'geo'
        geoid = self.get_decade(geoid)
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            sel_pop = ut.make_select([f'sum({x}) as {x}' for x in subpops.keys()])
            sel_den = ut.make_select([f'sum({x}) / greatest(1, sum(aland)) as {x.replace("pop", "den")}' for x in subpops.keys()])
            sel_geo = ut.make_select([f'sum({x}) as {x}' for x in ['aland', 'awater', 'atot']])
            qry = f"""
select
    {geoid},
    {sel_pop},
    {sel_den},
    min(dist_to_border) as dist_to_border,
    {sel_geo},
    st_union_agg(geometry) as geometry,
from {self.get_intersection()}
group by {geoid}"""
            f = lambda x: f'join (select {geoid}, {x}, sum(all_tot_pop) as p from {self.get_intersection()} group by {geoid}, {x} qualify row_number() over (partition by {geoid} order by p desc) = 1) as {x}_tbl using ({geoid})'
            plan = ['county', *self.bq.get_cols(self.get_plan())[1:]]
            join_plan = ut.make_select([f(x) for x in plan], 2, '\n')
            sel_plan  = ut.join(plan)
            sel_den = ut.make_select([f'{x} / greatest(1, aland) as {x.replace("pop", "den")}' for x in subpops.keys()])
            qry = f"""
select
    {geoid},
    {sel_plan},
    A.* except ({geoid}),
    st_perimeter(geometry) / 1000 as perim,
from (
    {ut.subquery(qry)}
) as A
{join_plan}"""
            qry = f"""
select
    * except (geometry),
    case when perim < 0.1 then 0 else 4 * {np.pi} * atot / (perim * perim) end as polsby_popper,
    geometry,
from (
    {ut.subquery(qry)})"""

            
#             qry = f"""
# select * except (geometry), case when perim < 0.1 then 0 else 4 * {np.pi} * atot / (perim * perim) end as polsby_popper, geometry,
# from (
#     select *, st_perimeter(geometry) / 1000 as perim,
#     from (
#         select
#             {geoid},
#             {sel_plan},
#             A.* except ({geoid}),
#         from (
#             select
#                 {geoid},
#                 {sel_sum},
#                 st_union_agg(geometry) as geometry,
#             from {self.get_intersection()}
#             group by {geoid}
#         ) as A
#         {join_plan}))"""
            self.qry_to_tbl(qry, tbl, True)
        return tbl


    def get_intersection(self):
        attr = 'intersection'
        tbl = f'geo.{self.state.abbr}_{attr}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            qry = f"""
select 
    A.*,
    A.aland / greatest(1, sum(A.aland) over (partition by A.block2020)) as aprop2020,
    st_intersection(B.geometry, C.geometry) as geometry,
from {self.get_crosswalk()} as A
join {self.get_shape()['block2010']} as B on A.block2010 = B.block2010
join {self.get_shape()['block2020']} as C on A.block2020 = C.block2020"""
            qry = f"""
select
    B.vtd2020,
    A.*,
    st_area(st_intersection(A.geometry, B.geometry)) as areaint2020,
from (
    {ut.subquery(qry)}
) as A
join {self.get_shape()['vtd2020']} as B on st_intersects(A.geometry, B.geometry)
qualify areaint2020 = max(areaint2020) over (partition by block2010, block2020)"""
            qry = f"""
select
    B.vtd2022,
    A.*,
    st_area(st_intersection(A.geometry, B.geometry)) as areaint2022,
from (
    {ut.subquery(qry)}
) as A
join {self.get_shape()['vtd2022']} as B on st_intersects(A.geometry, B.geometry)
qualify areaint2022 = max(areaint2022) over (partition by block2010, block2020)"""
            sel_id  = ut.make_select([f'div(A.block{year}, {10**(15-self.levels[level])}) as {level}{year}' for level in self.levels.keys() for year in [2020, 2010]][::-1])
            sel_pop = ut.make_select([f'A.aprop2020 * B.{p} as {p}' for p in subpops.keys()])
            sel_den = ut.make_select([f'A.aprop2020 * B.{p} / greatest(1, aland) as {p.replace("pop", "den")}' for p in subpops.keys()])
            qry = f"""
select
    {sel_id},
    vtd2020,
    vtd2022,
    county,
    C.* except (block2020),
    {sel_pop},
    {sel_den},
    --aprop2020,
    st_distance(A.geometry, (select st_boundary(us_outline_geom) from bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
    aland,
    awater,
    st_area(A.geometry) / 1000  / 1000 as atot,
    st_perimeter(A.geometry) / 1000 as perim,
    geometry,
from (
    {ut.subquery(qry)}
) as A
join {self.get_census()} as B using(block2020)
join {self.get_plan()} as C using(block2020)"""
            qry = f"""
select
    * except (geometry),
    case when perim < 0.1 then 0 else 4 * {np.pi} * atot / (perim * perim) end as polsby_popper,
    geometry,
from (
    {ut.subquery(qry)})"""
            self.qry_to_tbl(qry, tbl, True)
        return tbl


    def get_shape(self):
        attr = 'shape'
        urls = {
            'vtd2022':'https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/037e1de6-a862-49de-ae31-ae609e214972/download/vtds_22g.zip',
            'block2020':f'https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{self.state.fips}_tabblock20.zip',
            'vtd2020':'https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/06157b97-40b8-43af-99d5-bd9b5850b15e/download/vtds20g_2020.zip',
            'block2010':f'https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_{self.state.fips}_tabblock10.zip',
            }
        tbls = {geoid: f'{attr}.{self.state.abbr}_{geoid}' for geoid, url in urls.items()}
        for geoid, tbl in tbls.items():
            path, level, year, decade = self.parse(tbl)
            if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
                with Timer():
                    self.rpt(tbl)
                    url = urls[geoid]
                    zip_file = path / url.split('/')[-1]
                    download(zip_file, url, unzip=False)
                    d = decade % 100
                    repl = {'vtdkey':f'vtd{year}', f'geoid{d}':f'block{year}', f'aland{d}': 'aland', f'awater{d}': 'awater', 'geometry':'geometry',}
                    df = ut.prep(gpd.read_file(zip_file)).rename(columns=repl)
                    df.geometry = df.geometry.to_crs(CRS['bigquery']).buffer(0).apply(orient, args=(1,))
                    self.df_to_tbl(df, tbl, cols=repl.values())
        return tbls


    def get_crosswalk(self):
        attr = 'crosswalk'
        geoid = 'block2020'
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, level, year, decade = self.parse(tbl)
            with Timer():
                self.rpt(tbl)
                zip_file = path / f'TAB2010_TAB2020_ST{self.state.fips}.zip'
                url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
                download(zip_file, url)
                txt = zip_file.with_name(f'{zip_file.stem}_{self.state.abbr}.txt'.lower())
                repl = {'blk_2010': 'block_2010', 'blk_2020': 'block_2020', 'arealand_int': 'aland', 'areawater_int':'awater'}
                df = ut.prep(pd.read_csv(txt, sep='|')).rename(columns=repl)
                for year in [2010, 2020]:
                    geoid = self.get_geoid(df, level='block', year=year)
                self.df_to_tbl(df, tbl, cols=['block2010', 'block2020', 'aland', 'awater'])
        return tbl


    def get_census(self):
        attr = 'census'
        geoid = 'block2020'
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, level, year, decade = self.parse(tbl)
            with Timer():
                self.rpt(tbl)
                repl = {v:k for k,v in subpops.items() if v}
                print(repl)
                df = self.fetch_census(fields=['name', *repl.keys()], dataset='pl', year=year, level='block').rename(columns=repl)
                print(df.columns)
                self.compute_other(df, 'pop_tot_other')
                self.compute_other(df, 'pop_vap_other')
                county = df['name'].str.split(', ', expand=True)[3].str[:-7]
                df['county'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
                print(df.columns)
                self.df_to_tbl(df, tbl, cols=[geoid, 'county', *subpops.keys()])
        return tbl


    def get_plan(self):
        if self.state.abbr != 'TX':
            return False
        attr = 'plan'
        geoid = 'block2020'
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, level, year, decade = self.parse(tbl)
            with Timer():
                self.rpt(tbl)
                district_types = {'s':31, 'h':150, 'c':38}
                browser = mechanicalsoup.Browser()
                for dt in district_types.keys():
                    not_found = 0
                    for k in range(1000):
                        not_found += 1
                        proposal = f'plan{dt}{2100+k}'
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
                    ut.unzipper(file)
                L = []
                for file in path.iterdir():
                    if file.suffix == '.csv':
                        plan = ut.prep(file.stem)
                        df = ut.prep(pd.read_csv(file, names=[geoid, plan], header=0)).set_index(geoid)
                        if df[plan].nunique() == district_types[plan[4]]:
                            L.append(df)
                self.df_to_tbl(pd.concat(L, axis=1), tbl)
        return tbl
