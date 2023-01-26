from helpers.common_imports import *
from helpers import utilities as ut
import census, us, mechanicalsoup, geopandas as gpd
from .constants import *
from shapely.ops import orient
warnings.filterwarnings('ignore', message='.*ShapelyDeprecationWarning.*')
warnings.filterwarnings('ignore', message='.*DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.*')
warnings.simplefilter(action='ignore', category=FutureWarning)

def get_decade(x):
    try:
        return int(x) // 10 * 10
    except:
        return f'{x[:-4]}{get_decade(x[-4:])}'

def rpt(tbl):
    print(f'creating {tbl}', end=ellipsis)

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

def get_geoid(df, year=2020, level='block'):
    decade = str(get_decade(year))
    geoid = level+decade
    df[geoid] = ''
    for level, k in levels.items():
        for col in [level, level+'_'+decade]:
            if col in df.columns:
                df[geoid] += ut.rjust(df[col], k)
                break
    return geoid

def compute_other(df, feat):
    try:
        df['other_'+feat] = df['all_'+feat] - df['white_'+feat] - df['hisp_'+feat]
    except KeyError:
        print(f'Can not compute other_{feat}', end=ellipsis)

@dataclasses.dataclass
class Voting():
    census_api_key: str
    bq_project_id: str
    state: str = 'TX'
    geoid: str = 'vtd2022'
    root_path:str = '/content/'
    refresh: tuple = () 
    
    def __post_init__(self):
        self.level, self.year = self.geoid[:-4], int(self.geoid[-4:])
        self.root_path = pathlib.Path(self.root_path)
        self.data_path = self.root_path / 'data'
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.census_session = census.Census(self.census_api_key)
        self.bq = ut.BigQuery(project_id=self.bq_project_id)
        self.state = us.states.lookup(self.state)
        self.tbls = set()
        dependencies = {
            'block':'geo_block',
            self.level:'geo_block',
            'pl':'geo_block',
            'plan':'geo_block',
#             'assignment':{'geo_block', 'crosswalk'},
            'geo_block':{'geo', 'crosswalk'},
            'geo': {'acs5_transformed', 'election'},
            'crosswalk_raw':'crosswalk',
            'crosswalk': 'transformer',
            'transformer':'acs5_transformed',
            'acs5': 'acs5_transformed',
            'acs5_transformed':'final',
            'election_raw': 'election',
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
                    print(f'ignoring unknown attribute {attr} ... must be {list(dependencies.keys())}')

        
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
        path = self.data_path / attr
        geoid = tbl.split('_')[-1]
        level, year = geoid[:-4], int(geoid[-4:])
        decade = get_decade(year)
        if level != 'vtd':
            geoid = f'{level}{decade}'
        return path, geoid, level, year, decade


    def qry_to_tbl(self, qry, tbl, show=False):
        with Timer():
            rpt(tbl)
            if show:
                print(qry)
            self.bq.qry_to_tbl(qry, tbl)
            self.tbls.add(tbl)
            
    
    def df_to_tbl(self, df, tbl, cols=None):
        cols = ut.listify(cols) if cols else df.columns
        self.bq.df_to_tbl(ut.prep(df[df.columns.intersection(cols)]), tbl)
        self.tbls.add(tbl)


    def get_final(self):
        attr = 'final'
        if (self.state.abbr != 'TX') or (self.level != 'vtd'):
            return False
        tbl = f'{attr}.{self.state.abbr}_{self.geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            qry = f"""
select
    campaign,
    string_agg(candidate, " v " order by party) as candidates
from (
    select distinct
        office||"_"||year as campaign,
        name||"_"||party as candidate,
        party,
    from {self.get_election()}
    where
        office in ("Governor", "USSen", "President", "Comptroller", "AttorneyGen", "LtGovernor")
        and election = "general"
        and party in ("D", "R")
        and year >= 2015
)
group by campaign"""
            campaigns = ut.listify(self.bq.qry_to_df(qry))

            L = []
            for campaign, candidates in campaigns:
                office, year = campaign.split('_')
                year = min(int(year), datetime.date.today().year-2)
                A = self.get_acs5_transformed(year)
                cols1 = ['year', geoid, 'county2020', 'aland', 'awater', 'atot', 'perim', 'polsby_popper']
                cols2 = sorted(ut.setify(self.bq.get_cols(A)).difference(cols1))
                cols = cols1 + cols2
                qry = f"""
select 
    {ut.make_select(cols)},
    "{campaign}" as campaign,
    "{candidates}" as candidates,
    coalesce(B.D, 0) as dem_votes,
    coalesce(B.R, 0) as rep_votes,
    coalesce(B.D, 0) + coalesce(B.R, 0) as tot_votes,
from {A} as A
left join (
    select {geoid}, party, votes,
    from {self.get_election()}
    where office = "{office}" and year = {year})
    pivot(sum(votes) for party in ("D", "R")
) as B
using ({geoid})
"""
                qry = f"""
select
    *,
    case when tot_votes = 0  then 0 else dem_votes / tot_votes end as dem_prop,
    case when tot_votes = 0  then 0 else rep_votes / tot_votes end as rep_prop,
from (
    {ut.subquery(qry)}
)"""
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
            path, geoid, level, year, decade = self.parse(tbl)
            attr_raw = attr+'_raw'
            tbl_raw  = tbl+'_raw'
            if not self.bq.get_tbl(tbl_raw, overwrite=(attr_raw in self.refresh) & (tbl_raw not in self.tbls)):
                with Timer():
                    rpt(tbl_raw)
                    zip_file = path / f'2022-general-vtds-election-data.zip'                
                    url = f'https://data.capitol.texas.gov/dataset/35b16aee-0bb0-4866-b1ec-859f1f044241/resource/b9ebdbdb-3e31-4c98-b158-0e2993b05efc/download/{zip_file.name}'
                    download(zip_file, url)

                    L = []
                    cols = ['vtd2020', 'county2020', 'fips', 'office', 'year', 'election', 'name', 'party', 'incumbent', 'votes']                    
                    for file in path.iterdir():
                        a = ut.prep(file.stem.split('_'))
                        if a[-1] == 'returns':
                            df = ut.prep(pd.read_csv(file)).rename(columns={'vtd':'vtd2020', 'county':'county2020'})
                            mask = (df['votes'] > 0) & (df['party'].isin(('R', 'D', 'L', 'G')))
                            if mask.any():
                                repl = {(' ', '.', ','): ''}
                                df['vtd2020'] = ut.rjust(df['vtd2020'], 6)
                                df['fips'] = self.state.fips + ut.rjust(df['fips'], 3)
                                df['office'] = ut.replace(df['office'], repl)
                                df['year'] = int(a[0])
                                df['election'] = ut.join(a[1:-2], '_')
                                df['name'] = ut.replace(df['name'], repl)
                                df['incumbent'] = df['incumbent'] == 'Y'
                                L.append(df.loc[mask, cols])
                    df = ut.prep(pd.concat(L, axis=0)).reset_index(drop=True)
                    self.df_to_tbl(df, tbl_raw)
            qry = f"""
select
    coalesce(B.vtd2020, C.vtd2020) as vtd2020,
    A.* except (vtd2020, votes),
    sum(A.votes) as votes,
    sum(coalesce(B.all_tot_pop, C.all_tot_pop)) as all_tot_pop,
from {tbl_raw} as A
left join {self.get_geo()} as B
on A.fips || A.vtd2020 = B.vtd2020
left join {self.get_geo()} as C
on A.fips || '0' || left(A.vtd2020, 5) = C.vtd2020
group by 1,2,3,4,5,6,7,8,9"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_acs5_transformed(self, year=2018):
        attr = 'acs5_transformed'
        tbl_src  = self.get_acs5(year)
        path_src, geoid_src, level_src, year_src, decade_src = self.parse(tbl_src)
        tbl = f'{tbl_src}_{self.geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            feat = self.bq.get_cols(tbl_src)[2:]
            qry = f"""
select
    S.year,
    T.{geoid},
    {ut.make_select([f'sum(S.{x} * T.{x[:x.rfind("_")]}_pop) as {x}' for x in feat if x[:3] != 'all'])},
from {tbl_src} as S
inner join {self.get_transformer(geoid_src)} as T
using ({geoid_src})
group by 1, 2"""
            
            qry = f"""
select
    A.*,
    G.county2020,
    G.dist_to_border,
    G.aland,
    G.awater,
    G.atot,
    G.perim,
    G.polsby_popper,
    {ut.make_select([f'A.white_{x} + A.hisp_{x} + A.other_{x} as all_{x}' for x in features_universal])},
from (
    {ut.subquery(qry)}
) as A
join {self.get_geo()} as G
using ({geoid})"""
            
            def den(x):
                if x == 'hisp_vap_spanishathomeenglishwell':
                    return 'hisp_vap_spanishathome'
                elif x in subpops.keys():
                    return 'all'+x[ut.findn(x, '_', 1):]
                else:
                    return x[:ut.findn(x, '_', 2)+1]+'pop'
                    
            qry = f"""
select
    *,
    {ut.make_select([f'case when {den(x)} = 0 then 0 else {x} / {den(x)} end as {x}_prop' for x in feat])},
    {ut.make_select([f'case when aland = 0 then 0 else {x} / aland end as {x}_dens' for x in subpops.keys()])},
from (
    {ut.subquery(qry)}
)"""
            
            qry = f"""
select
    *,
    {ut.make_select([f'{x} / max({x}) over () as {x}_rel' for x in ['dist_to_border', *[f'{x}_dens' for x in subpops.keys()]]])},
from (
    {ut.subquery(qry)}
)"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_acs5(self, year=2018):
        attr = 'acs5'
        tbl = f'{attr}.{self.state.abbr}_tract{year}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            with Timer():
                rpt(tbl)
                base = set().union(*features.values())
                survey = {x for x in base if x[0]=='s'}
                base = base.difference(survey)
                B = self.fetch_census(fields=base  , dataset='acs5'  , year=year, level=level)
                S = self.fetch_census(fields=survey, dataset='acs5st', year=year, level=level)
                df = ut.prep(B.merge(S, on=['year', geoid]))
                for name, fields in features.items():
                    df[name] = df[fields].sum(axis=1)
                df = df[['year', geoid, *sorted(features.keys())]]
                for var in features_universal:
                    compute_other(df, var)
                self.df_to_tbl(df, tbl)
        return tbl

    
    def get_transformer(self, geoid_src='tract2018'):
        attr = 'transformer'
        tbl = f'{attr}.{self.state.abbr}_{get_decade(geoid_src)}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            g = geoid+', ' if year<2020 else ''
            qry = f"""
select
    {g}
    A.block2020,
    A.block_group2020,
    A.tract2020,
    A.vtd2020,
    A.county2020,
    {ut.make_select([f'sum(case when B.{x} = 0 then 0 else A.{x} / B.{x} end) as {x}' for x in subpops.keys()])},
from {self.get_crosswalk()} as A
inner join (
    select {geoid}, {ut.make_select([f'sum({x}) as {x}' for x in subpops.keys()], 2)},
    from {self.get_crosswalk()}
    group by {geoid}
) as B
using ({geoid})
group by {g}block2020, block_group2020, tract2020, vtd2020, county2020"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_crosswalk(self):
        attr = 'crosswalk'
        tbl = f'{attr}.{self.state.abbr}_block2020'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            attr_raw = attr+'_raw'
            tbl_raw  = tbl+'_raw'
            if not self.bq.get_tbl(tbl_raw, overwrite=(attr_raw in self.refresh) & (tbl_raw not in self.tbls)):
                self.tbls.add(tbl_raw)
                with Timer():
                    rpt(tbl_raw)
                    zip_file = path / f'TAB2010_TAB2020_ST{self.state.fips}.zip'
                    url = f'https://www2.census.gov/geo/docs/maps-data/data/rel2020/t10t20/{zip_file.name}'
                    download(zip_file, url)
                
                    txt = zip_file.with_name(f'{zip_file.stem}_{self.state.abbr}.txt'.lower())
                    df = ut.prep(pd.read_csv(txt, sep='|')).rename(columns={'arealand_int': 'aland', 'blk_2010': 'block_2010', 'blk_2020': 'block_2020'})
                    for dec in [2010, 2020]:
                        geoid = get_geoid(df, dec)
                        df[f'aprop{dec}'] = df['aland'] / np.fmax(df.groupby(geoid)['aland'].transform('sum'), 1)
                    self.df_to_tbl(df, tbl_raw, cols=['block2010', 'block2020', 'aland', 'aprop2010', 'aprop2020'])
            qry = f"""
select
    C.*,
    div(C.block2010, 1000) as block_group2010,
    div(C.block2020, 1000) as block_group2020,
    div(C.block2010, 10000) as tract2010,
    div(C.block2020, 10000) as tract2020,
    G.{self.geoid},
    G.county2020,
    {ut.make_select([f'C.aprop2020 * G.{subpop} as {subpop}' for subpop in subpops.keys()])}
from {tbl_raw} as C
left join {self.get_geo(block=True)} as G
using (block2020)"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_geo(self, block=False):
        r = f'geo.{self.state.abbr}_'
        if block:
            attr = 'geo_block'
            tbl = r+'block2020'
        else:
            attr = 'geo'
            tbl = r+self.geoid
        path, geoid, level, year, decade = self.parse(tbl)
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            if block:
                qry = f"""
select
    {geoid},
    P.county,
    S.* except (geometry),
    P.* except (county),
    geometry,
from (
    select
        V.{self.geoid},
        B.*,
        case when perim < 0.1 then 0 else 4 * {np.pi} * atot / (perim * perim) end as polsby_popper,
        st_area(st_intersection(B.geometry, V.geometry)) / atot as areaint,
    from (
        select
            {geoid},
            st_distance(geometry, (select st_boundary(us_outline_geom) from bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
            aland / 1000  / 1000 as aland,
            awater  / 1000  / 1000 as awater,
            st_area(geometry) / 1000  / 1000 as atot,
            st_perimeter(geometry) / 1000 as perim,
            geometry,
        from {self.get_block()}
    ) as B
    inner join {self.get_vtd()} as V
    on st_intersects(B.geometry, V.geometry)
    qualify areaint = max(areaint) over (partition by {geoid})
) as S
inner join {self.get_pl()} as P
using ({geoid})"""
                if self.state.abbr == 'TX':
                    qry += f"""
inner join {self.get_plan()} as B
using ({geoid})"""

            else:
                tbl_raw = self.get_geo(block=True)
                geo_cols = [geoid, 'dist_to_border', 'aland', 'awater', 'atot', 'perim', 'polsby_popper', 'geometry']
                pop_cols = ['all_tot_pop', 'all_vap_pop', 'white_tot_pop', 'white_vap_pop', 'hisp_tot_pop', 'hisp_vap_pop', 'other_tot_pop', 'other_vap_pop']
                exclude_cols = ['block2020']
                district_cols = [x for x in self.bq.get_cols(tbl_raw) if x not in geo_cols + pop_cols + exclude_cols]
                qry = f"""
select * except (geometry), geometry,
from (
    select *, case when perim < 0.1 then 0 else 4 * {np.pi} * atot / (perim * perim) end as polsby_popper,
    from (
        select *, st_perimeter(geometry) / 1000 as perim,        
        from (
            select
                {geoid},
                min(dist_to_border) as dist_to_border,
                sum(aland) as aland,
                sum(awater) as awater,
                sum(atot) as atot,
                st_union_agg(geometry) as geometry,
            from {tbl_raw}
            group by {geoid}
            )
        )
    ) as S
join (
    select
        {geoid},
        {ut.make_select([f"sum({col}) as {col}" for col in pop_cols], 2)},
    from {tbl_raw}
    group by {geoid}
) as P
using ({geoid})
join (
    select
        {geoid},
        {ut.make_select(district_cols, 2)},
    from {tbl_raw}
    qualify row_number() over (partition by {geoid} order by all_tot_pop desc) = 1
) as D
using ({geoid})"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_plan(self):
        if self.state.abbr != 'TX':
            return False
        attr = 'plan'
        tbl = f'{attr}.{self.state.abbr}_block2020'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            with Timer():
                rpt(tbl)
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


#     def get_assignments(self, year=2020):
#         attr = 'assignments'
#         tbl = f'{attr}.{self.state.abbr}_block{get_decade(year)}'
#         if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
#             path, geoid, level, year, decade = self.parse(tbl)
#             with Timer():
#                 rpt(tbl)
#                 zip_file = path / f'BlockAssign_ST{self.state.fips}_{self.state.abbr}.zip'
#                 if decade == 2010:
#                     url = f'https://www2.census.gov/geo/docs/maps-data/data/baf/{zip_file.name}'
#                 elif decade == 2020:
#                     url = f'https://www2.census.gov/geo/docs/maps-data/data/baf{decade}/{zip_file.name}'
#                 download(zip_file, url)

#                 dist = {'VTD':f'vtd{decade}', 'CD':f'congress{decade-10}', 'SLDU':f'senate{decade-10}', 'SLDL':f'house{decade-10}'}
#                 L = []
#                 for abbr, name in dist.items():
#                     f = zip_file.parent / f'{zip_file.stem}_{abbr}.txt'
#                     df = ut.prep(pd.read_csv(f, sep='|'))
#                     if abbr == 'VTD':
#                         # create vtd id using 3 fips + 6 vtd, pad on left with 0 as needed
#                         df['district'] = self.state.fips + ut.rjust(df['countyfp'], 3) + ut.rjust(df['district'], 6)
#                     repl = {'blockid': geoid, 'district':name}
#                     L.append(df.rename(columns=repl)[repl.values()].set_index(geoid))
#                 df = pd.concat(L, axis=1)
#                 self.bq.df_to_tbl(df, tbl)
#         return tbl


    def get_pl(self):
        attr = 'pl'
        tbl = f'{attr}.{self.state.abbr}_block2020'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            with Timer():
                rpt(tbl)
                repl = {v:k for k,v in subpops.items() if v}
                df = self.fetch_census(fields=['name', *repl.keys()], dataset='pl', year=year, level='block').rename(columns=repl)
                compute_other(df, 'tot_pop')
                compute_other(df, 'vap_pop')
                df.insert(1, 'county', df['name'].str.split(', ', expand=True)[3].str[:-7])
#                 df['county'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
                self.df_to_tbl(df, tbl, cols=[geoid, *subpops.keys(), 'county2020'])
        return tbl


    def get_shape(self, attr, geoid, url):
        tbl = f'shape.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, geoid, level, year, decade = self.parse(tbl)
            with Timer():
                rpt(tbl)
                zip_file = path / url.split('/')[-1]
                download(zip_file, url, unzip=False)
                d = decade % 100
                repl = {'vtdkey':f'vtd{year}', f'geoid{d}':geoid, f'aland{d}': 'aland', f'awater{d}': 'awater', 'geometry':'geometry',}
                df = ut.prep(gpd.read_file(zip_file)).rename(columns=repl)
                df.geometry = df.geometry.to_crs(CRS['bigquery']).buffer(0).apply(orient, args=(1,))
                self.df_to_tbl(df, tbl, cols=repl.values())
        return tbl


    def get_vtd(self):
        url = 'https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/037e1de6-a862-49de-ae31-ae609e214972/download/vtds_22g.zip'
        return self.get_shape('vtd', 'vtd2022', url)


    def get_block(self):
        url = f'https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{self.state.fips}_tabblock20.zip'
        return self.get_shape('block', 'block2020', url)
    

    
#     def get_blocks(self):
#         attr = 'blocks'
#         tbl = f'{attr}.{self.state.abbr}_{attr}2020'
#         if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
#             path, geoid, level, year, decade = self.parse(tbl)
#             with Timer():
#                 rpt(tbl)
#                 d = decade % 100
#                 zip_file = path / f'tl_{decade}_{self.state.fips}_tabblock{d}.zip'
#                 if decade == 2010:
#                     url = f'https://www2.census.gov/geo/tiger/TIGER{decade}/TABBLOCK/{decade}/{zip_file.name}'
#                 elif decade == 2020:
#                     url = f'https://www2.census.gov/geo/tiger/TIGER{decade}/TABBLOCK{d}/{zip_file.name}'
#                 download(zip_file, url, unzip=False)

#                 repl = {f'geoid{d}':geoid, f'aland{d}': 'aland', f'awater{d}': 'awater', 'geometry':'geometry',}
#                 df = ut.prep(gpd.read_file(zip_file)).rename(columns=repl)[repl.values()]
#                 df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
#                 self.bq.df_to_tbl(df, tbl)
#         return tbl

    
#     def get_vtds(self):
#         attr = 'vtds'
#         tbl = f'{attr}.{self.state.abbr}_{attr}2020'
#         if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
#             path, geoid, level, year, decade = self.parse(tbl)
#             with Timer():
#                 rpt(tbl)
#                 d = decade % 100
#                 zip_file = path / f'vtds_{d}g.zip'
#                 url = f'https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/037e1de6-a862-49de-ae31-ae609e214972/download/{zip_file.name}'
#                 download(zip_file, url, unzip=False)

#                 repl = {'vtdkey':f'vtd{year}', 'geometry':'geometry'}
#                 df = ut.prep(gpd.read_file(zip_file)).rename(columns=repl)[repl.values()]
#                 df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
#                 self.bq.df_to_tbl(df, tbl)
#         return tbl
