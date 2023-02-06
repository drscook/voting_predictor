from helpers.common_imports import *
from helpers import utilities as ut
import census, us, mechanicalsoup, geopandas as gpd, networkx as nx
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
    urbanizations: int = 3
    
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
            'geo':{'acs','adjacency'},
            'adjacency':set(),
            'acs_src':'acs',
            'acs':'combined',
            'election':'combined',
            'combined':'contracted',
            'contracted':set()}
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
                    i = j
                    break
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

    def qry_to_df(self, qry):
        return self.bq.qry_to_df(qry)
    
    def get_contracted(self):
        if (self.state.abbr != 'TX') or (self.level != 'vtd'):
            return False
        attr = 'contracted'
        geoid = self.geoid
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        attrs = ['county', 'pop_vap_all', 'vote_tot', 'vote_rate']
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            tbl_src = tbl+'_src'
            if not self.bq.get_tbl(tbl_src, overwrite=(attr in self.refresh) & (tbl_src not in self.tbls)):
                warnings.filterwarnings('ignore', message='.*divide by zero encountered.*')
                warnings.filterwarnings('ignore', message='.*invalid value encountered in true_divide.*')
                edges = self.bq.tbl_to_df(self.get_adjacency(), rows=-1)
                def contract(nodes):
                    print(f'contracting {nodes.name}')
                    G = nx.from_pandas_edgelist(edges, source='x', target='y', edge_attr=edges.columns.difference(['x', 'y']).tolist())
                    G.remove_edges_from(nx.selfloop_edges(G))
                    nx.set_node_attributes(G, nodes.to_dict(orient='index'))
                    contraction_dict = {node:node for node in G.nodes}
                    while True:
                        try:
                            v, src = max((node_data['vote_rate'], node) for node, node_data in G.nodes(data=True) if G.degree[node] > 0 and (node_data['vote_tot'] < 100 or node_data['vote_rate'] > 1))
                        except ValueError:
                            break
                        w, trg = min((edge_data['dist'], node) for node, edge_data in G.adj[src].items())
                        for key in attrs:
                            if key != 'county':
                                G.nodes[trg][key] += G.nodes[src][key]
                        G.nodes[trg]['vote_rate'] = G.nodes[trg]['vote_tot'] / G.nodes[trg]['pop_vap_all']
                        nx.contracted_nodes(G, trg, src, False, False)
                        contraction_dict[src] = trg

                        for node, edge_data in G.adj[trg].items():
                            if 'contraction' in edge_data:
                                edge_data['dist'] = min(edge_data['dist'], min(contracted_edge_data['dist'] for contracted_edge, contracted_edge_data in edge_data['contraction'].items()))

                # check that we did min dist on contracted edge correctly
                # for x, y, edge_data in G.edges(data=True):
                #     if 'contraction' in edge_data:
                #         dist = edge_data['dist']
                #         contracted_dist, contracted_edge = min((contracted_edge_data['dist'], contracted_edge) for contracted_edge, contracted_edge_data in edge_data['contraction'].items())
                #         assert dist <= contracted_dist, f'contraction error - edge ({x},{y}) has dist={dist} which is larger than contracted edge {contracted_edge} with dist={contracted_dist}'
                    nodes[geoid+'_contracted'] = pd.Series(contraction_dict)
                    return nodes


                df = self.qry_to_df(f'select {geoid}, {ut.join(attrs)} from {self.get_combined()}').set_index(geoid)
    #             df = self.bq.tbl_to_df(self.get_combined(), rows=-1).set_index(geoid)
                df['vote_rate'] = df['vote_tot'] / df['pop_vap_all']
                df[geoid+'_contracted'] = df.index
                df = df.groupby('campaign').apply(contract)
                self.df_to_tbl(df, tbl_src)
            qry = f'select A.{geoid}_contracted, B.* from {tbl_src} as A join {self.get_combined} as B using ({geoid})'
            self.qry_to_tbl(qry, tbl, True)
        return tbl
        

    def get_combined(self):
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
                year = int(year)
                qry = f"""
select 
    year,
    {geoid},
    county,
    "{campaign}" as campaign,
    "{candidates}" as candidates,
    {year%4==2} as midterm,
    {'President' in campaign or 'USSen' in campaign or 'USRep' in campaign} as federal,
    ifnull(D,0) as vote_dem,
    ifnull(R,0) as vote_rep,
    ifnull(D,0) + ifnull(R,0) as vote_tot,
    (ifnull(D,0) + ifnull(R,0)) / greatest(1, pop_vap_all) as vote_rate,
    ifnull(D,0) / greatest(1, ifnull(D,0) + ifnull(R,0)) as pref_dem,
    ifnull(R,0) / greatest(1, ifnull(D,0) + ifnull(R,0)) as pref_rep,
    ntile({self.urbanizations}) over (order by den_tot_all asc) as urbanization,
    A.* except (year, {geoid}, county),
from {self.get_acs(level='tract', year=min(year, datetime.date.today().year-2), geoid_trg=geoid)} as A
left join (
    select {geoid}, party, votes,
    from {self.get_election()}
    where office = "{office}" and year = {year})
    pivot(sum(votes) for party in ("D", "R")
) as B using ({geoid})"""
                L.append(qry)
            qry = ut.join(L, '\nunion all')
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
                cols = [self.geoid, 'year', 'office', 'election', 'name', 'party', 'incumbent', 'votes']
                for file in path.iterdir():
                    a = ut.prep(file.stem.split('_'))
                    if ('general' in a) & ('returns' in a):
                        df = ut.prep(pd.read_csv(file)).rename(columns={'vtdkeyvalue':self.geoid})
                        mask = (df['votes'] > 0) & (df['party'].isin(('R', 'D', 'L', 'G')))
                        if mask.any():
                            repl = {(' ', '.', ','): ''}
                            df['year'] = int(a[0])
                            df['office'] = ut.replace(df['office'], repl)
                            df['election'] = ut.join(a[1:-2], '_')
                            df['name'] = ut.replace(df['name'], repl)
                            df['incumbent'] = df['incumbent'] == 'Y'
                            L.append(df.loc[mask, cols])
                df = ut.prep(pd.concat(L, axis=0)).reset_index(drop=True)
                self.df_to_tbl(df, tbl)
        return tbl


    def get_acs(self, year=2018, level='tract', geoid_trg='vtd2022'):
        attr_trg = 'acs'
        attr_src = f'{attr_trg}_src'
        tbl_src  = f'{attr_trg}.{self.state.abbr}_{level}{year}'
        tbl_trg  = f'{tbl_src}_{geoid_trg}'
        path, level, year, decade = self.parse(tbl_src)
        geoid_src = f'{level}{decade}'
        if not self.bq.get_tbl(tbl_trg, overwrite=(attr_trg in self.refresh) & (tbl_trg not in self.tbls)):
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
                        if fields:
                            df[name] = df[fields].sum(axis=1)
                    for name, fields in features.items():
                        if not fields:
                            self.compute_other(df, name)
                    self.df_to_tbl(df, tbl_src, cols=['year', geoid_src, *features.keys()])    
            feat_geo = ['county', 'dist_to_border', 'arealand', 'areawater', 'areatot', 'areacomputed', 'perimcomputed', 'polsby_popper']
            feat_acs = self.bq.get_cols(tbl_src)[2:]
            sel_pop = [f'sum({x}) as {x}' for x in subpops.keys()]
            g = lambda x: 'pop'+x[x.find('_'):]
            sel_grp = [f'sum(case when S.{g(x)} > 0 then A.{x} * I.{g(x)} / S.{g(x)} else A.{x} / S.ct end) as {x}' for x in feat_acs if not "all" in x]
            sel_all = [f'A.{x.replace("all", "hisp")} + A.{x.replace("all", "other")} + A.{x.replace("all", "white")} as {x}' for x in feat_acs if "all" in x]
            sel_den = [f'{x} / areatot * 1000000 as {x.replace("pop", "den")}' for x in subpops.keys()]
            qry = f"""
select
    year, {geoid_trg},
    {ut.join(feat_geo)},
    {ut.select(sel_den)},
    {ut.join(feat_acs)},
from (
    select
        A.*,
        {ut.select(sel_all, 2)},
        {ut.join(feat_geo)},
    from (
        select
            year,
            {geoid_trg},
            {ut.select(sel_grp, 3)},
        from {tbl_src} as A
        join {self.get_intersection()} as I using ({geoid_src})
        join (
            select
                {geoid_src},
                count(*) as ct,
                {ut.select(sel_pop, 4)},
            from {self.get_intersection()}
            group by {geoid_src}
        ) as S using ({geoid_src})
        group by 1, 2
    ) as A
    join {self.get_geo(geoid_trg)} as T using ({geoid_trg}))"""
            self.qry_to_tbl(qry, tbl_trg)
        return tbl_trg
    
    
    def get_adjacency(self, geoid='vtd2022'):
        attr = 'adjacency'
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            qry = f"""
select
    A.{geoid} as x,
    B.{geoid} as y,
    A.county,
    st_distance(A.point, B.point) as dist,
from {self.get_geo(geoid)} as A
join {self.get_geo(geoid)} as B
on A.county = B.county and A.{geoid} <> B.{geoid} and st_intersects(A.geometry, B.geometry)"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_geo(self, geoid='vtd2022'):
        attr = 'geo'
        geoid = self.get_decade(geoid)
        tbl = f'{attr}.{self.state.abbr}_{geoid}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            path, level, year, decade = self.parse(tbl)
            block = f'block{decade}'
            sel_pop = {x:f'sum({x}) as {x}' for x in subpops.keys()}
            sel_den = {x.replace("pop", "den"):f'{x} / areatot * 1000000 as {x.replace("pop", "den")}' for x in subpops.keys()}
            g = lambda x: f'join (select {geoid}, {x}, sum(pop_tot_all) as p from {self.get_intersection()} group by 1, 2 qualify row_number() over (partition by {geoid} order by p desc) = 1) using ({geoid})'
            sel_plan = {x:g(x) for x in self.bq.get_cols(self.get_plan())[1:]}
            qry = f"""
select
    {geoid}, county, dist_to_border, arealand, areawater, areatot, areacomputed, perimcomputed,
    4 * {np.pi} * areacomputed / (perimcomputed * perimcomputed) as polsby_popper,
    {ut.select(sel_den.values())},
    {ut.join(sel_pop.keys())},
    {ut.join(sel_plan.keys())},
    geometry,
    st_centroid(geometry) as point,
from (
    select 
        *,
        st_distance(geometry, (select st_boundary(us_outline_geom) from bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
        st_area(geometry) as areacomputed,
        st_perimeter(geometry) as perimcomputed,
    from (
        select
            {geoid},
            {ut.select(sel_pop.values(), 3)},
            sum(A.arealand) as arealand,
            sum(A.areawater) as areawater,
            sum(A.areatot) as areatot,
            st_union_agg(B.geometry) as geometry,
        from {self.get_intersection()} as A
        join {self.get_shape()[block]} as B using ({block})
        group by {geoid}))
{g('county')}
"""+ut.join(sel_plan.values(), '\n')
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_intersection(self):
        attr = 'intersection'
        geoid = 'block2020'
        tbl = f'geo.{self.state.abbr}_{attr}'
        if not self.bq.get_tbl(tbl, overwrite=(attr in self.refresh) & (tbl not in self.tbls)):
            block = f'block2020'
            sel_id  = [f'div(block{year}, {10**(15-self.levels[level])}) as {level}{year}' for level in self.levels.keys() for year in [2020, 2010]][::-1]
            sel_pop = [f'areaprop * {x} as {x}' for x in subpops.keys()]
            sel_den = [f'areaprop * {x} / areatot * 1000000 as {x.replace("pop", "den")}' for x in subpops.keys()]
            sel_vtd = ['vtd2020', 'vtd2022']
            qry = f"""
select
    {ut.select(sel_id)},
    {ut.select(sel_vtd)},
    county,
    arealand,
    areawater,
    areatot,
    {ut.select(sel_den)},
    {ut.select(sel_pop)},
    plan.* except({geoid}),
from (select *, areatot / sum(areatot) over (partition by {geoid}) as areaprop from {self.get_crosswalk()}) as crosswalk
join {self.get_census()} as census using ({geoid})
join {self.get_plan()} as plan using ({geoid})"""
            for vtd in sel_vtd:
                qry += f"""
join (
    select {geoid}, {vtd}, st_area(st_intersection(A.geometry, B.geometry)) as areaint,
    from {self.get_shape()[block]} as A
    join {self.get_shape()[vtd]} as B
    on st_intersects(A.geometry, B.geometry)
    qualify areaint = max(areaint) over (partition by {geoid})
) using ({geoid})"""
            self.qry_to_tbl(qry, tbl)
        return tbl


    def get_shape(self):
        attr = 'shape'
        urls = {
            'vtd2022':'https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/037e1de6-a862-49de-ae31-ae609e214972/download/vtds_22g.zip',
            'block2020':f'https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_{self.state.fips}_tabblock20.zip',
            'vtd2020':'https://data.capitol.texas.gov/dataset/4d8298d0-d176-4c19-b174-42837027b73e/resource/06157b97-40b8-43af-99d5-bd9b5850b15e/download/vtds20g_2020.zip',
#             'block2010':f'https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_{self.state.fips}_tabblock10.zip',
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
                repl = {'blk_2010': 'block_2010', 'blk_2020': 'block_2020', 'arealand_int': 'arealand', 'areawater_int':'areawater'}
                df = ut.prep(pd.read_csv(txt, sep='|')).rename(columns=repl)#.query(f'state_2010=={self.state.fips} and state_2020=={self.state.fips}')
                df['areatot'] = df['arealand'] + df['areawater']
                for year in [2010, 2020]:
                    geoid = self.get_geoid(df, level='block', year=year)
                self.df_to_tbl(df, tbl, cols=['block2010', 'block2020', 'arealand', 'areawater', 'areatot'])
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
                df = self.fetch_census(fields=['name', *repl.keys()], dataset='pl', year=year, level='block').rename(columns=repl)
                self.compute_other(df, 'pop_tot_other')
                self.compute_other(df, 'pop_vap_other')
                county = df['name'].str.split(', ', expand=True)[3].str[:-7]
                df['county'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
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
