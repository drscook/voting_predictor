from helpers import *
from plans import *
from shapely.ops import orient

def get_shapes(level='block', overwrite=False, num_shapes=None):
    year = 2020
    geoid = f'{level}{decade_(year)}'
    tbl = f'shapes.{geoid}'
    if not check_exists(tbl, overwrite):
        if level == 'block':
            pq = pq_(tbl)
            L = []


            # shape files
            tbl_temp = tbl+'_shapes'
            if not check_exists(tbl_temp, overwrite):
                tbl_raw = tbl_temp + '_raw'
                if not check_exists(tbl_raw, overwrite):
                    print(f'getting {tbl_raw}')
                    zip_file = pq.parent / f'tl_2020_{STATE.fips}_tabblock20.zip'
                    url = f'https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/{zip_file.name}'
                    download(zip_file, url, unzip=False)
                    repl = {'geoid20':geoid, 'aland20': 'aland', 'awater20': 'awater', 'geometry':'geometry',}
                    df = prep(gpd.read_file(zip_file, rows=num_shapes)).rename(columns=repl)[repl.values()].to_crs(CRS['bigquery'])                    
                    df.geometry = df.geometry.buffer(0).apply(orient, args=(1,))
                    df_to_table(df, tbl_raw)
                print(f'getting {tbl_temp}')
                qry = f"""
select
    A.* except (geometry),
    4 * {np.pi} * A.atot / (A.perim * A.perim) as polsby_popper,
    A.geometry,
from (
    select
        A.{geoid},
        st_distance(A.geometry, (SELECT st_boundary(us_outline_geom) FROM bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
        A.aland,
        A.awater,
        st_area(A.geometry) as atot,
        st_perimeter(A.geometry) as perim,
        A.geometry,
    from
        {tbl_raw} as A
    ) as A
"""
                query_to_table(qry, tbl_temp)
            L.append(tbl_temp)
                

            # 2020 census data
            tbl_temp = tbl+'_census'
            if not check_exists(tbl_temp, overwrite):
                print(f'getting {tbl_temp}')
                df = get(['name', *SUBPOPS.keys()], 'pl', year, level)
                df['county'] = df['name'].str.split(', ', expand=True)[3].str[:-7]
                df = df.rename(columns=SUBPOPS)[[geoid, 'county', *SUBPOPS.values()]]
                compute_other(df, 'tot_pop')
                compute_other(df, 'vap_pop')
                df_to_table(df, tbl_temp)
            L.append(tbl_temp)


            # block assignment files
            zip_file = pq.parent / f'BlockAssign_ST{STATE.fips}_{STATE.abbr}.zip'
            url = f'https://www2.census.gov/geo/docs/maps-data/data/baf2020/{zip_file.name}'
            download(zip_file, url)
            d = {'VTD':'vtd2020', 'CD':'congress2010', 'SLDU':'senate2010', 'SLDL':'house2010'}
            for abbr, name in d.items():
                tbl_temp = tbl+'_'+name
                if not check_exists(tbl_temp, overwrite):
                    print(f'getting {tbl_temp}')
                    f = zip_file.parent / f'{zip_file.stem}_{abbr}.txt'
                    df = prep(pd.read_csv(f, sep='|'))
                    if abbr == 'VTD':
                        # create vtd id using 3 fips + 6 vtd, pad on left with 0 as needed
                        df['district'] = STATE.fips + df['countyfp'].astype(str).str.rjust(3, '0') + df['district'].astype(str).str.rjust(6, '0')
                    repl = {'blockid': geoid, 'district':name}
                    df = df.rename(columns=repl)[repl.values()]
                    df_to_table(df, tbl_temp)
                L.append(tbl_temp)


            # plans
            L.append(get_plans())


            # concatenate
            print(f'getting {tbl}')
            qry = f"""
select
    {geoid},
    vtd2020,
    county,
    * except({geoid}, vtd2020, county, geometry),
    geometry,
from
    {L[0]}""" + join([f"\njoin\n    {tbl}\nusing\n    ({geoid})" for tbl in L[1:]], '')
            query_to_table(qry, tbl)
    
        else:
            blk = get_shapes(level='block')
            print(f'getting {tbl}')
            geo_cols = ['aland', 'awater', 'atot']
            pop_cols = ['all_tot_pop', 'all_vap_pop', 'white_tot_pop', 'white_vap_pop', 'hisp_tot_pop', 'hisp_vap_pop', 'other_tot_pop', 'other_vap_pop']
            district_cols = [x for x in get_columns(blk) if x not in [
                'block2020', geoid, 'perim', 'polsby_popper', 'dist_to_border', 'geometry', *geo_cols, *pop_cols]]

            if level == 'vtd':
                geoid_calc = "vtd2020"
                idx = ['vtd2020', 'county']
            else:
                geoid_calc = f"div(A.block2020, {level_changer(level)})"
                idx = [geoid, 'vtd2020', 'county']

            qry = f"""
select
    {make_select(idx, tbl='D')},
    A.* except ({geoid}),
    B.* except ({geoid}, geometry),
    4 * {np.pi} * atot / (perim * perim) as polsby_popper,
    C.* except ({geoid}),
    D.* except ({join(idx)}),
    geometry,
from (
    select
        {geoid_calc} as {geoid},
        min(dist_to_border) as dist_to_border,
        {make_select([f"sum({col}) as {col}" for col in geo_cols], 2)},
    from
        {blk} as A
    group by
        1
    ) as A
join (
    select
        A.*,
        st_perimeter(A.geometry) as perim,  
    from (
        select
            {geoid_calc} as {geoid},
            st_union_agg(A.geometry) as geometry,
        from
            {blk} as A
        group by
            1
        ) as A
    ) as B
using
    ({geoid})
join (
    select
        {geoid_calc} as {geoid},
        {make_select([f"sum({col}) as {col}" for col in pop_cols], 2)},
    from
        {blk} as A
    group by
        1
    ) as C
using
    ({geoid})
join (
    select
        A.* except (r),
    from (
        select
            {geoid_calc} as {geoid},
            {make_select(district_cols, 3)},
            row_number() over (partition by {geoid_calc} order by all_tot_pop desc) as r,
        from
            {blk} as A
        ) as A
    where
        r = 1
    ) as D
using
    ({geoid})
"""
            # print(qry)
            query_to_table(qry, tbl)
    return tbl





#         sep = ",\n    "
#         qry = f"""
# select
#     {L[0].split('.')[1]}.* except(geometry),
#     st_distance(geometry, (SELECT st_boundary(us_outline_geom) FROM bigquery-public-data.geo_us_boundaries.national_outline)) as dist_to_border,
#     {join([tbl.split('.')[1]+'.* except (year, block2020)' for tbl in L[1:]], sep)},
#     geometry,
# from
#     {L[0]}""" + join([f"\njoin\n    {tbl}\nusing\n    (year, block2020)" for tbl in L[1:]], '')
            # S = []
            # for s in us.STATES:
            #     zip = pq.parent / f'states/tl_2020_{s.fips}_state20.zip'
            #     url = f'https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/STATE/2020/{zip.name}'
            #     download(zip, url, False)
            #     gdf = gpd.read_file(zip)[['geometry']]
            #     S.append(gdf)
            #     if s.fips == STATE.fips:
            #         STATE.geometry = gdf.copy()
            # usa = pd.concat(S).to_crs(CRS['length']).unary_union.buffer(0).simplify(10)
            # A = [g.area for g in usa.geoms]
            # m = max(A)
            # conus = usa.geoms[A.index(m)]
            # border = conus.boundary
            # k+=1; print(str(k))
            # df['atot'] = df.to_crs(CRS['area']).area
            # k+=1; print(str(k))
            # df['perim'] = df.to_crs(CRS['length']).length
            # k+=1; print(str(k))
            # df['polsby_popper'] = 4 * np.pi * df['atot'] / (df['perim']**2)
            # # df['dist_to_border'] = df.to_crs(CRS['length']).distance(border)
            # k+=1; print(str(k))

            # df_to_table(df, tbl_temp)
