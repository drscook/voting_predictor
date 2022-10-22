from crosswalks import *
from shapes import *

def get_transformer(year=2018, level='tract', overwrite=False):
    dec = decade_(year)
    geoid = f'{level}{dec}'
    tbl = {'blk': f'transformers.{geoid}_block2020', 'vtd': f'transformers.{geoid}_vtd2020'}

    if not check_exists(tbl['vtd'], overwrite):
        cols = [x.replace('_pop', '') for x in [*SUBPOPS.values(), 'other_tot_pop', 'other_vap_pop']]
        if not check_exists(tbl['blk'], overwrite):
            cross = get_crosswalks(year=year)
            shapes = get_shapes(level='block')
            print(f"getting {tbl['blk']}")
            sel = [f'sum(A.prop2020 * B.{x}_pop) as {x}_pop' for x in cols]
            qry = f"""
select
    div(A.block{dec}, {level_changer(level)}) as {geoid},
    A.block2020 as block2020_,
    B.vtd2020,
    sum(A.aland) as aland,
    {make_select(sel)},
from
    {cross} as A
join
    {shapes} as B
using
    (block2020)
group by
    1, 2, 3
"""
            sel = [f'sum({x}_pop) over (partition by {geoid}) as {x}_sum' for x in cols]
            qry = f"""
select
    *,
    count(*) over (partition by {geoid}) as n,
    sum(aland) over (partition by {geoid}) as aland_sum,
    {make_select(sel)},
from (
    {subquery(qry)}
    )
"""
            sel = [f'case when {x}_sum > 0 then {x}_pop / {x}_sum when aland_sum > 0 then aland / aland_sum else 1 / n end as {x}_prop' for x in cols]
            qry = f"""
select
    {geoid},
    block2020_,
    vtd2020,
    case when aland_sum > 0 then aland / aland_sum else 1 / n end as aland_prop,
    {make_select(sel)},
from (
    {subquery(qry)}
    )
"""
            query_to_table(qry, tbl['blk'])
        
        print(f"getting {tbl['vtd']}")
        sel = [f'sum({x}_prop) as {x}_prop' for x in cols]
        qry = f"""
select
    {geoid},
    vtd2020,
    sum(aland_prop) as aland_prop,
    {make_select(sel)},
from
    {tbl['blk']}
group by
    1, 2
"""
        query_to_table(qry, tbl['vtd'])
    return tbl