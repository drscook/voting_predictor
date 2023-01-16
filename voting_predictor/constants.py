levels = {
    'state':2,
    'county':3,
    'tract':6,
    'block_group':1,
    'block':4,
}
crs = {
    'census'  : 'EPSG:4269'  , # degrees - used by Census
    'bigquery': 'EPSG:4326'  , # WSG84 - used by Bigquery
    'area'    : 'ESRI:102003', # meters
    'length'  : 'ESRI:102005', # meters
}
subpops = {
    'all_tot_pop'  : 'p2_001n',
    'all_vap_pop'  : 'p4_001n',
    'white_tot_pop': 'p2_005n',
    'white_vap_pop': 'p4_005n',
    'hisp_tot_pop' : 'p2_002n',
    'hisp_vap_pop' : 'p4_002n',
    'other_tot_pop': None,
    'other_vap_pop': None,
}

features = {
    'all_tot_pop': ['b01001_001e'],
    'white_tot_pop': ['b01001h_001e'],
    'hisp_tot_pop': ['b01001i_001e'],
    'all_vap_pop': [
        'b01001_007e',
        'b01001_008e',
        'b01001_009e',
        'b01001_010e',
        'b01001_011e',
        'b01001_012e',
        'b01001_013e',
        'b01001_014e',
        'b01001_015e',
        'b01001_016e',
        'b01001_017e',
        'b01001_018e',
        'b01001_019e',
        'b01001_020e',
        'b01001_021e',
        'b01001_022e',
        'b01001_023e',
        'b01001_024e',
        'b01001_025e',
        'b01001_031e',
        'b01001_032e',
        'b01001_033e',
        'b01001_034e',
        'b01001_035e',
        'b01001_036e',
        'b01001_037e',
        'b01001_038e',
        'b01001_039e',
        'b01001_040e',
        'b01001_041e',
        'b01001_042e',
        'b01001_043e',
        'b01001_044e',
        'b01001_045e',
        'b01001_046e',
        'b01001_047e',
        'b01001_048e',
        'b01001_049e'],
    'white_vap_pop': [
        'b01001h_007e',
        'b01001h_008e',
        'b01001h_009e',
        'b01001h_010e',
        'b01001h_011e',
        'b01001h_012e',
        'b01001h_013e',
        'b01001h_014e',
        'b01001h_015e',
        'b01001h_016e',
        'b01001h_022e',
        'b01001h_023e',
        'b01001h_024e',
        'b01001h_025e',
        'b01001h_026e',
        'b01001h_027e',
        'b01001h_028e',
        'b01001h_029e',
        'b01001h_030e',
        'b01001h_031e'],
    'hisp_vap_pop': [
        'b01001i_007e',
        'b01001i_008e',
        'b01001i_009e',
        'b01001i_010e',
        'b01001i_011e',
        'b01001i_012e',
        'b01001i_013e',
        'b01001i_014e',
        'b01001i_015e',
        'b01001i_016e',
        'b01001i_022e',
        'b01001i_023e',
        'b01001i_024e',
        'b01001i_025e',
        'b01001i_026e',
        'b01001i_027e',
        'b01001i_028e',
        'b01001i_029e',
        'b01001i_030e',
        'b01001i_031e'],
    'all_vap_elderly': [
        'b01001_017e',
        'b01001_018e',
        'b01001_019e',
        'b01001_020e',
        'b01001_021e',
        'b01001_022e',
        'b01001_023e',
        'b01001_024e',
        'b01001_025e',
        'b01001_041e',
        'b01001_042e',
        'b01001_043e',
        'b01001_044e',
        'b01001_045e',
        'b01001_046e',
        'b01001_047e',
        'b01001_048e',
        'b01001_049e'],
    'white_vap_elderly': [
        'b01001h_013e',
        'b01001h_014e',
        'b01001h_015e',
        'b01001h_016e',
        'b01001h_028e',
        'b01001h_029e',
        'b01001h_030e',
        'b01001h_031e'],
    'hisp_vap_elderly': [
        'b01001i_013e',
        'b01001i_014e',
        'b01001i_015e',
        'b01001i_016e',
        'b01001i_028e',
        'b01001i_029e',
        'b01001i_030e',
        'b01001i_031e'],
    'all_vap_poverty': [
        'b17001_010e',
        'b17001_011e',
        'b17001_012e',
        'b17001_013e',
        'b17001_014e',
        'b17001_015e',
        'b17001_016e',
        'b17001_024e',
        'b17001_025e',
        'b17001_026e',
        'b17001_027e',
        'b17001_028e',
        'b17001_029e',
        'b17001_030e'],
    'white_vap_poverty': [
        'b17001h_010e',
        'b17001h_011e',
        'b17001h_012e',
        'b17001h_013e',
        'b17001h_014e',
        'b17001h_015e',
        'b17001h_016e',
        'b17001h_024e',
        'b17001h_025e',
        'b17001h_026e',
        'b17001h_027e',
        'b17001h_028e',
        'b17001h_029e',
        'b17001h_030e'],
    'hisp_vap_poverty': [
        'b17001i_010e',
        'b17001i_011e',
        'b17001i_012e',
        'b17001i_013e',
        'b17001i_014e',
        'b17001i_015e',
        'b17001i_016e',
        'b17001i_024e',
        'b17001i_025e',
        'b17001i_026e',
        'b17001i_027e',
        'b17001i_028e',
        'b17001i_029e',
        'b17001i_030e'],
    'all_vap_highschool': ['s1501_c01_009e', 's1501_c01_010e', 's1501_c01_011e', 's1501_c01_012e', 's1501_c01_013e'],
    'white_vap_highschool': ['s1501_c01_032e'],
    'hisp_vap_highschool': ['s1501_c01_053e'],
    'all_vap_homeowner': ['b25003_002e'],
    'white_vap_homeowner': ['b25003h_002e'],
    'hisp_vap_homeowner': ['b25003i_002e'],
    'hisp_vap_spanishathome': ['b16004_026e'],
    'hisp_vap_spanishathomeenglishwell': ['b16004_027e', 'b16004_028e'],
}
