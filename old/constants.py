import pandas as pd
elipsis = ' ... '

LEVELS = {
    'state':2,
    'county':3,
    'tract':6,
    'block_group':1,
    'block':4,
}

CRS = {
    'census'  : 'EPSG:4269'  , # degrees - used by Census
    'bigquery': 'EPSG:4326'  , # WSG84 - used by Bigquery
    'area'    : 'ESRI:102003', # meters
    'length'  : 'ESRI:102005', # meters
}

SUBPOPS = {
    'p2_001n': 'all_tot_pop',
    'p4_001n': 'all_vap_pop',
    'p2_005n': 'white_tot_pop',
    'p4_005n': 'white_vap_pop',
    'p2_002n': 'hisp_tot_pop',
    'p4_002n': 'hisp_vap_pop',
}

FEATURES = [
    ['all'  , 'tot', 'pop', ['b01001_001e']],
    ['white', 'tot', 'pop', ['b01001h_001e']],
    ['hisp' , 'tot', 'pop', ['b01001i_001e']],


    ['all'  , 'vap', 'pop', [
        'b01001_007e', #pop male 18-19
        'b01001_008e', #pop male 20
        'b01001_009e', #pop male 21
        'b01001_010e', #pop male 22-24
        'b01001_011e', #pop male 25-29
        'b01001_012e', #pop male 30-34
        'b01001_013e', #pop male 35-39
        'b01001_014e', #pop male 40-44
        'b01001_015e', #pop male 45-49
        'b01001_016e', #pop male 50-54
        'b01001_017e', #pop male 55-59
        'b01001_018e', #pop male 60-61
        'b01001_019e', #pop male 62-64
        'b01001_020e', #pop male 65-66
        'b01001_021e', #pop male 67-69
        'b01001_022e', #pop male 70-74
        'b01001_023e', #pop male 75-79
        'b01001_024e', #pop male 80-84
        'b01001_025e', #pop male 85-inf
        'b01001_031e', #pop female 18-19
        'b01001_032e', #pop female 20
        'b01001_033e', #pop female 21
        'b01001_034e', #pop female 22-24
        'b01001_035e', #pop female 25-29
        'b01001_036e', #pop female 30-34
        'b01001_037e', #pop female 35-39
        'b01001_038e', #pop female 40-44
        'b01001_039e', #pop female 45-49
        'b01001_040e', #pop female 50-54
        'b01001_041e', #pop female 55-59
        'b01001_042e', #pop female 60-61
        'b01001_043e', #pop female 62-64
        'b01001_044e', #pop female 65-66
        'b01001_045e', #pop female 67-69
        'b01001_046e', #pop female 70-74
        'b01001_047e', #pop female 75-79
        'b01001_048e', #pop female 80-84
        'b01001_049e', #pop female 85-inf
    ]],
    ['white', 'vap', 'pop', [
        'b01001h_007e', #pop male 18-19
        'b01001h_008e', #pop male 20-24
        'b01001h_009e', #pop male 25-29
        'b01001h_010e', #pop male 30-34
        'b01001h_011e', #pop male 35-44
        'b01001h_012e', #pop male 45-54
        'b01001h_013e', #pop male 55-64
        'b01001h_014e', #pop male 65-74
        'b01001h_015e', #pop male 75-84
        'b01001h_016e', #pop male 85-inf
        'b01001h_022e', #pop female 18-19
        'b01001h_023e', #pop female 20-24
        'b01001h_024e', #pop female 25-29
        'b01001h_025e', #pop female 30-34
        'b01001h_026e', #pop female 35-44
        'b01001h_027e', #pop female 45-54
        'b01001h_028e', #pop female 55-64
        'b01001h_029e', #pop female 65-74
        'b01001h_030e', #pop female 75-84
        'b01001h_031e', #pop female 85-inf
    ]],
    ['hisp' , 'vap', 'pop', [
        'b01001i_007e', #pop male 18-19
        'b01001i_008e', #pop male 20-24
        'b01001i_009e', #pop male 25-29
        'b01001i_010e', #pop male 30-34
        'b01001i_011e', #pop male 35-44
        'b01001i_012e', #pop male 45-54
        'b01001i_013e', #pop male 55-64
        'b01001i_014e', #pop male 65-74
        'b01001i_015e', #pop male 75-84
        'b01001i_016e', #pop male 85-inf
        'b01001i_022e', #pop female 18-19
        'b01001i_023e', #pop female 20-24
        'b01001i_024e', #pop female 25-29
        'b01001i_025e', #pop female 30-34
        'b01001i_026e', #pop female 35-44
        'b01001i_027e', #pop female 45-54
        'b01001i_028e', #pop female 55-64
        'b01001i_029e', #pop female 65-74
        'b01001i_030e', #pop female 75-84
        'b01001i_031e', #pop female 85-inf
    ]],


    ['all'  , 'vap', 'elderly', [
        'b01001_017e', #pop male 55-59
        'b01001_018e', #pop male 60-61
        'b01001_019e', #pop male 62-64
        'b01001_020e', #pop male 65-66
        'b01001_021e', #pop male 67-69
        'b01001_022e', #pop male 70-74
        'b01001_023e', #pop male 75-79
        'b01001_024e', #pop male 80-84
        'b01001_025e', #pop male 85-inf
        'b01001_041e', #pop female 55-59
        'b01001_042e', #pop female 60-61
        'b01001_043e', #pop female 62-64
        'b01001_044e', #pop female 65-66
        'b01001_045e', #pop female 67-69
        'b01001_046e', #pop female 70-74
        'b01001_047e', #pop female 75-79
        'b01001_048e', #pop female 80-84
        'b01001_049e', #pop female 85-inf
    ]],
    ['white', 'vap', 'elderly', [
            'b01001h_013e', #pop male 55-64
            'b01001h_014e', #pop male 65-74
            'b01001h_015e', #pop male 75-84
            'b01001h_016e', #pop male 85-inf
            'b01001h_028e', #pop female 55-64
            'b01001h_029e', #pop female 65-74
            'b01001h_030e', #pop female 75-84
            'b01001h_031e', #pop female 85-inf
    ]],
    ['hisp' , 'vap', 'elderly', [
        'b01001i_013e', #pop male 55-64
        'b01001i_014e', #pop male 65-74
        'b01001i_015e', #pop male 75-84
        'b01001i_016e', #pop male 85-inf  
        'b01001i_028e', #pop female 55-64
        'b01001i_029e', #pop female 65-74
        'b01001i_030e', #pop female 75-84
        'b01001i_031e', #pop female 85-inf
    ]],


    ['all'  , 'vap', 'poverty', [
        'b17001_010e', #poverty male 18-24'
        'b17001_011e', #poverty male 25-34'
        'b17001_012e', #poverty male 35-44'
        'b17001_013e', #poverty male 45-54'
        'b17001_014e', #poverty male 55-64'
        'b17001_015e', #poverty male 65-74'
        'b17001_016e', #poverty male 74-inf'
        'b17001_024e', #poverty female 18-24'
        'b17001_025e', #poverty female 25-34'
        'b17001_026e', #poverty female 35-44'
        'b17001_027e', #poverty female 45-54'
        'b17001_028e', #poverty female 55-64'
        'b17001_029e', #poverty female 65-74'
        'b17001_030e', #poverty female 74-inf'
    ]],
    ['white', 'vap', 'poverty', [
        'b17001h_010e', #poverty male 18-24'
        'b17001h_011e', #poverty male 25-34'
        'b17001h_012e', #poverty male 35-44'
        'b17001h_013e', #poverty male 45-54'
        'b17001h_014e', #poverty male 55-64'
        'b17001h_015e', #poverty male 65-74'
        'b17001h_016e', #poverty male 74-inf'
        'b17001h_024e', #poverty female 18-24'
        'b17001h_025e', #poverty female 25-34'
        'b17001h_026e', #poverty female 35-44'
        'b17001h_027e', #poverty female 45-54'
        'b17001h_028e', #poverty female 55-64'
        'b17001h_029e', #poverty female 65-74'
        'b17001h_030e', #poverty female 74-inf'
    ]],
    ['hisp' , 'vap', 'poverty', [
        'b17001i_010e', #poverty male 18-24'
        'b17001i_011e', #poverty male 25-34'
        'b17001i_012e', #poverty male 35-44'
        'b17001i_013e', #poverty male 45-54'
        'b17001i_014e', #poverty male 55-64'
        'b17001i_015e', #poverty male 65-74'
        'b17001i_016e', #poverty male 74-inf'
        'b17001i_024e', #poverty female 18-24'
        'b17001i_025e', #poverty female 25-34'
        'b17001i_026e', #poverty female 35-44'
        'b17001i_027e', #poverty female 45-54'
        'b17001i_028e', #poverty female 55-64'
        'b17001i_029e', #poverty female 65-74'
        'b17001i_030e', #poverty female 74-inf'
    ]],


    ['all'  , 'vap', 'highschool', [
        # 's1501_c01_003e', #High school graduate (includes equivalency) 18-24
        # 's1501_c01_004e', #Some college or associate's degree 18-24
        # 's1501_c01_005e', #Bachelor's degree or higher 18-24
        's1501_c01_009e', #High school graduate (includes equivalency) 25-inf
        's1501_c01_010e', #Some college, no degree 25-inf
        's1501_c01_011e', #Associate's degree 25-inf
        's1501_c01_012e', #Bachelor's degree 25-inf
        's1501_c01_013e', #Graduate or professional degree 25-inf
    ]],
    ['white', 'vap', 'highschool', ['s1501_c01_032e']],
    ['hisp' , 'vap', 'highschool', ['s1501_c01_053e']],


    ['all'  , 'vap', 'homeowner', ['b25003_002e']],
    ['white', 'vap', 'homeowner', ['b25003h_002e']],
    ['hisp' , 'vap', 'homeowner', ['b25003i_002e']],


    ['hisp' , 'vap', 'spanish_at_home', ['b16004_026e']],
    ['hisp' , 'vap', 'spanish_at_home_english_well', ['b16004_027e', 'b16004_028e']],
]

FEATURES = pd.DataFrame(FEATURES, columns=['race', 'age', 'var', 'cols'])
FEATURES['subpop'] = FEATURES['race'] + '_' + FEATURES['age']
FEATURES['name'] = FEATURES['subpop'] + '_' + FEATURES['var']
FEATURES['age_var'] = FEATURES['age'] + '_' + FEATURES['var']