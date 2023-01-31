subpops = {
    'pop_tot_all'  : 'p2_001n',
    'pop_tot_hisp' : 'p2_002n',
    'pop_tot_white': 'p2_005n',
    'pop_tot_other': None,
    'pop_vap_all'  : 'p4_001n',
    'pop_vap_hisp' : 'p4_002n',
    'pop_vap_white': 'p4_005n',
    'pop_vap_other': None,
}

features = {
    'pop_tot_all': ['b01001_001e'],
    'pop_tot_hisp': ['b01001i_001e'],
    'pop_tot_white': ['b01001h_001e'],
    'pop_vap_all': [
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
    'pop_vap_hisp': [
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
    'pop_vap_white': [
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
    'elderly_vap_all': [
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
    'elderly_vap_hisp': [
        'b01001i_013e',
        'b01001i_014e',
        'b01001i_015e',
        'b01001i_016e',
        'b01001i_028e',
        'b01001i_029e',
        'b01001i_030e',
        'b01001i_031e'],
    'elderly_vap_white': [
        'b01001h_013e',
        'b01001h_014e',
        'b01001h_015e',
        'b01001h_016e',
        'b01001h_028e',
        'b01001h_029e',
        'b01001h_030e',
        'b01001h_031e'],
    'poverty_vap_all': [
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
    'poverty_vap_hisp': [
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
    'poverty_vap_white': [
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
    'highschool_vap_all': ['s1501_c01_009e', 's1501_c01_010e', 's1501_c01_011e', 's1501_c01_012e', 's1501_c01_013e'],
    'highschool_vap_hisp': ['s1501_c01_053e'],
    'highschool_vap_white': ['s1501_c01_032e'],
    'homeowner_vap_all': ['b25003_002e'],
    'homeowner_vap_hisp': ['b25003i_002e'],
    'homeowner_vap_white': ['b25003h_002e'],
    'spanishathome_vap_hisp': ['b16004_026e'],
    'spanishathomeenglishwell_vap_hisp': ['b16004_027e', 'b16004_028e'],
}
