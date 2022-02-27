from handler import main
import numpy as np

{
    'account_id': 2,
    'account': 'nbkc',
    'first_run': True,
    'today': '2020-10-01',
    'eval_weeks': 5,
    'swap_weeks': 2,
    'tier_count': 5,
    'hit_modifier': 1,
    'miss_modifier': 1,
    'swap_ratio': 0.15,
    'swap_method': 'ranked',
    'quantiles': [-np.inf, 0.34995463360238743, 0.43985066523199173, np.inf],
    'weight_dict': {3: 0.01707287644787645, 2: 0.03812741312741313, 1: 0.11631274131274132},
    'filename': 'mix_tiers_jfq.csv.gz',
    'tablename': 'mix_tiers_jfq',
    'prev_tablename': 'mix_tiers_jfq',
    'write': True,
    'return_df': True,
    'success_var': 'ilock'
}
main(event, {})