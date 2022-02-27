import itertools
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from pplibs.logUtils import secure_log, get_account_config
from pplibs.redshift_client import RedshiftClient
from pplibs.customErrors import ProPairError, report_to_rollbar

def activator(score_df, tier_df, tier_count):
    lo_col = 'profile_id_loan_officer_default'
    new_tier = sum(divmod(tier_count, 2))
    lost = set(tier_df.loc[:, lo_col]) - set(score_df.loc[:, lo_col])
    gained = set(score_df.loc[:, lo_col]) - set(tier_df.loc[:, lo_col])
    if len(lost) > 0:
        secure_log(f'{len(lost)} lost LOs')
        secure_log(lost)
    tier_df = pd.merge(score_df.loc[:, lo_col], tier_df, how='left')

    if len(gained) > 0:
        secure_log(f'{tier_df.tier.isna().sum()} new LOs')
        secure_log(gained)
    return tier_df.fillna(new_tier)

def epoch_updater_ahistorical(df: pd.DataFrame, tier_count: int):
    """"Docstring TBD.

    Args:
        - df: listing ranks of all active LOs, "lower is better", best assigned rank 1.
        - tier_count: number of tiers to evenly divide LOs into, "lower is better".

    Returns:
        - Array with ith entry the tier of los[i] in current epoch.
    """

    out_arr = pd.qcut(df.loc[:, 'lo_scores'], tier_count, labels=False, retbins=False)
    out_arr = tier_count - out_arr
    return pd.concat([df.loc[:, 'profile_id_loan_officer_default'], out_arr], axis=1)


def epoch_updater_end_swap(tiers: pd.DataFrame, ranks: pd.DataFrame, tier_count: int, swap_length: int,
                           swap_method: str):
    ### Make sure there are enough LOs in each tier to perform the swap:
    vcs = tiers.tier.value_counts()
    if vcs[1] < swap_length:
        secure_log(f'Only {vcs[1]} LOs in tier 1 -- insufficient for an end-swap.')
        return tiers
    for i in range(2, tier_count):
        if vcs[i] < 2 * swap_length:
            secure_log(f'Only {vcs[i]} LOs in tier {i} -- insufficient for an end-swap.')
            return tiers
    if vcs[tier_count] < swap_length:
        secure_log(f'Only {vcs[tier_count]} LOs in tier {tier_count} -- insufficient for an end-swap.')
        return tiers

    ### Perform the swap
    lo_col = 'profile_id_loan_officer_default'
    df = pd.merge(tiers, ranks, on=lo_col, how='left')
    df.global_rank.fillna(len(df), inplace=True)

    swappable_dict = []
    for i in range(1, tier_count + 1):
        sorted_vals = df.loc[df['tier'] == i].sort_values('global_rank').loc[:, lo_col]
        sd = {}
        sd["tail"] = sorted_vals.tail(swap_length).index.to_list()
        sd["head"] = sorted_vals.head(swap_length).index.to_list()
        swappable_dict.append(sd)

    if swap_method == 'brutal':
        corr = 1000
    elif swap_method == 'ranked':
        corr = 1

    df.loc[:, 'pre_swap_tier'] = df.loc[:, 'tier']
    for i in range(1, tier_count):
        df.loc[:, "corr_rank"] = np.nan
        head_swaps = swappable_dict[i]["head"]
        tail_swaps = swappable_dict[i - 1]["tail"]
        df.loc[head_swaps, "corr_rank"] = df.loc[head_swaps, 'global_rank']
        df.loc[tail_swaps, "corr_rank"] = corr * df.loc[tail_swaps, 'global_rank']
        df.sort_values("corr_rank", inplace=True)
        df.loc[df.iloc[:swap_length].index, 'tier'] = i
        df.loc[df.iloc[swap_length:2 * swap_length].index, 'tier'] = i + 1
    secure_log(df.loc[df.tier != df.pre_swap_tier, [lo_col, 'pre_swap_tier', 'tier', 'global_rank', 'lo_scores']])
    return df.sort_values(lo_col).loc[:, [lo_col, 'pre_swap_tier', 'tier', 'global_rank']]


class MixLambda:
    """Pull data from DB, bin and plot it, upload results to DB."""

    def __init__(self, meta_data):
        self.event = meta_data
        self.RSC = RedshiftClient(os.environ['ENV'])

    def run(self):
        """Main interface for Lambda to run routine."""

        self.extract_event()
        sql = self.get_sql()
        df = self.RSC.query(sql, return_df=True, name='Main Data')
        self.get_quantiles(self.event.get('quantiles'), self.event.get('weight_dict'))
        score_dfs = self.calc_global_ranks(df)
        prev_tiers = self.get_prev_tiers(score_dfs[0])
        tiers = self.update_tiers(score_dfs, prev_tiers)
        tiers.loc[:, 'account_id'] = self.account_id
        if self.write:
            cols_of_interest = ["account_id", "profile_id_loan_officer_default", "tier"]
            self.tablename = self.write_results(tiers.loc[:, cols_of_interest])
            secure_log(self.tablename)
            self.test_rs()
        else:
            tiers.to_csv(self.filename)
        if self.return_df:
            return tiers
        else:
            return None

    def extract_event(self):
        """Get relevant information from event object."""

        self.args = self.event.get('rs_args', {})
        self.success_var = self.event.get('success_var', 'istp')
        self.first_run = self.event.get('first_run', False)
        self.lead_weight = self.event.get('lead_weight', 'cr_priors_5')
        self.today = self.event.get('today', str(datetime.today()))
        self.today = datetime.fromisoformat(self.today)
        self.eval_weeks = self.event.get('eval_weeks', 5)
        self.swap_weeks = self.event.get('swap_weeks', 2)
        self.eval_weeks += self.swap_weeks
        self.swap_length = self.event.get('swap_length', 3)
        self.swap_method = self.event.get('swap_method', 'ranked')
        self.tier_count = self.event.get('tier_count', 5)
        self.hit_modifier = self.event.get('hit_modifier', 1)
        self.miss_modifier = self.event.get('miss_modifier', 1)
        self.eval_start = self.today - timedelta(weeks=self.swap_weeks + self.eval_weeks)
        self.swap_start = self.today - timedelta(weeks=self.swap_weeks)
        self.account_id = self.event.get('account_id', None)
        self.write = self.event.get('write', False)
        self.tablename = self.event.get('tablename', None)
        self.prev_tablename = self.event.get('prev_tablename', None)
        self.filename = self.event.get('filename', None)
        self.return_df = self.event.get('return_df', False)
        if self.account_id is None:
            raise ValueError("Need to send numeric account_id via event")

    def get_sql(self):
        """Create sql query from composite parts."""

        WHERES = f"""
            WHERE account_id = {self.account_id}
              AND reassignment = 0
              AND custom_rank_timeframe = 0
              AND custom_rank_value IS NOT NULL
              AND created_at >= '{str(self.eval_start)}'
              AND created_at <= '{str(self.today)}'
        """

        sub_sql = f"""
            SELECT account_lead_id,
                   custom_rank_value, 
                   created_at
              FROM summaries
            {WHERES}
        """

        agg_sql = f"""
              SELECT account_lead_id
                FROM summaries
            {WHERES}
            GROUP BY account_lead_id
              HAVING COUNT(custom_rank_value) = 1
        """

        eld_sql = f"""
            SELECT profile_id_loan_officer_default, account_lead_id
              FROM external_lead_details
             WHERE account_id = {self.account_id}
        """

        lc_sql = f"""
            SELECT account_lead_id, {self.success_var}
              FROM lockclose
             WHERE account_id = {self.account_id}
        """

        lo_sql = f"""
            SELECT id
              FROM agent_profiles
             WHERE account_id = {self.account_id}
               AND active = 1
        """

        sql = f"""
            SELECT sub.*, eld.profile_id_loan_officer_default, lc.{self.success_var}
              FROM ({sub_sql}) sub
              JOIN ({agg_sql}) agg
                ON sub.account_lead_id = agg.account_lead_id
              JOIN ({eld_sql}) eld
                ON sub.account_lead_id = eld.account_lead_id
              JOIN ({lc_sql}) lc
                ON sub.account_lead_id = lc.account_lead_id
              JOIN ({lo_sql}) lo
                ON eld.profile_id_loan_officer_default = lo.id
        """
        return sql

    def get_prev_tiers(self, df):
        # need to define df form in and out
        if self.first_run:
            prev_df = epoch_updater_ahistorical(df, self.tier_count)
        elif not self.write:
            prev_df = pd.read_csv(self.filename)
        else:
            sql = f'SELECT * FROM {self.prev_tablename}'
            prev_df = self.RSC.query(sql, return_df=True, name=f'Select from {self.prev_tablename}')
        return prev_df.rename({'lo_scores': 'tier'}, axis=1)

    def get_quantiles(self, eq, wd):
        if not self.first_run:
            if self.write:
                sql = f'SELECT * FROM mix_quantiles WHERE account_id = {self.account_id}'
                quantiles = self.RSC.query(sql, return_df=True, name="Select from mix_quantiles")
                self.quantiles = eval(quantiles.quantiles.iloc[0].replace('inf', 'np.inf'))
                self.weight_dict = eval(quantiles.weight_dict.iloc[0])
        if self.first_run or not self.write:
            self.quantiles = eq
            self.weight_dict = wd
        if getattr(self, 'quantiles') is None:
            raise ValueError('Need to specify quantiles on first run')

    def calc_global_ranks(self, df):
        df.custom_rank_value = df.custom_rank_value.astype('float')
        df.loc[:, self.success_var] = df.loc[:, self.success_var].astype('int')
        tranche_count = len(self.quantiles) - 1
        df.loc[:, 'lead_tranche'] = pd.cut(df.custom_rank_value, self.quantiles,
                                           labels=[tranche_count - x for x in range(tranche_count)])
        df.loc[:, 'CR_value'] = [self.weight_dict[i] for i in df.loc[:, 'lead_tranche']]
        if self.lead_weight.startswith('cr_priors'):
            df.loc[:, 'lo_scores'] = df.loc[:, self.success_var].mul(1 - df.loc[:, "CR_value"]) * self.hit_modifier
            df.loc[:, 'lo_scores'] += (df.loc[:, self.success_var] - 1).mul(df.loc[:, "CR_value"] * self.miss_modifier)
        elif self.lead_weight.startswith('cr_weight'):
            df.loc[:, 'lo_scores'] = df.loc[:, self.success_var].mul(df.loc[:, "CR_value"])

        score_dfs = []
        for sub_df in (
                df.loc[(df.created_at >= self.eval_start) & (df.created_at < self.swap_start)],
                df.loc[df.created_at >= self.swap_start]
        ):
            secure_log(sub_df.created_at.min())
            secure_log(sub_df.created_at.max())
            if self.lead_weight.startswith('cr_priors'):
                score_df = sub_df.groupby('profile_id_loan_officer_default').lo_scores.sum().reset_index()
            elif self.lead_weight.startswith('cr_weight'):
                score_df = sub_df.groupby('profile_id_loan_officer_default').lo_scores.mean().reset_index()
            score_df.loc[:, 'global_rank'] = score_df.lo_scores.rank(method='first', ascending=False).fillna(-1).astype(
                int)
            score_dfs.append(score_df)

        # LOs must appear in evaluation period, not just the swap period
        approved_los = set(score_dfs[0].profile_id_loan_officer_default.values)
        prev_los = set(score_dfs[1].profile_id_loan_officer_default.values)
        score_dfs[1] = score_dfs[1].loc[score_dfs[1]['profile_id_loan_officer_default'].isin(approved_los)]
        secure_log(f'No eval data for LOs: {prev_los - approved_los}')
        return score_dfs

    def update_tiers(self, score_dfs, prev_tiers):
        # Assign tier-0 LOs:
        tiers = activator(score_dfs[0], prev_tiers, self.tier_count)
        tiers.tier = tiers.tier.astype('int')

        # Get count of fully active LOs and desired LOs per tier:
        tier_size, rem = divmod(len(tiers), self.tier_count)

        # Get initial tier count
        cur_tier_counts = tiers.tier.value_counts()

        # Check for imbalance:
        if (cur_tier_counts.max() > tier_size + 1) or (cur_tier_counts.min() < tier_size):
            secure_log('Rebalance occurring')
            secure_log(tiers.tier.value_counts())
            # Compute desired tier sizes, remainder defaulting toward better tiers
            tier_size_lst = [tier_size + 1] * rem + [tier_size] * (self.tier_count - rem)
            tsl_acc = itertools.accumulate(tier_size_lst[::-1])

            rankings = 1000 * tiers.tier + score_dfs[0].loc[:, 'global_rank']
            rankings = pd.concat([tiers.profile_id_loan_officer_default, rankings], axis=1)
            rankings.columns = ['profile_id_loan_officer_default', 'ranking']

            rankings.sort_values('ranking', ascending=False, inplace=True)
            last_ta = 0
            for i, ta in enumerate(tsl_acc):
                rankings.iloc[last_ta:ta].loc[:, 'ranking'] = self.tier_count - i
                last_ta = ta
            rankings.sort_values('profile_id_loan_officer_default', inplace=True)
            tiers = rankings.rename({'ranking': 'tier'}, axis=1)
            secure_log(tiers.tier.value_counts())

        ###Perform swap
        tiers = epoch_updater_end_swap(
            tiers,
            score_dfs[1],
            self.tier_count,
            self.swap_length,
            self.swap_method
        )
        return tiers

    def write_results(self, result):
        """Write result back to redshift."""

        d_quant = f'DELETE FROM mix_quantiles WHERE account_id = {self.account_id}'
        c_quant = f'CREATE TABLE mix_quantiles (account_id int, quantiles text, weight_dict text)'
        i_quant = f"INSERT INTO mix_quantiles values ({self.account_id}, '{self.quantiles}', '{self.weight_dict}');"

        # Connecting to DB
        self.RSC.connect()

        # Attempt to Delete mix_quantiles
        # In case of failure, create the table
        try:
            self.RSC.execute(d_quant, name='delete from mix_quantiles')

        except Exception as e:
            secure_log('Exception during delete from mix_quantiles: {}'.format(e))

            try:
                self.RSC.execute('ROLLBACK;', name='Rollback')
                self.RSC.execute(c_quant, name='create mix_quantiles')
            except Exception as e:
                raise Exception('Exception during create mix_quantiles: {}'.format(e))

        self.RSC.execute(i_quant, name='Insert into mix_quantiles')

        tablename = self.tablename
        account_id = self.account_id

        if tablename is None:
            tablename = 'mix_tiers_{}_{}'.format(account_id, int(time.time()))

        drop = f'DROP TABLE IF EXISTS {tablename}'
        create = f'CREATE TABLE {tablename} (account_id int, profile_id_loan_officer_default int, tier int)'
        insert = f'INSERT INTO {tablename} values '
        x = MixLambda._compile_rows(result, account_id)

        while True:
            try:
                insert += '{}, '.format(next(x))
            except StopIteration:
                break
        insert = insert[:-2] + ';'

        try:
            self.RSC.execute(drop, name=f'drop table {tablename}')
            self.RSC.execute(create, name=f'create table {tablename}')
            self.RSC.execute(insert, name=f'insert into {tablename}')
        except Exception as e:
            self.RSC.execute('ROLLBACK;', name='rollback')
            raise Exception('Exception during write_result: {}'.format(e))
        finally:
            self.RSC.disconnect()

        return tablename

    def test_rs(self):
        """Verify that redshift write was successful."""

        secure_log('\nTesting upload:\n')
        tablename = self.tablename
        upload = self.RSC.query(f"""SELECT * FROM {tablename}""", return_df=True, name='Testing upload')
        secure_log(upload)

    @staticmethod
    def _compile_rows(df, account_id):
        """Generator that yields flattened tuples ready for upload to RS."""

        cols = ['profile_id_loan_officer_default', 'tier']
        for col in cols:
            df.loc[:, col] = df.loc[:, col].astype(int)
        for row in df.loc[:, cols].iterrows():
            yield account_id, row[1][0], row[1][1]


def main(event, context):
    try:
        if 'Records' in event:
            meta_data = json.loads(event['Records'][0]['Sns']['Message'])
        else:
            meta_data = event
        
        config = get_account_config(meta_data.get('account_id'))

        secure_log('::::: Initializing Mix Function', configuration=config)

        ml = MixLambda(meta_data)
        return ml.run()

    except Exception as err:
        secure_log("::::: ERROR ", err)
        secure_log(traceback.format_exc())

        py_err = ProPairError(err, "MixTiers", exec_info=sys.exc_info(), stack=traceback.format_exc())
        try:
            py_err.account = meta_data['account']
            report_to_rollbar(os.environ['ROLLBAR_TOPIC'], py_err)
        except Exception as err:
            py_err.account = "NO_ACCOUNT"
            report_to_rollbar(os.environ['ROLLBAR_TOPIC'], py_err)

