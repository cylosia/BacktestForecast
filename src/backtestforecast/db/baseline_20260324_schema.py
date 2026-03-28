from __future__ import annotations

"""Static schema snapshot for Alembic baseline revision 20260324_0001.

Generated from the checked-in ORM metadata so the baseline migration can apply
a deterministic schema snapshot without calling Base.metadata.create_all() at
migration runtime.
"""

BASELINE_TABLE_NAMES = ['historical_ex_dividend_dates',
 'historical_option_day_bars',
 'historical_treasury_yields',
 'historical_underlying_day_bars',
 'nightly_pipeline_runs',
 'option_contract_catalog_snapshots',
 'outbox_messages',
 'task_results',
 'users',
 'audit_events',
 'backtest_runs',
 'backtest_templates',
 'daily_recommendations',
 'multi_step_runs',
 'multi_symbol_runs',
 'scanner_jobs',
 'stripe_events',
 'sweep_jobs',
 'symbol_analyses',
 'backtest_equity_points',
 'backtest_trades',
 'export_jobs',
 'multi_step_equity_points',
 'multi_step_run_steps',
 'multi_step_trades',
 'multi_symbol_equity_points',
 'multi_symbol_run_symbols',
 'multi_symbol_trade_groups',
 'scanner_recommendations',
 'sweep_results',
 'multi_step_step_events',
 'multi_symbol_symbol_equity_points',
 'multi_symbol_trades']

POSTGRESQL_DDL_STATEMENTS = ['CREATE TABLE historical_ex_dividend_dates (\n'
 '\tid UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tex_dividend_date DATE NOT NULL, \n'
 '\tcash_amount NUMERIC(18, 6), \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'rest_dividends' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_historical_ex_dividend_dates PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_ex_dividend_dates_symbol_date UNIQUE (symbol, ex_dividend_date), \n'
 '\tCONSTRAINT ck_historical_ex_dividend_dates_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT ck_historical_ex_dividend_dates_cash_nonneg CHECK (cash_amount IS NULL OR cash_amount >= 0)\n'
 ')',
 'CREATE INDEX ix_historical_ex_dividend_dates_symbol_date ON historical_ex_dividend_dates (symbol, ex_dividend_date)',
 'CREATE TABLE historical_option_day_bars (\n'
 '\tid UUID NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tunderlying_symbol VARCHAR(32) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\texpiration_date DATE NOT NULL, \n'
 '\tcontract_type VARCHAR(8) NOT NULL, \n'
 '\tstrike_price NUMERIC(18, 4) NOT NULL, \n'
 '\topen_price NUMERIC(18, 6) NOT NULL, \n'
 '\thigh_price NUMERIC(18, 6) NOT NULL, \n'
 '\tlow_price NUMERIC(18, 6) NOT NULL, \n'
 '\tclose_price NUMERIC(18, 6) NOT NULL, \n'
 '\tvolume NUMERIC(24, 4) NOT NULL, \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'flatfile_day_aggs' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_historical_option_day_bars PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_option_day_bars_ticker_date UNIQUE (option_ticker, trade_date), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_ticker_not_empty CHECK (length(option_ticker) > 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_symbol_not_empty CHECK (length(underlying_symbol) > 0), \n'
 "\tCONSTRAINT ck_historical_option_day_bars_contract_type CHECK (contract_type IN ('call', 'put')), \n"
 '\tCONSTRAINT ck_historical_option_day_bars_strike_positive CHECK (strike_price > 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_open_nonneg CHECK (open_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_high_nonneg CHECK (high_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_low_nonneg CHECK (low_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_close_nonneg CHECK (close_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_volume_nonneg CHECK (volume >= 0)\n'
 ')',
 'CREATE INDEX ix_historical_option_day_bars_lookup ON historical_option_day_bars (underlying_symbol, trade_date, '
 'contract_type, expiration_date, strike_price)',
 'CREATE INDEX ix_historical_option_day_bars_underlying_date ON historical_option_day_bars (underlying_symbol, '
 'trade_date)',
 'CREATE TABLE historical_treasury_yields (\n'
 '\tid UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tyield_3_month NUMERIC(10, 6) NOT NULL, \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'rest_treasury' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_historical_treasury_yields PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_treasury_yields_trade_date UNIQUE (trade_date), \n'
 '\tCONSTRAINT ck_historical_treasury_yields_3m_range CHECK (yield_3_month >= 0 AND yield_3_month <= 1)\n'
 ')',
 'CREATE INDEX ix_historical_treasury_yields_trade_date ON historical_treasury_yields (trade_date)',
 'CREATE TABLE historical_underlying_day_bars (\n'
 '\tid UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\topen_price NUMERIC(18, 6) NOT NULL, \n'
 '\thigh_price NUMERIC(18, 6) NOT NULL, \n'
 '\tlow_price NUMERIC(18, 6) NOT NULL, \n'
 '\tclose_price NUMERIC(18, 6) NOT NULL, \n'
 '\tvolume NUMERIC(24, 4) NOT NULL, \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'flatfile_day_aggs' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_historical_underlying_day_bars PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_underlying_day_bars_symbol_date UNIQUE (symbol, trade_date), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_open_positive CHECK (open_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_high_positive CHECK (high_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_low_positive CHECK (low_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_close_positive CHECK (close_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_volume_nonneg CHECK (volume >= 0)\n'
 ')',
 'CREATE INDEX ix_historical_underlying_day_bars_symbol_date ON historical_underlying_day_bars (symbol, trade_date)',
 'CREATE TABLE nightly_pipeline_runs (\n'
 '\tid UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 "\tstage VARCHAR(32) DEFAULT 'universe_screen' NOT NULL, \n"
 "\tsymbols_screened INTEGER DEFAULT '0' NOT NULL, \n"
 "\tsymbols_after_screen INTEGER DEFAULT '0' NOT NULL, \n"
 "\tpairs_generated INTEGER DEFAULT '0' NOT NULL, \n"
 "\tquick_backtests_run INTEGER DEFAULT '0' NOT NULL, \n"
 "\tfull_backtests_run INTEGER DEFAULT '0' NOT NULL, \n"
 "\trecommendations_produced INTEGER DEFAULT '0' NOT NULL, \n"
 '\tduration_seconds NUMERIC(10, 2), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\terror_code VARCHAR(64), \n'
 "\tstage_details_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_nightly_pipeline_runs PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_nightly_pipeline_runs_valid_pipeline_status CHECK (status IN ('queued', 'running', 'succeeded', "
 "'failed', 'cancelled')), \n"
 "\tCONSTRAINT ck_nightly_pipeline_runs_valid_stage CHECK (stage IN ('universe_screen', 'strategy_match', "
 "'quick_backtest', 'full_backtest', 'forecast_rank')), \n"
 '\tCONSTRAINT ck_nightly_pipeline_runs_symbols_screened_nonneg CHECK (symbols_screened >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_symbols_after_nonneg CHECK (symbols_after_screen >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_pairs_nonneg CHECK (pairs_generated >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_quick_bt_nonneg CHECK (quick_backtests_run >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_full_bt_nonneg CHECK (full_backtests_run >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_recs_nonneg CHECK (recommendations_produced >= 0)\n'
 ')',
 'CREATE INDEX ix_nightly_pipeline_runs_celery_task_id ON nightly_pipeline_runs (celery_task_id)',
 'CREATE INDEX ix_nightly_pipeline_runs_cursor ON nightly_pipeline_runs (created_at, id)',
 'CREATE INDEX ix_nightly_pipeline_runs_date_status ON nightly_pipeline_runs (trade_date, status)',
 "CREATE INDEX ix_nightly_pipeline_runs_queued ON nightly_pipeline_runs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_nightly_pipeline_runs_status ON nightly_pipeline_runs (status)',
 'CREATE INDEX ix_nightly_pipeline_runs_status_celery_created ON nightly_pipeline_runs (status, celery_task_id, '
 'created_at)',
 'CREATE INDEX ix_nightly_pipeline_runs_status_created ON nightly_pipeline_runs (status, created_at)',
 'CREATE INDEX ix_nightly_pipeline_runs_trade_date ON nightly_pipeline_runs (trade_date)',
 'CREATE UNIQUE INDEX uq_pipeline_runs_succeeded_trade_date ON nightly_pipeline_runs (trade_date) WHERE status = '
 "'succeeded'",
 'CREATE TABLE option_contract_catalog_snapshots (\n'
 '\tid UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tas_of_date DATE NOT NULL, \n'
 '\tcontract_type VARCHAR(8) NOT NULL, \n'
 '\texpiration_date DATE NOT NULL, \n'
 '\tstrike_price_gte NUMERIC(18, 4), \n'
 '\tstrike_price_lte NUMERIC(18, 4), \n'
 "\tcontracts_json JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 "\tcontract_count INTEGER DEFAULT '0' NOT NULL, \n"
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_option_contract_catalog_snapshots PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_option_contract_catalog_snapshots_query UNIQUE (symbol, as_of_date, contract_type, expiration_date, '
 'strike_price_gte, strike_price_lte), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_symbol_not_empty CHECK (length(symbol) > 0), \n'
 "\tCONSTRAINT ck_option_contract_catalog_snapshots_contract_type CHECK (contract_type IN ('call', 'put')), \n"
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_strike_gte_nonneg CHECK (strike_price_gte IS NULL OR '
 'strike_price_gte >= 0), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_strike_lte_nonneg CHECK (strike_price_lte IS NULL OR '
 'strike_price_lte >= 0), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_strike_bounds CHECK (strike_price_gte IS NULL OR strike_price_lte '
 'IS NULL OR strike_price_gte <= strike_price_lte), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_contract_count_nonneg CHECK (contract_count >= 0)\n'
 ')',
 'CREATE INDEX ix_option_contract_catalog_snapshots_lookup ON option_contract_catalog_snapshots (symbol, as_of_date, '
 'contract_type, expiration_date)',
 'CREATE TABLE outbox_messages (\n'
 '\tid UUID NOT NULL, \n'
 '\ttask_name VARCHAR(128) NOT NULL, \n'
 "\ttask_kwargs_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tqueue VARCHAR(64) NOT NULL, \n'
 "\tstatus VARCHAR(16) DEFAULT 'pending' NOT NULL, \n"
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 "\tretry_count INTEGER DEFAULT '0' NOT NULL, \n"
 '\terror_message TEXT, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcorrelation_id UUID, \n'
 '\tCONSTRAINT pk_outbox_messages PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_outbox_messages_valid_status CHECK (status IN ('pending', 'sent', 'failed')), \n"
 '\tCONSTRAINT ck_outbox_messages_retry_count_nonneg CHECK (retry_count >= 0)\n'
 ')',
 'CREATE INDEX ix_outbox_messages_correlation_id ON outbox_messages (correlation_id)',
 'CREATE INDEX ix_outbox_messages_status_created ON outbox_messages (status, created_at)',
 'CREATE TABLE task_results (\n'
 '\tid UUID NOT NULL, \n'
 '\ttask_name VARCHAR(128) NOT NULL, \n'
 '\ttask_id VARCHAR(64) NOT NULL, \n'
 '\tstatus VARCHAR(16) NOT NULL, \n'
 '\tcorrelation_id UUID, \n'
 '\tcorrelation_type VARCHAR(64), \n'
 '\tduration_seconds NUMERIC(10, 3), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\tresult_summary_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tworker_hostname VARCHAR(255), \n'
 "\tretries INTEGER DEFAULT '0' NOT NULL, \n"
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_task_results PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_task_results_valid_status CHECK (status IN ('succeeded', 'failed', 'retried', 'timeout')), \n"
 '\tCONSTRAINT uq_task_results_task_id UNIQUE (task_id)\n'
 ')',
 'CREATE INDEX ix_task_results_correlation_id ON task_results (correlation_id)',
 'CREATE INDEX ix_task_results_status_created ON task_results (status, created_at)',
 'CREATE INDEX ix_task_results_task_name_created ON task_results (task_name, created_at)',
 'CREATE TABLE users (\n'
 '\tid UUID NOT NULL, \n'
 '\tclerk_user_id VARCHAR(255) NOT NULL, \n'
 '\temail VARCHAR(320), \n'
 "\tplan_tier VARCHAR(16) DEFAULT 'free' NOT NULL, \n"
 '\tstripe_customer_id VARCHAR(64), \n'
 '\tstripe_subscription_id VARCHAR(64), \n'
 '\tstripe_price_id VARCHAR(64), \n'
 '\tsubscription_status VARCHAR(32), \n'
 '\tsubscription_billing_interval VARCHAR(16), \n'
 '\tsubscription_current_period_end TIMESTAMP WITH TIME ZONE, \n'
 '\tcancel_at_period_end BOOLEAN DEFAULT false NOT NULL, \n'
 '\tplan_updated_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_users PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_users_valid_plan_tier CHECK (plan_tier IN ('free', 'pro', 'premium')), \n"
 '\tCONSTRAINT ck_users_valid_subscription_status CHECK (subscription_status IS NULL OR subscription_status IN '
 "('incomplete', 'incomplete_expired', 'trialing', 'active', 'past_due', 'canceled', 'unpaid', 'paused')), \n"
 '\tCONSTRAINT ck_users_valid_billing_interval CHECK (subscription_billing_interval IS NULL OR '
 "subscription_billing_interval IN ('monthly', 'yearly')), \n"
 '\tCONSTRAINT ck_users_email_not_empty CHECK (email IS NULL OR length(email) > 0), \n'
 '\tCONSTRAINT uq_users_clerk_user_id UNIQUE (clerk_user_id), \n'
 '\tCONSTRAINT uq_users_stripe_customer_id UNIQUE (stripe_customer_id), \n'
 '\tCONSTRAINT uq_users_stripe_subscription_id UNIQUE (stripe_subscription_id)\n'
 ')',
 'CREATE INDEX ix_users_email ON users (email)',
 'CREATE TABLE audit_events (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID, \n'
 '\trequest_id VARCHAR(64), \n'
 '\tevent_type VARCHAR(128) NOT NULL, \n'
 '\tsubject_type VARCHAR(64) NOT NULL, \n'
 '\tsubject_id VARCHAR(255), \n'
 '\tip_hash VARCHAR(128), \n'
 "\tmetadata_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_audit_events PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_audit_events_dedup UNIQUE (event_type, subject_type, subject_id), \n'
 '\tCONSTRAINT ck_audit_events_subject_id_not_empty CHECK (subject_id IS NULL OR length(subject_id) > 0), \n'
 '\tCONSTRAINT fk_audit_events_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_audit_events_created_at ON audit_events (created_at)',
 'CREATE INDEX ix_audit_events_event_type ON audit_events (event_type)',
 'CREATE INDEX ix_audit_events_event_type_created_at ON audit_events (event_type, created_at)',
 'CREATE INDEX ix_audit_events_user_created_at ON audit_events (user_id, created_at)',
 'CREATE INDEX ix_audit_events_user_id ON audit_events (user_id)',
 'CREATE UNIQUE INDEX uq_audit_events_dedup_null_subject ON audit_events (event_type, subject_type) WHERE subject_id '
 'IS NULL',
 'CREATE TABLE backtest_runs (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tdate_from DATE NOT NULL, \n'
 '\tdate_to DATE NOT NULL, \n'
 '\ttarget_dte INTEGER NOT NULL, \n'
 '\tdte_tolerance_days INTEGER NOT NULL, \n'
 '\tmax_holding_days INTEGER NOT NULL, \n'
 '\taccount_size NUMERIC(18, 4) NOT NULL, \n'
 '\trisk_per_trade_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tcommission_per_contract NUMERIC(18, 4) NOT NULL, \n'
 "\tinput_snapshot_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 "\twarnings_json JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 "\tengine_version VARCHAR(32) DEFAULT 'options-multileg-v2' NOT NULL, \n"
 "\tdata_source VARCHAR(32) DEFAULT 'massive' NOT NULL, \n"
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_win_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_loss_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_holding_period_days NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_dte_at_open NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tprofit_factor NUMERIC(10, 4), \n'
 '\tpayoff_ratio NUMERIC(10, 4), \n'
 "\texpectancy NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tsharpe_ratio NUMERIC(10, 4), \n'
 '\tsortino_ratio NUMERIC(10, 4), \n'
 '\tcagr_pct NUMERIC(10, 4), \n'
 '\tcalmar_ratio NUMERIC(10, 4), \n'
 "\tmax_consecutive_wins INTEGER DEFAULT '0' NOT NULL, \n"
 "\tmax_consecutive_losses INTEGER DEFAULT '0' NOT NULL, \n"
 '\trecovery_factor NUMERIC(10, 4), \n'
 '\trisk_free_rate NUMERIC(6, 4), \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_backtest_runs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_runs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_backtest_runs_valid_run_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_backtest_runs_account_positive CHECK (account_size > 0), \n'
 '\tCONSTRAINT ck_backtest_runs_risk_pct_range CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100), \n'
 '\tCONSTRAINT ck_backtest_runs_commission_nonneg CHECK (commission_per_contract >= 0), \n'
 '\tCONSTRAINT ck_backtest_runs_date_order CHECK (date_from < date_to), \n'
 '\tCONSTRAINT ck_backtest_runs_holding_days_positive CHECK (max_holding_days >= 1), \n'
 '\tCONSTRAINT ck_backtest_runs_target_dte_nonneg CHECK (target_dte >= 0), \n'
 '\tCONSTRAINT ck_backtest_runs_dte_tolerance_nonneg CHECK (dte_tolerance_days >= 0), \n'
 '\tCONSTRAINT ck_backtest_runs_holding_days_range CHECK (max_holding_days >= 1 AND max_holding_days <= 120), \n'
 '\tCONSTRAINT ck_backtest_runs_target_dte_range CHECK (target_dte >= 1 AND target_dte <= 365), \n'
 '\tCONSTRAINT ck_backtest_runs_dte_tolerance_range CHECK (dte_tolerance_days >= 0 AND dte_tolerance_days <= 60), \n'
 '\tCONSTRAINT ck_backtest_runs_account_size_max CHECK (account_size <= 100000000), \n'
 '\tCONSTRAINT ck_backtest_runs_commission_max CHECK (commission_per_contract <= 100), \n'
 "\tCONSTRAINT ck_backtest_runs_valid_engine_version CHECK (engine_version IN ('options-multileg-v1', "
 "'options-multileg-v2')), \n"
 "\tCONSTRAINT ck_backtest_runs_valid_data_source CHECK (data_source IN ('massive', 'manual', "
 "'historical_flatfile')), \n"
 '\tCONSTRAINT ck_backtest_runs_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_backtest_runs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_runs_celery_task_id ON backtest_runs (celery_task_id)',
 'CREATE INDEX ix_backtest_runs_dispatch_started_at ON backtest_runs (dispatch_started_at)',
 "CREATE INDEX ix_backtest_runs_queued ON backtest_runs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_backtest_runs_started_at ON backtest_runs (started_at)',
 'CREATE INDEX ix_backtest_runs_status_celery_created ON backtest_runs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_backtest_runs_user_created_at ON backtest_runs (user_id, created_at)',
 'CREATE INDEX ix_backtest_runs_user_id ON backtest_runs (user_id)',
 'CREATE INDEX ix_backtest_runs_user_status ON backtest_runs (user_id, status)',
 'CREATE INDEX ix_backtest_runs_user_symbol ON backtest_runs (user_id, symbol)',
 'CREATE TABLE backtest_templates (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 '\tname VARCHAR(120) NOT NULL, \n'
 '\tdescription VARCHAR(2000), \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 "\tconfig_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_backtest_templates PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_templates_user_name UNIQUE (user_id, name), \n'
 '\tCONSTRAINT ck_backtest_templates_name_not_empty CHECK (length(name) > 0), \n'
 '\tCONSTRAINT ck_backtest_templates_desc_length CHECK (description IS NULL OR length(description) <= 2000), \n'
 '\tCONSTRAINT fk_backtest_templates_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_templates_user_created_at ON backtest_templates (user_id, created_at)',
 'CREATE INDEX ix_backtest_templates_user_strategy ON backtest_templates (user_id, strategy_type)',
 'CREATE INDEX ix_backtest_templates_user_updated_at ON backtest_templates (user_id, updated_at)',
 'CREATE TABLE daily_recommendations (\n'
 '\tid UUID NOT NULL, \n'
 '\tpipeline_run_id UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\trank INTEGER NOT NULL, \n'
 '\tscore NUMERIC(18, 6) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 "\tregime_labels JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 '\tclose_price NUMERIC(18, 4) NOT NULL, \n'
 '\ttarget_dte INTEGER NOT NULL, \n'
 '\tconfig_snapshot_json JSONB, \n'
 '\tsummary_json JSONB, \n'
 '\tforecast_json JSONB, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_daily_recommendations PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_daily_recs_pipeline_rank UNIQUE (pipeline_run_id, rank), \n'
 '\tCONSTRAINT ck_daily_recommendations_rank_positive CHECK (rank >= 1), \n'
 '\tCONSTRAINT ck_daily_recommendations_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_daily_recommendations_pipeline_run_id_nightly_pipeline_runs FOREIGN KEY(pipeline_run_id) REFERENCES '
 'nightly_pipeline_runs (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_daily_recs_symbol_strategy ON daily_recommendations (symbol, strategy_type)',
 'CREATE INDEX ix_daily_recs_trade_date ON daily_recommendations (trade_date)',
 'CREATE TABLE multi_step_runs (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tname VARCHAR(120), \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tworkflow_type VARCHAR(80) NOT NULL, \n'
 '\tstart_date DATE NOT NULL, \n'
 '\tend_date DATE NOT NULL, \n'
 '\taccount_size NUMERIC(18, 4) NOT NULL, \n'
 '\trisk_per_trade_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tcommission_per_contract NUMERIC(18, 4) NOT NULL, \n'
 "\tslippage_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tinput_snapshot_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 "\twarnings_json JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_win_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_loss_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_holding_period_days NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_dte_at_open NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tprofit_factor NUMERIC(10, 4), \n'
 '\tpayoff_ratio NUMERIC(10, 4), \n'
 "\texpectancy NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tsharpe_ratio NUMERIC(10, 4), \n'
 '\tsortino_ratio NUMERIC(10, 4), \n'
 '\tcagr_pct NUMERIC(10, 4), \n'
 '\tcalmar_ratio NUMERIC(10, 4), \n'
 "\tmax_consecutive_wins INTEGER DEFAULT '0' NOT NULL, \n"
 "\tmax_consecutive_losses INTEGER DEFAULT '0' NOT NULL, \n"
 '\trecovery_factor NUMERIC(10, 4), \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_multi_step_runs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_step_runs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_multi_step_runs_valid_run_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_multi_step_runs_date_order CHECK (start_date < end_date), \n'
 '\tCONSTRAINT ck_multi_step_runs_account_positive CHECK (account_size > 0), \n'
 '\tCONSTRAINT ck_multi_step_runs_risk_pct_range CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100), \n'
 '\tCONSTRAINT ck_multi_step_runs_commission_nonneg CHECK (commission_per_contract >= 0), \n'
 '\tCONSTRAINT ck_multi_step_runs_slippage_range CHECK (slippage_pct >= 0 AND slippage_pct <= 5), \n'
 '\tCONSTRAINT fk_multi_step_runs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_runs_celery_task_id ON multi_step_runs (celery_task_id)',
 'CREATE INDEX ix_multi_step_runs_dispatch_started_at ON multi_step_runs (dispatch_started_at)',
 "CREATE INDEX ix_multi_step_runs_queued ON multi_step_runs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_multi_step_runs_status ON multi_step_runs (status)',
 'CREATE INDEX ix_multi_step_runs_status_celery_created ON multi_step_runs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_multi_step_runs_user_created_at ON multi_step_runs (user_id, created_at)',
 'CREATE INDEX ix_multi_step_runs_user_id ON multi_step_runs (user_id)',
 'CREATE TABLE multi_symbol_runs (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tname VARCHAR(120), \n'
 '\tstart_date DATE NOT NULL, \n'
 '\tend_date DATE NOT NULL, \n'
 '\taccount_size NUMERIC(18, 4) NOT NULL, \n'
 "\tcapital_allocation_mode VARCHAR(24) DEFAULT 'equal_weight' NOT NULL, \n"
 '\tcommission_per_contract NUMERIC(18, 4) NOT NULL, \n'
 "\tslippage_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tinput_snapshot_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 "\twarnings_json JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_win_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_loss_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_holding_period_days NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_dte_at_open NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tprofit_factor NUMERIC(10, 4), \n'
 '\tpayoff_ratio NUMERIC(10, 4), \n'
 "\texpectancy NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tsharpe_ratio NUMERIC(10, 4), \n'
 '\tsortino_ratio NUMERIC(10, 4), \n'
 '\tcagr_pct NUMERIC(10, 4), \n'
 '\tcalmar_ratio NUMERIC(10, 4), \n'
 "\tmax_consecutive_wins INTEGER DEFAULT '0' NOT NULL, \n"
 "\tmax_consecutive_losses INTEGER DEFAULT '0' NOT NULL, \n"
 '\trecovery_factor NUMERIC(10, 4), \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_multi_symbol_runs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_runs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_multi_symbol_runs_valid_run_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_multi_symbol_runs_date_order CHECK (start_date < end_date), \n'
 '\tCONSTRAINT ck_multi_symbol_runs_account_positive CHECK (account_size > 0), \n'
 '\tCONSTRAINT ck_multi_symbol_runs_commission_nonneg CHECK (commission_per_contract >= 0), \n'
 '\tCONSTRAINT ck_multi_symbol_runs_slippage_range CHECK (slippage_pct >= 0 AND slippage_pct <= 5), \n'
 '\tCONSTRAINT fk_multi_symbol_runs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_runs_celery_task_id ON multi_symbol_runs (celery_task_id)',
 'CREATE INDEX ix_multi_symbol_runs_dispatch_started_at ON multi_symbol_runs (dispatch_started_at)',
 "CREATE INDEX ix_multi_symbol_runs_queued ON multi_symbol_runs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_multi_symbol_runs_status ON multi_symbol_runs (status)',
 'CREATE INDEX ix_multi_symbol_runs_status_celery_created ON multi_symbol_runs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_multi_symbol_runs_user_created_at ON multi_symbol_runs (user_id, created_at)',
 'CREATE INDEX ix_multi_symbol_runs_user_id ON multi_symbol_runs (user_id)',
 'CREATE TABLE scanner_jobs (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 '\tparent_job_id UUID, \n'
 '\tpipeline_run_id UUID, \n'
 '\tname VARCHAR(120), \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tmode VARCHAR(16) NOT NULL, \n'
 '\tplan_tier_snapshot VARCHAR(16) NOT NULL, \n'
 "\tjob_kind VARCHAR(32) DEFAULT 'manual' NOT NULL, \n"
 '\trequest_hash VARCHAR(64) NOT NULL, \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\trefresh_key VARCHAR(120), \n'
 '\trefresh_daily BOOLEAN DEFAULT false NOT NULL, \n'
 "\trefresh_priority INTEGER DEFAULT '0' NOT NULL, \n"
 "\tcandidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\tevaluated_candidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\trecommendation_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\trequest_snapshot_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 "\twarnings_json JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 "\tranking_version VARCHAR(32) DEFAULT 'scanner-ranking-v1' NOT NULL, \n"
 "\tengine_version VARCHAR(32) DEFAULT 'options-multileg-v2' NOT NULL, \n"
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_scanner_jobs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_scanner_jobs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 '\tCONSTRAINT uq_scanner_jobs_refresh_key UNIQUE (refresh_key), \n'
 "\tCONSTRAINT ck_scanner_jobs_valid_job_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_plan_tier CHECK (plan_tier_snapshot IN ('free', 'pro', 'premium')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_mode CHECK (mode IN ('basic', 'advanced')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_job_kind CHECK (job_kind IN ('manual', 'refresh', 'nightly')), \n"
 '\tCONSTRAINT ck_scanner_jobs_refresh_priority_range CHECK (refresh_priority >= 0 AND refresh_priority <= 100), \n'
 '\tCONSTRAINT ck_scanner_jobs_candidate_count_nonneg CHECK (candidate_count >= 0), \n'
 '\tCONSTRAINT ck_scanner_jobs_evaluated_count_nonneg CHECK (evaluated_candidate_count >= 0), \n'
 '\tCONSTRAINT ck_scanner_jobs_recommendation_count_nonneg CHECK (recommendation_count >= 0), \n'
 "\tCONSTRAINT ck_scanner_jobs_valid_engine_version CHECK (engine_version IN ('options-multileg-v1', "
 "'options-multileg-v2')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_ranking_version CHECK (ranking_version IN ('scanner-ranking-v1', "
 "'scanner-ranking-v2')), \n"
 '\tCONSTRAINT ck_scanner_jobs_name_not_empty CHECK (name IS NULL OR length(name) > 0), \n'
 '\tCONSTRAINT fk_scanner_jobs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_scanner_jobs_parent_job_id_scanner_jobs FOREIGN KEY(parent_job_id) REFERENCES scanner_jobs (id) ON '
 'DELETE SET NULL, \n'
 '\tCONSTRAINT fk_scanner_jobs_pipeline_run_id_nightly_pipeline_runs FOREIGN KEY(pipeline_run_id) REFERENCES '
 'nightly_pipeline_runs (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_scanner_jobs_celery_task_id ON scanner_jobs (celery_task_id)',
 'CREATE INDEX ix_scanner_jobs_dedup_lookup ON scanner_jobs (user_id, request_hash, mode, created_at)',
 'CREATE INDEX ix_scanner_jobs_dispatch_started_at ON scanner_jobs (dispatch_started_at)',
 'CREATE INDEX ix_scanner_jobs_parent_job_id ON scanner_jobs (parent_job_id)',
 'CREATE INDEX ix_scanner_jobs_pipeline_run_id ON scanner_jobs (pipeline_run_id)',
 "CREATE INDEX ix_scanner_jobs_queued ON scanner_jobs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_scanner_jobs_refresh_sources ON scanner_jobs (refresh_daily, status)',
 'CREATE INDEX ix_scanner_jobs_request_hash ON scanner_jobs (request_hash)',
 'CREATE INDEX ix_scanner_jobs_status_celery_created ON scanner_jobs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_scanner_jobs_user_created_at ON scanner_jobs (user_id, created_at)',
 'CREATE INDEX ix_scanner_jobs_user_id ON scanner_jobs (user_id)',
 'CREATE INDEX ix_scanner_jobs_user_status ON scanner_jobs (user_id, status)',
 'CREATE UNIQUE INDEX uq_scanner_jobs_active_dedup ON scanner_jobs (user_id, request_hash, mode) WHERE status IN '
 "('queued', 'running')",
 'CREATE TABLE stripe_events (\n'
 '\tid UUID NOT NULL, \n'
 '\tstripe_event_id VARCHAR(255) NOT NULL, \n'
 '\tevent_type VARCHAR(128) NOT NULL, \n'
 '\tlivemode BOOLEAN DEFAULT false NOT NULL, \n'
 "\tidempotency_status VARCHAR(16) DEFAULT 'processing' NOT NULL, \n"
 '\tuser_id UUID, \n'
 '\trequest_id VARCHAR(64), \n'
 '\tip_hash VARCHAR(128), \n'
 '\terror_detail TEXT, \n'
 "\tpayload_summary JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_stripe_events PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_stripe_events_event_id UNIQUE (stripe_event_id), \n'
 "\tCONSTRAINT ck_stripe_events_valid_status CHECK (idempotency_status IN ('processing', 'processed', 'ignored', "
 "'error')), \n"
 '\tCONSTRAINT fk_stripe_events_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_stripe_events_created_at ON stripe_events (created_at)',
 'CREATE INDEX ix_stripe_events_event_id_status ON stripe_events (stripe_event_id, idempotency_status)',
 'CREATE INDEX ix_stripe_events_event_type ON stripe_events (event_type)',
 'CREATE INDEX ix_stripe_events_idempotency_status ON stripe_events (idempotency_status)',
 'CREATE INDEX ix_stripe_events_user_id ON stripe_events (user_id)',
 'CREATE TABLE sweep_jobs (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 "\tmode VARCHAR(16) DEFAULT 'grid' NOT NULL, \n"
 "\tplan_tier_snapshot VARCHAR(16) DEFAULT 'free' NOT NULL, \n"
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 "\tcandidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\tevaluated_candidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\tresult_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\trequest_snapshot_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\trequest_hash VARCHAR(64), \n'
 "\twarnings_json JSONB DEFAULT '[]'::jsonb NOT NULL, \n"
 '\tprefetch_summary_json JSONB, \n'
 "\tengine_version VARCHAR(32) DEFAULT 'options-multileg-v2' NOT NULL, \n"
 '\tcelery_task_id VARCHAR(64), \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_sweep_jobs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_sweep_jobs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_sweep_jobs_valid_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_sweep_jobs_candidate_count_nonneg CHECK (candidate_count >= 0), \n'
 '\tCONSTRAINT ck_sweep_jobs_evaluated_count_nonneg CHECK (evaluated_candidate_count >= 0), \n'
 '\tCONSTRAINT ck_sweep_jobs_result_count_nonneg CHECK (result_count >= 0), \n'
 "\tCONSTRAINT ck_sweep_jobs_valid_plan_tier CHECK (plan_tier_snapshot IN ('free', 'pro', 'premium')), \n"
 "\tCONSTRAINT ck_sweep_jobs_valid_engine_version CHECK (engine_version IN ('options-multileg-v1', "
 "'options-multileg-v2')), \n"
 "\tCONSTRAINT ck_sweep_jobs_valid_mode CHECK (mode IN ('grid', 'genetic')), \n"
 '\tCONSTRAINT ck_sweep_jobs_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_sweep_jobs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_sweep_jobs_active_dedup_lookup ON sweep_jobs (user_id, symbol, request_hash, created_at) WHERE '
 "status IN ('queued', 'running') AND request_hash IS NOT NULL",
 'CREATE INDEX ix_sweep_jobs_celery_task_id ON sweep_jobs (celery_task_id)',
 'CREATE INDEX ix_sweep_jobs_dispatch_started_at ON sweep_jobs (dispatch_started_at)',
 "CREATE INDEX ix_sweep_jobs_queued ON sweep_jobs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_sweep_jobs_request_hash ON sweep_jobs (request_hash)',
 'CREATE INDEX ix_sweep_jobs_status_celery_created ON sweep_jobs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_sweep_jobs_user_created_at ON sweep_jobs (user_id, created_at)',
 'CREATE INDEX ix_sweep_jobs_user_id ON sweep_jobs (user_id)',
 'CREATE INDEX ix_sweep_jobs_user_status ON sweep_jobs (user_id, status)',
 'CREATE INDEX ix_sweep_jobs_user_symbol ON sweep_jobs (user_id, symbol)',
 'CREATE INDEX ix_sweep_jobs_user_symbol_created ON sweep_jobs (user_id, symbol, created_at)',
 'CREATE TABLE symbol_analyses (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 "\tstage VARCHAR(32) DEFAULT 'pending' NOT NULL, \n"
 '\tclose_price NUMERIC(18, 4), \n'
 '\tregime_json JSONB, \n'
 '\tlandscape_json JSONB, \n'
 '\ttop_results_json JSONB, \n'
 '\tforecast_json JSONB, \n'
 "\tstrategies_tested INTEGER DEFAULT '0' NOT NULL, \n"
 "\tconfigs_tested INTEGER DEFAULT '0' NOT NULL, \n"
 "\ttop_results_count INTEGER DEFAULT '0' NOT NULL, \n"
 '\tduration_seconds NUMERIC(10, 2), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_symbol_analyses PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_symbol_analyses_user_idempotency UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_symbol_analyses_valid_analysis_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_symbol_analyses_strategies_tested_nonneg CHECK (strategies_tested >= 0), \n'
 '\tCONSTRAINT ck_symbol_analyses_configs_tested_nonneg CHECK (configs_tested >= 0), \n'
 '\tCONSTRAINT ck_symbol_analyses_top_results_nonneg CHECK (top_results_count >= 0), \n'
 "\tCONSTRAINT ck_symbol_analyses_valid_stage CHECK (stage IN ('pending', 'regime', 'landscape', 'deep_dive', "
 "'forecast')), \n"
 '\tCONSTRAINT ck_symbol_analyses_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_symbol_analyses_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_symbol_analyses_celery_task_id ON symbol_analyses (celery_task_id)',
 'CREATE INDEX ix_symbol_analyses_dispatch_started_at ON symbol_analyses (dispatch_started_at)',
 "CREATE INDEX ix_symbol_analyses_queued ON symbol_analyses (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_symbol_analyses_status_celery_created ON symbol_analyses (status, celery_task_id, created_at)',
 'CREATE INDEX ix_symbol_analyses_status_created ON symbol_analyses (status, created_at)',
 'CREATE INDEX ix_symbol_analyses_symbol ON symbol_analyses (symbol)',
 'CREATE INDEX ix_symbol_analyses_user_created ON symbol_analyses (user_id, created_at)',
 'CREATE INDEX ix_symbol_analyses_user_id ON symbol_analyses (user_id)',
 'CREATE TABLE backtest_equity_points (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_backtest_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_equity_points_run_date UNIQUE (run_id, trade_date), \n'
 '\tCONSTRAINT fk_backtest_equity_points_run_id_backtest_runs FOREIGN KEY(run_id) REFERENCES backtest_runs (id) ON '
 'DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_equity_points_run_id ON backtest_equity_points (run_id)',
 'CREATE INDEX ix_backtest_equity_points_trade_date ON backtest_equity_points (trade_date)',
 'CREATE TABLE backtest_trades (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tunderlying_symbol VARCHAR(32) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE NOT NULL, \n'
 '\texpiration_date DATE NOT NULL, \n'
 '\tquantity INTEGER NOT NULL, \n'
 '\tdte_at_open INTEGER NOT NULL, \n'
 '\tholding_period_days INTEGER NOT NULL, \n'
 '\tholding_period_trading_days INTEGER, \n'
 '\tentry_underlying_close NUMERIC(18, 4) NOT NULL, \n'
 '\texit_underlying_close NUMERIC(18, 4) NOT NULL, \n'
 '\tentry_mid NUMERIC(18, 4) NOT NULL, \n'
 '\texit_mid NUMERIC(18, 4) NOT NULL, \n'
 '\tgross_pnl NUMERIC(18, 4) NOT NULL, \n'
 '\tnet_pnl NUMERIC(18, 4) NOT NULL, \n'
 '\ttotal_commissions NUMERIC(18, 4) NOT NULL, \n'
 '\tentry_reason VARCHAR(128) NOT NULL, \n'
 '\texit_reason VARCHAR(128) NOT NULL, \n'
 "\tdetail_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tCONSTRAINT pk_backtest_trades PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_trades_dedup UNIQUE (run_id, entry_date, option_ticker), \n'
 '\tCONSTRAINT ck_backtest_trades_quantity_positive CHECK (quantity > 0), \n'
 '\tCONSTRAINT ck_backtest_trades_date_order CHECK (entry_date <= exit_date), \n'
 '\tCONSTRAINT ck_backtest_trades_dte_at_open_nonneg CHECK (dte_at_open >= 0), \n'
 '\tCONSTRAINT ck_backtest_trades_holding_period_nonneg CHECK (holding_period_days >= 0), \n'
 '\tCONSTRAINT ck_backtest_trades_holding_trading_days_nonneg CHECK (holding_period_trading_days IS NULL OR '
 'holding_period_trading_days >= 0), \n'
 '\tCONSTRAINT fk_backtest_trades_run_id_backtest_runs FOREIGN KEY(run_id) REFERENCES backtest_runs (id) ON DELETE '
 'CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_trades_run_entry_date ON backtest_trades (run_id, entry_date)',
 'CREATE INDEX ix_backtest_trades_run_id ON backtest_trades (run_id)',
 'CREATE TABLE export_jobs (\n'
 '\tid UUID NOT NULL, \n'
 '\tuser_id UUID NOT NULL, \n'
 '\tbacktest_run_id UUID, \n'
 '\tmulti_symbol_run_id UUID, \n'
 '\tmulti_step_run_id UUID, \n'
 "\texport_target_kind VARCHAR(24) DEFAULT 'backtest' NOT NULL, \n"
 '\texport_format VARCHAR(16) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tfile_name VARCHAR(255) NOT NULL, \n'
 '\tmime_type VARCHAR(128) NOT NULL, \n'
 "\tsize_bytes BIGINT DEFAULT '0' NOT NULL, \n"
 '\tsha256_hex VARCHAR(64), \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\tcontent_bytes BYTEA, \n'
 '\tstorage_key VARCHAR(512), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tlast_heartbeat_at TIMESTAMP WITH TIME ZONE, \n'
 '\tdispatch_started_at TIMESTAMP WITH TIME ZONE, \n'
 '\tstarted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tcompleted_at TIMESTAMP WITH TIME ZONE, \n'
 '\texpires_at TIMESTAMP WITH TIME ZONE, \n'
 '\tCONSTRAINT pk_export_jobs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_export_jobs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_export_jobs_valid_export_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled', 'expired')), \n"
 "\tCONSTRAINT ck_export_jobs_valid_target_kind CHECK (export_target_kind IN ('backtest', 'multi_symbol', "
 "'multi_step')), \n"
 '\tCONSTRAINT ck_export_jobs_exactly_one_target CHECK (((CASE WHEN backtest_run_id IS NOT NULL THEN 1 ELSE 0 END) + '
 '(CASE WHEN multi_symbol_run_id IS NOT NULL THEN 1 ELSE 0 END) + (CASE WHEN multi_step_run_id IS NOT NULL THEN 1 ELSE '
 '0 END)) = 1), \n'
 "\tCONSTRAINT ck_export_jobs_succeeded_has_storage CHECK (status != 'succeeded' OR content_bytes IS NOT NULL OR "
 'storage_key IS NOT NULL), \n'
 '\tCONSTRAINT ck_export_jobs_size_bytes_nonneg CHECK (size_bytes >= 0), \n'
 "\tCONSTRAINT ck_export_jobs_valid_export_format CHECK (export_format IN ('csv', 'pdf')), \n"
 '\tCONSTRAINT fk_export_jobs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_export_jobs_backtest_run_id_backtest_runs FOREIGN KEY(backtest_run_id) REFERENCES backtest_runs (id) '
 'ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_export_jobs_multi_symbol_run_id_multi_symbol_runs FOREIGN KEY(multi_symbol_run_id) REFERENCES '
 'multi_symbol_runs (id) ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_export_jobs_multi_step_run_id_multi_step_runs FOREIGN KEY(multi_step_run_id) REFERENCES '
 'multi_step_runs (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_export_jobs_backtest_run_id ON export_jobs (backtest_run_id)',
 'CREATE INDEX ix_export_jobs_celery_task_id ON export_jobs (celery_task_id)',
 'CREATE INDEX ix_export_jobs_dispatch_started_at ON export_jobs (dispatch_started_at)',
 'CREATE INDEX ix_export_jobs_multi_step_run_id ON export_jobs (multi_step_run_id)',
 'CREATE INDEX ix_export_jobs_multi_symbol_run_id ON export_jobs (multi_symbol_run_id)',
 "CREATE INDEX ix_export_jobs_queued ON export_jobs (created_at) WHERE status = 'queued'",
 'CREATE INDEX ix_export_jobs_sha256_hex ON export_jobs (sha256_hex)',
 'CREATE INDEX ix_export_jobs_status_celery_created ON export_jobs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_export_jobs_status_expires_at ON export_jobs (status, expires_at)',
 'CREATE INDEX ix_export_jobs_storage_key ON export_jobs (storage_key)',
 'CREATE INDEX ix_export_jobs_user_created_at ON export_jobs (user_id, created_at)',
 'CREATE INDEX ix_export_jobs_user_id ON export_jobs (user_id)',
 'CREATE INDEX ix_export_jobs_user_status ON export_jobs (user_id, status)',
 'CREATE TABLE multi_step_equity_points (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_multi_step_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_step_equity_points_run_date UNIQUE (run_id, trade_date), \n'
 '\tCONSTRAINT fk_multi_step_equity_points_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) '
 'ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_equity_points_run_id ON multi_step_equity_points (run_id)',
 'CREATE TABLE multi_step_run_steps (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\tstep_number INTEGER NOT NULL, \n'
 '\tname VARCHAR(120) NOT NULL, \n'
 '\taction VARCHAR(32) NOT NULL, \n'
 "\ttrigger_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 "\tcontract_selection_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 "\tfailure_policy VARCHAR(32) DEFAULT 'liquidate' NOT NULL, \n"
 "\tstatus VARCHAR(24) DEFAULT 'pending' NOT NULL, \n"
 '\ttriggered_at TIMESTAMP WITH TIME ZONE, \n'
 '\texecuted_at TIMESTAMP WITH TIME ZONE, \n'
 '\tfailure_reason TEXT, \n'
 '\tCONSTRAINT pk_multi_step_run_steps PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_step_run_steps_run_step_number UNIQUE (run_id, step_number), \n'
 '\tCONSTRAINT fk_multi_step_run_steps_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) ON '
 'DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_run_steps_run_id ON multi_step_run_steps (run_id)',
 'CREATE TABLE multi_step_trades (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\tstep_number INTEGER NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE NOT NULL, \n'
 '\texpiration_date DATE, \n'
 '\tquantity INTEGER NOT NULL, \n'
 '\tdte_at_open INTEGER, \n'
 '\tholding_period_days INTEGER, \n'
 '\tentry_underlying_close NUMERIC(18, 4), \n'
 '\texit_underlying_close NUMERIC(18, 4), \n'
 '\tentry_mid NUMERIC(18, 4), \n'
 '\texit_mid NUMERIC(18, 4), \n'
 "\tgross_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tnet_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tentry_reason VARCHAR(128) NOT NULL, \n'
 '\texit_reason VARCHAR(128) NOT NULL, \n'
 "\tdetail_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tCONSTRAINT pk_multi_step_trades PRIMARY KEY (id), \n'
 '\tCONSTRAINT ck_multi_step_trades_quantity_positive CHECK (quantity > 0), \n'
 '\tCONSTRAINT ck_multi_step_trades_date_order CHECK (entry_date <= exit_date), \n'
 '\tCONSTRAINT fk_multi_step_trades_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) ON '
 'DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_trades_run_entry_date ON multi_step_trades (run_id, entry_date)',
 'CREATE INDEX ix_multi_step_trades_run_id ON multi_step_trades (run_id)',
 'CREATE TABLE multi_symbol_equity_points (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_multi_symbol_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_equity_points_run_date UNIQUE (run_id, trade_date), \n'
 '\tCONSTRAINT fk_multi_symbol_equity_points_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs '
 '(id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_equity_points_run_id ON multi_symbol_equity_points (run_id)',
 'CREATE TABLE multi_symbol_run_symbols (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\trisk_per_trade_pct NUMERIC(10, 4) NOT NULL, \n'
 "\tmax_open_positions INTEGER DEFAULT '1' NOT NULL, \n"
 '\tcapital_allocation_pct NUMERIC(10, 4), \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tCONSTRAINT pk_multi_symbol_run_symbols PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_run_symbols_run_symbol UNIQUE (run_id, symbol), \n'
 '\tCONSTRAINT ck_multi_symbol_run_symbols_risk_pct_range CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= '
 '100), \n'
 '\tCONSTRAINT ck_multi_symbol_run_symbols_max_open_positions_positive CHECK (max_open_positions >= 1), \n'
 '\tCONSTRAINT fk_multi_symbol_run_symbols_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs '
 '(id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_run_symbols_run_id ON multi_symbol_run_symbols (run_id)',
 'CREATE TABLE multi_symbol_trade_groups (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE, \n'
 "\tstatus VARCHAR(16) DEFAULT 'open' NOT NULL, \n"
 "\tdetail_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tCONSTRAINT pk_multi_symbol_trade_groups PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_multi_symbol_trade_groups_status CHECK (status IN ('open', 'closed', 'cancelled')), \n"
 '\tCONSTRAINT fk_multi_symbol_trade_groups_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs '
 '(id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_trade_groups_run_entry_date ON multi_symbol_trade_groups (run_id, entry_date)',
 'CREATE INDEX ix_multi_symbol_trade_groups_run_id ON multi_symbol_trade_groups (run_id)',
 'CREATE TABLE scanner_recommendations (\n'
 '\tid UUID NOT NULL, \n'
 '\tscanner_job_id UUID NOT NULL, \n'
 '\trank INTEGER NOT NULL, \n'
 '\tscore NUMERIC(18, 6) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\trule_set_name VARCHAR(120) NOT NULL, \n'
 '\trule_set_hash VARCHAR(64) NOT NULL, \n'
 '\trequest_snapshot_json JSONB NOT NULL, \n'
 '\tsummary_json JSONB NOT NULL, \n'
 '\twarnings_json JSONB NOT NULL, \n'
 '\ttrades_json JSONB NOT NULL, \n'
 '\tequity_curve_json JSONB NOT NULL, \n'
 '\thistorical_performance_json JSONB NOT NULL, \n'
 '\tforecast_json JSONB NOT NULL, \n'
 '\tranking_features_json JSONB NOT NULL, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_scanner_recommendations PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_scanner_recommendations_job_rank UNIQUE (scanner_job_id, rank), \n'
 '\tCONSTRAINT ck_scanner_recommendations_rank_positive CHECK (rank >= 1), \n'
 '\tCONSTRAINT ck_scanner_recommendations_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_scanner_recommendations_scanner_job_id_scanner_jobs FOREIGN KEY(scanner_job_id) REFERENCES '
 'scanner_jobs (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_scanner_recommendations_lookup ON scanner_recommendations (symbol, strategy_type, rule_set_hash)',
 'CREATE INDEX ix_scanner_recommendations_summary_gin ON scanner_recommendations USING gin (summary_json '
 'jsonb_path_ops)',
 'CREATE TABLE sweep_results (\n'
 '\tid UUID NOT NULL, \n'
 '\tsweep_job_id UUID NOT NULL, \n'
 '\trank INTEGER NOT NULL, \n'
 '\tscore NUMERIC(18, 6) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tparameter_snapshot_json JSONB NOT NULL, \n'
 '\tsummary_json JSONB NOT NULL, \n'
 '\twarnings_json JSONB NOT NULL, \n'
 '\ttrades_json JSONB NOT NULL, \n'
 '\tequity_curve_json JSONB NOT NULL, \n'
 '\tcreated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tupdated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tCONSTRAINT pk_sweep_results PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_sweep_results_job_rank UNIQUE (sweep_job_id, rank), \n'
 '\tCONSTRAINT ck_sweep_results_rank_positive CHECK (rank >= 1), \n'
 '\tCONSTRAINT fk_sweep_results_sweep_job_id_sweep_jobs FOREIGN KEY(sweep_job_id) REFERENCES sweep_jobs (id) ON DELETE '
 'CASCADE\n'
 ')',
 'CREATE INDEX ix_sweep_results_job_id ON sweep_results (sweep_job_id)',
 'CREATE INDEX ix_sweep_results_summary_gin ON sweep_results USING gin (summary_json jsonb_path_ops)',
 'CREATE TABLE multi_step_step_events (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\tstep_id UUID, \n'
 '\tstep_number INTEGER NOT NULL, \n'
 '\tevent_type VARCHAR(24) NOT NULL, \n'
 '\tevent_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL, \n'
 '\tmessage TEXT, \n'
 "\tpayload_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tCONSTRAINT pk_multi_step_step_events PRIMARY KEY (id), \n'
 '\tCONSTRAINT fk_multi_step_step_events_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) ON '
 'DELETE CASCADE, \n'
 '\tCONSTRAINT fk_multi_step_step_events_step_id_multi_step_run_steps FOREIGN KEY(step_id) REFERENCES '
 'multi_step_run_steps (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_multi_step_step_events_run_event_at ON multi_step_step_events (run_id, event_at)',
 'CREATE INDEX ix_multi_step_step_events_run_id ON multi_step_step_events (run_id)',
 'CREATE INDEX ix_multi_step_step_events_step_number ON multi_step_step_events (step_number)',
 'CREATE TABLE multi_symbol_symbol_equity_points (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_symbol_id UUID NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_multi_symbol_symbol_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_symbol_equity_points_symbol_date UNIQUE (run_symbol_id, trade_date), \n'
 '\tCONSTRAINT fk_multi_symbol_symbol_equity_points_run_symbol_id_mult_53e4 FOREIGN KEY(run_symbol_id) REFERENCES '
 'multi_symbol_run_symbols (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_symbol_equity_points_run_symbol_id ON multi_symbol_symbol_equity_points (run_symbol_id)',
 'CREATE TABLE multi_symbol_trades (\n'
 '\tid UUID NOT NULL, \n'
 '\trun_id UUID NOT NULL, \n'
 '\ttrade_group_id UUID NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE NOT NULL, \n'
 '\texpiration_date DATE, \n'
 '\tquantity INTEGER NOT NULL, \n'
 '\tdte_at_open INTEGER, \n'
 '\tholding_period_days INTEGER, \n'
 '\tentry_underlying_close NUMERIC(18, 4), \n'
 '\texit_underlying_close NUMERIC(18, 4), \n'
 '\tentry_mid NUMERIC(18, 4), \n'
 '\texit_mid NUMERIC(18, 4), \n'
 "\tgross_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tnet_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tentry_reason VARCHAR(128) NOT NULL, \n'
 '\texit_reason VARCHAR(128) NOT NULL, \n'
 "\tdetail_json JSONB DEFAULT '{}'::jsonb NOT NULL, \n"
 '\tCONSTRAINT pk_multi_symbol_trades PRIMARY KEY (id), \n'
 '\tCONSTRAINT ck_multi_symbol_trades_quantity_positive CHECK (quantity > 0), \n'
 '\tCONSTRAINT ck_multi_symbol_trades_date_order CHECK (entry_date <= exit_date), \n'
 '\tCONSTRAINT fk_multi_symbol_trades_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs (id) '
 'ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_multi_symbol_trades_trade_group_id_multi_symbol_trade_groups FOREIGN KEY(trade_group_id) REFERENCES '
 'multi_symbol_trade_groups (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_trades_run_entry_date ON multi_symbol_trades (run_id, entry_date)',
 'CREATE INDEX ix_multi_symbol_trades_run_id ON multi_symbol_trades (run_id)',
 'CREATE INDEX ix_multi_symbol_trades_trade_group_id ON multi_symbol_trades (trade_group_id)']

SQLITE_DDL_STATEMENTS = ['CREATE TABLE historical_ex_dividend_dates (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tex_dividend_date DATE NOT NULL, \n'
 '\tcash_amount NUMERIC(18, 6), \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'rest_dividends' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_historical_ex_dividend_dates PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_ex_dividend_dates_symbol_date UNIQUE (symbol, ex_dividend_date), \n'
 '\tCONSTRAINT ck_historical_ex_dividend_dates_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT ck_historical_ex_dividend_dates_cash_nonneg CHECK (cash_amount IS NULL OR cash_amount >= 0)\n'
 ')',
 'CREATE INDEX ix_historical_ex_dividend_dates_symbol_date ON historical_ex_dividend_dates (symbol, ex_dividend_date)',
 'CREATE TABLE historical_option_day_bars (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tunderlying_symbol VARCHAR(32) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\texpiration_date DATE NOT NULL, \n'
 '\tcontract_type VARCHAR(8) NOT NULL, \n'
 '\tstrike_price NUMERIC(18, 4) NOT NULL, \n'
 '\topen_price NUMERIC(18, 6) NOT NULL, \n'
 '\thigh_price NUMERIC(18, 6) NOT NULL, \n'
 '\tlow_price NUMERIC(18, 6) NOT NULL, \n'
 '\tclose_price NUMERIC(18, 6) NOT NULL, \n'
 '\tvolume NUMERIC(24, 4) NOT NULL, \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'flatfile_day_aggs' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_historical_option_day_bars PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_option_day_bars_ticker_date UNIQUE (option_ticker, trade_date), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_ticker_not_empty CHECK (length(option_ticker) > 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_symbol_not_empty CHECK (length(underlying_symbol) > 0), \n'
 "\tCONSTRAINT ck_historical_option_day_bars_contract_type CHECK (contract_type IN ('call', 'put')), \n"
 '\tCONSTRAINT ck_historical_option_day_bars_strike_positive CHECK (strike_price > 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_open_nonneg CHECK (open_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_high_nonneg CHECK (high_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_low_nonneg CHECK (low_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_close_nonneg CHECK (close_price >= 0), \n'
 '\tCONSTRAINT ck_historical_option_day_bars_volume_nonneg CHECK (volume >= 0)\n'
 ')',
 'CREATE INDEX ix_historical_option_day_bars_lookup ON historical_option_day_bars (underlying_symbol, trade_date, '
 'contract_type, expiration_date, strike_price)',
 'CREATE INDEX ix_historical_option_day_bars_underlying_date ON historical_option_day_bars (underlying_symbol, '
 'trade_date)',
 'CREATE TABLE historical_treasury_yields (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tyield_3_month NUMERIC(10, 6) NOT NULL, \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'rest_treasury' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_historical_treasury_yields PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_treasury_yields_trade_date UNIQUE (trade_date), \n'
 '\tCONSTRAINT ck_historical_treasury_yields_3m_range CHECK (yield_3_month >= 0 AND yield_3_month <= 1)\n'
 ')',
 'CREATE INDEX ix_historical_treasury_yields_trade_date ON historical_treasury_yields (trade_date)',
 'CREATE TABLE historical_underlying_day_bars (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\topen_price NUMERIC(18, 6) NOT NULL, \n'
 '\thigh_price NUMERIC(18, 6) NOT NULL, \n'
 '\tlow_price NUMERIC(18, 6) NOT NULL, \n'
 '\tclose_price NUMERIC(18, 6) NOT NULL, \n'
 '\tvolume NUMERIC(24, 4) NOT NULL, \n'
 "\tsource_dataset VARCHAR(64) DEFAULT 'flatfile_day_aggs' NOT NULL, \n"
 '\tsource_file_date DATE NOT NULL, \n'
 '\tingested_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_historical_underlying_day_bars PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_historical_underlying_day_bars_symbol_date UNIQUE (symbol, trade_date), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_open_positive CHECK (open_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_high_positive CHECK (high_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_low_positive CHECK (low_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_close_positive CHECK (close_price > 0), \n'
 '\tCONSTRAINT ck_historical_underlying_day_bars_volume_nonneg CHECK (volume >= 0)\n'
 ')',
 'CREATE INDEX ix_historical_underlying_day_bars_symbol_date ON historical_underlying_day_bars (symbol, trade_date)',
 'CREATE TABLE nightly_pipeline_runs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 "\tstage VARCHAR(32) DEFAULT 'universe_screen' NOT NULL, \n"
 "\tsymbols_screened INTEGER DEFAULT '0' NOT NULL, \n"
 "\tsymbols_after_screen INTEGER DEFAULT '0' NOT NULL, \n"
 "\tpairs_generated INTEGER DEFAULT '0' NOT NULL, \n"
 "\tquick_backtests_run INTEGER DEFAULT '0' NOT NULL, \n"
 "\tfull_backtests_run INTEGER DEFAULT '0' NOT NULL, \n"
 "\trecommendations_produced INTEGER DEFAULT '0' NOT NULL, \n"
 '\tduration_seconds NUMERIC(10, 2), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\terror_code VARCHAR(64), \n'
 "\tstage_details_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_nightly_pipeline_runs PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_nightly_pipeline_runs_valid_pipeline_status CHECK (status IN ('queued', 'running', 'succeeded', "
 "'failed', 'cancelled')), \n"
 "\tCONSTRAINT ck_nightly_pipeline_runs_valid_stage CHECK (stage IN ('universe_screen', 'strategy_match', "
 "'quick_backtest', 'full_backtest', 'forecast_rank')), \n"
 '\tCONSTRAINT ck_nightly_pipeline_runs_symbols_screened_nonneg CHECK (symbols_screened >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_symbols_after_nonneg CHECK (symbols_after_screen >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_pairs_nonneg CHECK (pairs_generated >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_quick_bt_nonneg CHECK (quick_backtests_run >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_full_bt_nonneg CHECK (full_backtests_run >= 0), \n'
 '\tCONSTRAINT ck_nightly_pipeline_runs_recs_nonneg CHECK (recommendations_produced >= 0)\n'
 ')',
 'CREATE INDEX ix_nightly_pipeline_runs_celery_task_id ON nightly_pipeline_runs (celery_task_id)',
 'CREATE INDEX ix_nightly_pipeline_runs_cursor ON nightly_pipeline_runs (created_at, id)',
 'CREATE INDEX ix_nightly_pipeline_runs_date_status ON nightly_pipeline_runs (trade_date, status)',
 'CREATE INDEX ix_nightly_pipeline_runs_queued ON nightly_pipeline_runs (created_at)',
 'CREATE INDEX ix_nightly_pipeline_runs_status ON nightly_pipeline_runs (status)',
 'CREATE INDEX ix_nightly_pipeline_runs_status_celery_created ON nightly_pipeline_runs (status, celery_task_id, '
 'created_at)',
 'CREATE INDEX ix_nightly_pipeline_runs_status_created ON nightly_pipeline_runs (status, created_at)',
 'CREATE INDEX ix_nightly_pipeline_runs_trade_date ON nightly_pipeline_runs (trade_date)',
 'CREATE UNIQUE INDEX uq_pipeline_runs_succeeded_trade_date ON nightly_pipeline_runs (trade_date)',
 'CREATE TABLE option_contract_catalog_snapshots (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tas_of_date DATE NOT NULL, \n'
 '\tcontract_type VARCHAR(8) NOT NULL, \n'
 '\texpiration_date DATE NOT NULL, \n'
 '\tstrike_price_gte NUMERIC(18, 4), \n'
 '\tstrike_price_lte NUMERIC(18, 4), \n'
 "\tcontracts_json JSON DEFAULT '[]' NOT NULL, \n"
 "\tcontract_count INTEGER DEFAULT '0' NOT NULL, \n"
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_option_contract_catalog_snapshots PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_option_contract_catalog_snapshots_query UNIQUE (symbol, as_of_date, contract_type, expiration_date, '
 'strike_price_gte, strike_price_lte), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_symbol_not_empty CHECK (length(symbol) > 0), \n'
 "\tCONSTRAINT ck_option_contract_catalog_snapshots_contract_type CHECK (contract_type IN ('call', 'put')), \n"
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_strike_gte_nonneg CHECK (strike_price_gte IS NULL OR '
 'strike_price_gte >= 0), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_strike_lte_nonneg CHECK (strike_price_lte IS NULL OR '
 'strike_price_lte >= 0), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_strike_bounds CHECK (strike_price_gte IS NULL OR strike_price_lte '
 'IS NULL OR strike_price_gte <= strike_price_lte), \n'
 '\tCONSTRAINT ck_option_contract_catalog_snapshots_contract_count_nonneg CHECK (contract_count >= 0)\n'
 ')',
 'CREATE INDEX ix_option_contract_catalog_snapshots_lookup ON option_contract_catalog_snapshots (symbol, as_of_date, '
 'contract_type, expiration_date)',
 'CREATE TABLE outbox_messages (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\ttask_name VARCHAR(128) NOT NULL, \n'
 "\ttask_kwargs_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tqueue VARCHAR(64) NOT NULL, \n'
 "\tstatus VARCHAR(16) DEFAULT 'pending' NOT NULL, \n"
 '\tstarted_at DATETIME, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 "\tretry_count INTEGER DEFAULT '0' NOT NULL, \n"
 '\terror_message TEXT, \n'
 '\tcompleted_at DATETIME, \n'
 '\tcorrelation_id CHAR(36), \n'
 '\tCONSTRAINT pk_outbox_messages PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_outbox_messages_valid_status CHECK (status IN ('pending', 'sent', 'failed')), \n"
 '\tCONSTRAINT ck_outbox_messages_retry_count_nonneg CHECK (retry_count >= 0)\n'
 ')',
 'CREATE INDEX ix_outbox_messages_correlation_id ON outbox_messages (correlation_id)',
 'CREATE INDEX ix_outbox_messages_status_created ON outbox_messages (status, created_at)',
 'CREATE TABLE task_results (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\ttask_name VARCHAR(128) NOT NULL, \n'
 '\ttask_id VARCHAR(64) NOT NULL, \n'
 '\tstatus VARCHAR(16) NOT NULL, \n'
 '\tcorrelation_id CHAR(36), \n'
 '\tcorrelation_type VARCHAR(64), \n'
 '\tduration_seconds NUMERIC(10, 3), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\tresult_summary_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tworker_hostname VARCHAR(255), \n'
 "\tretries INTEGER DEFAULT '0' NOT NULL, \n"
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tcompleted_at DATETIME, \n'
 '\tCONSTRAINT pk_task_results PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_task_results_valid_status CHECK (status IN ('succeeded', 'failed', 'retried', 'timeout')), \n"
 '\tCONSTRAINT uq_task_results_task_id UNIQUE (task_id)\n'
 ')',
 'CREATE INDEX ix_task_results_correlation_id ON task_results (correlation_id)',
 'CREATE INDEX ix_task_results_status_created ON task_results (status, created_at)',
 'CREATE INDEX ix_task_results_task_name_created ON task_results (task_name, created_at)',
 'CREATE TABLE users (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tclerk_user_id VARCHAR(255) NOT NULL, \n'
 '\temail VARCHAR(320), \n'
 "\tplan_tier VARCHAR(16) DEFAULT 'free' NOT NULL, \n"
 '\tstripe_customer_id VARCHAR(64), \n'
 '\tstripe_subscription_id VARCHAR(64), \n'
 '\tstripe_price_id VARCHAR(64), \n'
 '\tsubscription_status VARCHAR(32), \n'
 '\tsubscription_billing_interval VARCHAR(16), \n'
 '\tsubscription_current_period_end DATETIME, \n'
 '\tcancel_at_period_end BOOLEAN DEFAULT false NOT NULL, \n'
 '\tplan_updated_at DATETIME, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_users PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_users_valid_plan_tier CHECK (plan_tier IN ('free', 'pro', 'premium')), \n"
 '\tCONSTRAINT ck_users_valid_subscription_status CHECK (subscription_status IS NULL OR subscription_status IN '
 "('incomplete', 'incomplete_expired', 'trialing', 'active', 'past_due', 'canceled', 'unpaid', 'paused')), \n"
 '\tCONSTRAINT ck_users_valid_billing_interval CHECK (subscription_billing_interval IS NULL OR '
 "subscription_billing_interval IN ('monthly', 'yearly')), \n"
 '\tCONSTRAINT ck_users_email_not_empty CHECK (email IS NULL OR length(email) > 0), \n'
 '\tCONSTRAINT uq_users_clerk_user_id UNIQUE (clerk_user_id), \n'
 '\tCONSTRAINT uq_users_stripe_customer_id UNIQUE (stripe_customer_id), \n'
 '\tCONSTRAINT uq_users_stripe_subscription_id UNIQUE (stripe_subscription_id)\n'
 ')',
 'CREATE INDEX ix_users_email ON users (email)',
 'CREATE TABLE audit_events (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36), \n'
 '\trequest_id VARCHAR(64), \n'
 '\tevent_type VARCHAR(128) NOT NULL, \n'
 '\tsubject_type VARCHAR(64) NOT NULL, \n'
 '\tsubject_id VARCHAR(255), \n'
 '\tip_hash VARCHAR(128), \n'
 "\tmetadata_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_audit_events PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_audit_events_dedup UNIQUE (event_type, subject_type, subject_id), \n'
 '\tCONSTRAINT ck_audit_events_subject_id_not_empty CHECK (subject_id IS NULL OR length(subject_id) > 0), \n'
 '\tCONSTRAINT fk_audit_events_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_audit_events_created_at ON audit_events (created_at)',
 'CREATE INDEX ix_audit_events_event_type ON audit_events (event_type)',
 'CREATE INDEX ix_audit_events_event_type_created_at ON audit_events (event_type, created_at)',
 'CREATE INDEX ix_audit_events_user_created_at ON audit_events (user_id, created_at)',
 'CREATE INDEX ix_audit_events_user_id ON audit_events (user_id)',
 'CREATE UNIQUE INDEX uq_audit_events_dedup_null_subject ON audit_events (event_type, subject_type)',
 'CREATE TABLE backtest_runs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tdate_from DATE NOT NULL, \n'
 '\tdate_to DATE NOT NULL, \n'
 '\ttarget_dte INTEGER NOT NULL, \n'
 '\tdte_tolerance_days INTEGER NOT NULL, \n'
 '\tmax_holding_days INTEGER NOT NULL, \n'
 '\taccount_size NUMERIC(18, 4) NOT NULL, \n'
 '\trisk_per_trade_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tcommission_per_contract NUMERIC(18, 4) NOT NULL, \n'
 "\tinput_snapshot_json JSON DEFAULT '{}' NOT NULL, \n"
 "\twarnings_json JSON DEFAULT '[]' NOT NULL, \n"
 "\tengine_version VARCHAR(32) DEFAULT 'options-multileg-v2' NOT NULL, \n"
 "\tdata_source VARCHAR(32) DEFAULT 'massive' NOT NULL, \n"
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_win_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_loss_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_holding_period_days NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_dte_at_open NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tprofit_factor NUMERIC(10, 4), \n'
 '\tpayoff_ratio NUMERIC(10, 4), \n'
 "\texpectancy NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tsharpe_ratio NUMERIC(10, 4), \n'
 '\tsortino_ratio NUMERIC(10, 4), \n'
 '\tcagr_pct NUMERIC(10, 4), \n'
 '\tcalmar_ratio NUMERIC(10, 4), \n'
 "\tmax_consecutive_wins INTEGER DEFAULT '0' NOT NULL, \n"
 "\tmax_consecutive_losses INTEGER DEFAULT '0' NOT NULL, \n"
 '\trecovery_factor NUMERIC(10, 4), \n'
 '\trisk_free_rate NUMERIC(6, 4), \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tCONSTRAINT pk_backtest_runs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_runs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_backtest_runs_valid_run_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_backtest_runs_account_positive CHECK (account_size > 0), \n'
 '\tCONSTRAINT ck_backtest_runs_risk_pct_range CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100), \n'
 '\tCONSTRAINT ck_backtest_runs_commission_nonneg CHECK (commission_per_contract >= 0), \n'
 '\tCONSTRAINT ck_backtest_runs_date_order CHECK (date_from < date_to), \n'
 '\tCONSTRAINT ck_backtest_runs_holding_days_positive CHECK (max_holding_days >= 1), \n'
 '\tCONSTRAINT ck_backtest_runs_target_dte_nonneg CHECK (target_dte >= 0), \n'
 '\tCONSTRAINT ck_backtest_runs_dte_tolerance_nonneg CHECK (dte_tolerance_days >= 0), \n'
 '\tCONSTRAINT ck_backtest_runs_holding_days_range CHECK (max_holding_days >= 1 AND max_holding_days <= 120), \n'
 '\tCONSTRAINT ck_backtest_runs_target_dte_range CHECK (target_dte >= 1 AND target_dte <= 365), \n'
 '\tCONSTRAINT ck_backtest_runs_dte_tolerance_range CHECK (dte_tolerance_days >= 0 AND dte_tolerance_days <= 60), \n'
 '\tCONSTRAINT ck_backtest_runs_account_size_max CHECK (account_size <= 100000000), \n'
 '\tCONSTRAINT ck_backtest_runs_commission_max CHECK (commission_per_contract <= 100), \n'
 "\tCONSTRAINT ck_backtest_runs_valid_engine_version CHECK (engine_version IN ('options-multileg-v1', "
 "'options-multileg-v2')), \n"
 "\tCONSTRAINT ck_backtest_runs_valid_data_source CHECK (data_source IN ('massive', 'manual', "
 "'historical_flatfile')), \n"
 '\tCONSTRAINT ck_backtest_runs_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_backtest_runs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_runs_celery_task_id ON backtest_runs (celery_task_id)',
 'CREATE INDEX ix_backtest_runs_dispatch_started_at ON backtest_runs (dispatch_started_at)',
 'CREATE INDEX ix_backtest_runs_queued ON backtest_runs (created_at)',
 'CREATE INDEX ix_backtest_runs_started_at ON backtest_runs (started_at)',
 'CREATE INDEX ix_backtest_runs_status_celery_created ON backtest_runs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_backtest_runs_user_created_at ON backtest_runs (user_id, created_at)',
 'CREATE INDEX ix_backtest_runs_user_id ON backtest_runs (user_id)',
 'CREATE INDEX ix_backtest_runs_user_status ON backtest_runs (user_id, status)',
 'CREATE INDEX ix_backtest_runs_user_symbol ON backtest_runs (user_id, symbol)',
 'CREATE TABLE backtest_templates (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 '\tname VARCHAR(120) NOT NULL, \n'
 '\tdescription VARCHAR(2000), \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 "\tconfig_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_backtest_templates PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_templates_user_name UNIQUE (user_id, name), \n'
 '\tCONSTRAINT ck_backtest_templates_name_not_empty CHECK (length(name) > 0), \n'
 '\tCONSTRAINT ck_backtest_templates_desc_length CHECK (description IS NULL OR length(description) <= 2000), \n'
 '\tCONSTRAINT fk_backtest_templates_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_templates_user_created_at ON backtest_templates (user_id, created_at)',
 'CREATE INDEX ix_backtest_templates_user_strategy ON backtest_templates (user_id, strategy_type)',
 'CREATE INDEX ix_backtest_templates_user_updated_at ON backtest_templates (user_id, updated_at)',
 'CREATE TABLE daily_recommendations (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tpipeline_run_id CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\trank INTEGER NOT NULL, \n'
 '\tscore NUMERIC(18, 6) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 "\tregime_labels JSON DEFAULT '[]' NOT NULL, \n"
 '\tclose_price NUMERIC(18, 4) NOT NULL, \n'
 '\ttarget_dte INTEGER NOT NULL, \n'
 '\tconfig_snapshot_json JSON, \n'
 '\tsummary_json JSON, \n'
 '\tforecast_json JSON, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_daily_recommendations PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_daily_recs_pipeline_rank UNIQUE (pipeline_run_id, rank), \n'
 '\tCONSTRAINT ck_daily_recommendations_rank_positive CHECK (rank >= 1), \n'
 '\tCONSTRAINT ck_daily_recommendations_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_daily_recommendations_pipeline_run_id_nightly_pipeline_runs FOREIGN KEY(pipeline_run_id) REFERENCES '
 'nightly_pipeline_runs (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_daily_recs_symbol_strategy ON daily_recommendations (symbol, strategy_type)',
 'CREATE INDEX ix_daily_recs_trade_date ON daily_recommendations (trade_date)',
 'CREATE TABLE multi_step_runs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tname VARCHAR(120), \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tworkflow_type VARCHAR(80) NOT NULL, \n'
 '\tstart_date DATE NOT NULL, \n'
 '\tend_date DATE NOT NULL, \n'
 '\taccount_size NUMERIC(18, 4) NOT NULL, \n'
 '\trisk_per_trade_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tcommission_per_contract NUMERIC(18, 4) NOT NULL, \n'
 "\tslippage_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tinput_snapshot_json JSON DEFAULT '{}' NOT NULL, \n"
 "\twarnings_json JSON DEFAULT '[]' NOT NULL, \n"
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_win_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_loss_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_holding_period_days NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_dte_at_open NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tprofit_factor NUMERIC(10, 4), \n'
 '\tpayoff_ratio NUMERIC(10, 4), \n'
 "\texpectancy NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tsharpe_ratio NUMERIC(10, 4), \n'
 '\tsortino_ratio NUMERIC(10, 4), \n'
 '\tcagr_pct NUMERIC(10, 4), \n'
 '\tcalmar_ratio NUMERIC(10, 4), \n'
 "\tmax_consecutive_wins INTEGER DEFAULT '0' NOT NULL, \n"
 "\tmax_consecutive_losses INTEGER DEFAULT '0' NOT NULL, \n"
 '\trecovery_factor NUMERIC(10, 4), \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tCONSTRAINT pk_multi_step_runs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_step_runs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_multi_step_runs_valid_run_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_multi_step_runs_date_order CHECK (start_date < end_date), \n'
 '\tCONSTRAINT ck_multi_step_runs_account_positive CHECK (account_size > 0), \n'
 '\tCONSTRAINT ck_multi_step_runs_risk_pct_range CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= 100), \n'
 '\tCONSTRAINT ck_multi_step_runs_commission_nonneg CHECK (commission_per_contract >= 0), \n'
 '\tCONSTRAINT ck_multi_step_runs_slippage_range CHECK (slippage_pct >= 0 AND slippage_pct <= 5), \n'
 '\tCONSTRAINT fk_multi_step_runs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_runs_celery_task_id ON multi_step_runs (celery_task_id)',
 'CREATE INDEX ix_multi_step_runs_dispatch_started_at ON multi_step_runs (dispatch_started_at)',
 'CREATE INDEX ix_multi_step_runs_queued ON multi_step_runs (created_at)',
 'CREATE INDEX ix_multi_step_runs_status ON multi_step_runs (status)',
 'CREATE INDEX ix_multi_step_runs_status_celery_created ON multi_step_runs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_multi_step_runs_user_created_at ON multi_step_runs (user_id, created_at)',
 'CREATE INDEX ix_multi_step_runs_user_id ON multi_step_runs (user_id)',
 'CREATE TABLE multi_symbol_runs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tname VARCHAR(120), \n'
 '\tstart_date DATE NOT NULL, \n'
 '\tend_date DATE NOT NULL, \n'
 '\taccount_size NUMERIC(18, 4) NOT NULL, \n'
 "\tcapital_allocation_mode VARCHAR(24) DEFAULT 'equal_weight' NOT NULL, \n"
 '\tcommission_per_contract NUMERIC(18, 4) NOT NULL, \n'
 "\tslippage_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tinput_snapshot_json JSON DEFAULT '{}' NOT NULL, \n"
 "\twarnings_json JSON DEFAULT '[]' NOT NULL, \n"
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_win_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_loss_amount NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_holding_period_days NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\taverage_dte_at_open NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tprofit_factor NUMERIC(10, 4), \n'
 '\tpayoff_ratio NUMERIC(10, 4), \n'
 "\texpectancy NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tsharpe_ratio NUMERIC(10, 4), \n'
 '\tsortino_ratio NUMERIC(10, 4), \n'
 '\tcagr_pct NUMERIC(10, 4), \n'
 '\tcalmar_ratio NUMERIC(10, 4), \n'
 "\tmax_consecutive_wins INTEGER DEFAULT '0' NOT NULL, \n"
 "\tmax_consecutive_losses INTEGER DEFAULT '0' NOT NULL, \n"
 '\trecovery_factor NUMERIC(10, 4), \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tCONSTRAINT pk_multi_symbol_runs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_runs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_multi_symbol_runs_valid_run_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_multi_symbol_runs_date_order CHECK (start_date < end_date), \n'
 '\tCONSTRAINT ck_multi_symbol_runs_account_positive CHECK (account_size > 0), \n'
 '\tCONSTRAINT ck_multi_symbol_runs_commission_nonneg CHECK (commission_per_contract >= 0), \n'
 '\tCONSTRAINT ck_multi_symbol_runs_slippage_range CHECK (slippage_pct >= 0 AND slippage_pct <= 5), \n'
 '\tCONSTRAINT fk_multi_symbol_runs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_runs_celery_task_id ON multi_symbol_runs (celery_task_id)',
 'CREATE INDEX ix_multi_symbol_runs_dispatch_started_at ON multi_symbol_runs (dispatch_started_at)',
 'CREATE INDEX ix_multi_symbol_runs_queued ON multi_symbol_runs (created_at)',
 'CREATE INDEX ix_multi_symbol_runs_status ON multi_symbol_runs (status)',
 'CREATE INDEX ix_multi_symbol_runs_status_celery_created ON multi_symbol_runs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_multi_symbol_runs_user_created_at ON multi_symbol_runs (user_id, created_at)',
 'CREATE INDEX ix_multi_symbol_runs_user_id ON multi_symbol_runs (user_id)',
 'CREATE TABLE scanner_jobs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 '\tparent_job_id CHAR(36), \n'
 '\tpipeline_run_id CHAR(36), \n'
 '\tname VARCHAR(120), \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tmode VARCHAR(16) NOT NULL, \n'
 '\tplan_tier_snapshot VARCHAR(16) NOT NULL, \n'
 "\tjob_kind VARCHAR(32) DEFAULT 'manual' NOT NULL, \n"
 '\trequest_hash VARCHAR(64) NOT NULL, \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\trefresh_key VARCHAR(120), \n'
 '\trefresh_daily BOOLEAN DEFAULT false NOT NULL, \n'
 "\trefresh_priority INTEGER DEFAULT '0' NOT NULL, \n"
 "\tcandidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\tevaluated_candidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\trecommendation_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\trequest_snapshot_json JSON DEFAULT '{}' NOT NULL, \n"
 "\twarnings_json JSON DEFAULT '[]' NOT NULL, \n"
 "\tranking_version VARCHAR(32) DEFAULT 'scanner-ranking-v1' NOT NULL, \n"
 "\tengine_version VARCHAR(32) DEFAULT 'options-multileg-v2' NOT NULL, \n"
 '\tcelery_task_id VARCHAR(64), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tCONSTRAINT pk_scanner_jobs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_scanner_jobs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 '\tCONSTRAINT uq_scanner_jobs_refresh_key UNIQUE (refresh_key), \n'
 "\tCONSTRAINT ck_scanner_jobs_valid_job_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_plan_tier CHECK (plan_tier_snapshot IN ('free', 'pro', 'premium')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_mode CHECK (mode IN ('basic', 'advanced')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_job_kind CHECK (job_kind IN ('manual', 'refresh', 'nightly')), \n"
 '\tCONSTRAINT ck_scanner_jobs_refresh_priority_range CHECK (refresh_priority >= 0 AND refresh_priority <= 100), \n'
 '\tCONSTRAINT ck_scanner_jobs_candidate_count_nonneg CHECK (candidate_count >= 0), \n'
 '\tCONSTRAINT ck_scanner_jobs_evaluated_count_nonneg CHECK (evaluated_candidate_count >= 0), \n'
 '\tCONSTRAINT ck_scanner_jobs_recommendation_count_nonneg CHECK (recommendation_count >= 0), \n'
 "\tCONSTRAINT ck_scanner_jobs_valid_engine_version CHECK (engine_version IN ('options-multileg-v1', "
 "'options-multileg-v2')), \n"
 "\tCONSTRAINT ck_scanner_jobs_valid_ranking_version CHECK (ranking_version IN ('scanner-ranking-v1', "
 "'scanner-ranking-v2')), \n"
 '\tCONSTRAINT ck_scanner_jobs_name_not_empty CHECK (name IS NULL OR length(name) > 0), \n'
 '\tCONSTRAINT fk_scanner_jobs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_scanner_jobs_parent_job_id_scanner_jobs FOREIGN KEY(parent_job_id) REFERENCES scanner_jobs (id) ON '
 'DELETE SET NULL, \n'
 '\tCONSTRAINT fk_scanner_jobs_pipeline_run_id_nightly_pipeline_runs FOREIGN KEY(pipeline_run_id) REFERENCES '
 'nightly_pipeline_runs (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_scanner_jobs_celery_task_id ON scanner_jobs (celery_task_id)',
 'CREATE INDEX ix_scanner_jobs_dedup_lookup ON scanner_jobs (user_id, request_hash, mode, created_at)',
 'CREATE INDEX ix_scanner_jobs_dispatch_started_at ON scanner_jobs (dispatch_started_at)',
 'CREATE INDEX ix_scanner_jobs_parent_job_id ON scanner_jobs (parent_job_id)',
 'CREATE INDEX ix_scanner_jobs_pipeline_run_id ON scanner_jobs (pipeline_run_id)',
 'CREATE INDEX ix_scanner_jobs_queued ON scanner_jobs (created_at)',
 'CREATE INDEX ix_scanner_jobs_refresh_sources ON scanner_jobs (refresh_daily, status)',
 'CREATE INDEX ix_scanner_jobs_request_hash ON scanner_jobs (request_hash)',
 'CREATE INDEX ix_scanner_jobs_status_celery_created ON scanner_jobs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_scanner_jobs_user_created_at ON scanner_jobs (user_id, created_at)',
 'CREATE INDEX ix_scanner_jobs_user_id ON scanner_jobs (user_id)',
 'CREATE INDEX ix_scanner_jobs_user_status ON scanner_jobs (user_id, status)',
 'CREATE UNIQUE INDEX uq_scanner_jobs_active_dedup ON scanner_jobs (user_id, request_hash, mode)',
 'CREATE TABLE stripe_events (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tstripe_event_id VARCHAR(255) NOT NULL, \n'
 '\tevent_type VARCHAR(128) NOT NULL, \n'
 '\tlivemode BOOLEAN DEFAULT false NOT NULL, \n'
 "\tidempotency_status VARCHAR(16) DEFAULT 'processing' NOT NULL, \n"
 '\tuser_id CHAR(36), \n'
 '\trequest_id VARCHAR(64), \n'
 '\tip_hash VARCHAR(128), \n'
 '\terror_detail TEXT, \n'
 "\tpayload_summary JSON DEFAULT '{}' NOT NULL, \n"
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_stripe_events PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_stripe_events_event_id UNIQUE (stripe_event_id), \n'
 "\tCONSTRAINT ck_stripe_events_valid_status CHECK (idempotency_status IN ('processing', 'processed', 'ignored', "
 "'error')), \n"
 '\tCONSTRAINT fk_stripe_events_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_stripe_events_created_at ON stripe_events (created_at)',
 'CREATE INDEX ix_stripe_events_event_id_status ON stripe_events (stripe_event_id, idempotency_status)',
 'CREATE INDEX ix_stripe_events_event_type ON stripe_events (event_type)',
 'CREATE INDEX ix_stripe_events_idempotency_status ON stripe_events (idempotency_status)',
 'CREATE INDEX ix_stripe_events_user_id ON stripe_events (user_id)',
 'CREATE TABLE sweep_jobs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 "\tmode VARCHAR(16) DEFAULT 'grid' NOT NULL, \n"
 "\tplan_tier_snapshot VARCHAR(16) DEFAULT 'free' NOT NULL, \n"
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 "\tcandidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\tevaluated_candidate_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\tresult_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\trequest_snapshot_json JSON DEFAULT '{}' NOT NULL, \n"
 '\trequest_hash VARCHAR(64), \n'
 "\twarnings_json JSON DEFAULT '[]' NOT NULL, \n"
 '\tprefetch_summary_json JSON, \n'
 "\tengine_version VARCHAR(32) DEFAULT 'options-multileg-v2' NOT NULL, \n"
 '\tcelery_task_id VARCHAR(64), \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tCONSTRAINT pk_sweep_jobs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_sweep_jobs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_sweep_jobs_valid_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_sweep_jobs_candidate_count_nonneg CHECK (candidate_count >= 0), \n'
 '\tCONSTRAINT ck_sweep_jobs_evaluated_count_nonneg CHECK (evaluated_candidate_count >= 0), \n'
 '\tCONSTRAINT ck_sweep_jobs_result_count_nonneg CHECK (result_count >= 0), \n'
 "\tCONSTRAINT ck_sweep_jobs_valid_plan_tier CHECK (plan_tier_snapshot IN ('free', 'pro', 'premium')), \n"
 "\tCONSTRAINT ck_sweep_jobs_valid_engine_version CHECK (engine_version IN ('options-multileg-v1', "
 "'options-multileg-v2')), \n"
 "\tCONSTRAINT ck_sweep_jobs_valid_mode CHECK (mode IN ('grid', 'genetic')), \n"
 '\tCONSTRAINT ck_sweep_jobs_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_sweep_jobs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_sweep_jobs_active_dedup_lookup ON sweep_jobs (user_id, symbol, request_hash, created_at)',
 'CREATE INDEX ix_sweep_jobs_celery_task_id ON sweep_jobs (celery_task_id)',
 'CREATE INDEX ix_sweep_jobs_dispatch_started_at ON sweep_jobs (dispatch_started_at)',
 'CREATE INDEX ix_sweep_jobs_queued ON sweep_jobs (created_at)',
 'CREATE INDEX ix_sweep_jobs_request_hash ON sweep_jobs (request_hash)',
 'CREATE INDEX ix_sweep_jobs_status_celery_created ON sweep_jobs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_sweep_jobs_user_created_at ON sweep_jobs (user_id, created_at)',
 'CREATE INDEX ix_sweep_jobs_user_id ON sweep_jobs (user_id)',
 'CREATE INDEX ix_sweep_jobs_user_status ON sweep_jobs (user_id, status)',
 'CREATE INDEX ix_sweep_jobs_user_symbol ON sweep_jobs (user_id, symbol)',
 'CREATE INDEX ix_sweep_jobs_user_symbol_created ON sweep_jobs (user_id, symbol, created_at)',
 'CREATE TABLE symbol_analyses (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 "\tstage VARCHAR(32) DEFAULT 'pending' NOT NULL, \n"
 '\tclose_price NUMERIC(18, 4), \n'
 '\tregime_json JSON, \n'
 '\tlandscape_json JSON, \n'
 '\ttop_results_json JSON, \n'
 '\tforecast_json JSON, \n'
 "\tstrategies_tested INTEGER DEFAULT '0' NOT NULL, \n"
 "\tconfigs_tested INTEGER DEFAULT '0' NOT NULL, \n"
 "\ttop_results_count INTEGER DEFAULT '0' NOT NULL, \n"
 '\tduration_seconds NUMERIC(10, 2), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\tCONSTRAINT pk_symbol_analyses PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_symbol_analyses_user_idempotency UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_symbol_analyses_valid_analysis_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled')), \n"
 '\tCONSTRAINT ck_symbol_analyses_strategies_tested_nonneg CHECK (strategies_tested >= 0), \n'
 '\tCONSTRAINT ck_symbol_analyses_configs_tested_nonneg CHECK (configs_tested >= 0), \n'
 '\tCONSTRAINT ck_symbol_analyses_top_results_nonneg CHECK (top_results_count >= 0), \n'
 "\tCONSTRAINT ck_symbol_analyses_valid_stage CHECK (stage IN ('pending', 'regime', 'landscape', 'deep_dive', "
 "'forecast')), \n"
 '\tCONSTRAINT ck_symbol_analyses_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_symbol_analyses_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_symbol_analyses_celery_task_id ON symbol_analyses (celery_task_id)',
 'CREATE INDEX ix_symbol_analyses_dispatch_started_at ON symbol_analyses (dispatch_started_at)',
 'CREATE INDEX ix_symbol_analyses_queued ON symbol_analyses (created_at)',
 'CREATE INDEX ix_symbol_analyses_status_celery_created ON symbol_analyses (status, celery_task_id, created_at)',
 'CREATE INDEX ix_symbol_analyses_status_created ON symbol_analyses (status, created_at)',
 'CREATE INDEX ix_symbol_analyses_symbol ON symbol_analyses (symbol)',
 'CREATE INDEX ix_symbol_analyses_user_created ON symbol_analyses (user_id, created_at)',
 'CREATE INDEX ix_symbol_analyses_user_id ON symbol_analyses (user_id)',
 'CREATE TABLE backtest_equity_points (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_backtest_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_equity_points_run_date UNIQUE (run_id, trade_date), \n'
 '\tCONSTRAINT fk_backtest_equity_points_run_id_backtest_runs FOREIGN KEY(run_id) REFERENCES backtest_runs (id) ON '
 'DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_equity_points_run_id ON backtest_equity_points (run_id)',
 'CREATE INDEX ix_backtest_equity_points_trade_date ON backtest_equity_points (trade_date)',
 'CREATE TABLE backtest_trades (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tunderlying_symbol VARCHAR(32) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE NOT NULL, \n'
 '\texpiration_date DATE NOT NULL, \n'
 '\tquantity INTEGER NOT NULL, \n'
 '\tdte_at_open INTEGER NOT NULL, \n'
 '\tholding_period_days INTEGER NOT NULL, \n'
 '\tholding_period_trading_days INTEGER, \n'
 '\tentry_underlying_close NUMERIC(18, 4) NOT NULL, \n'
 '\texit_underlying_close NUMERIC(18, 4) NOT NULL, \n'
 '\tentry_mid NUMERIC(18, 4) NOT NULL, \n'
 '\texit_mid NUMERIC(18, 4) NOT NULL, \n'
 '\tgross_pnl NUMERIC(18, 4) NOT NULL, \n'
 '\tnet_pnl NUMERIC(18, 4) NOT NULL, \n'
 '\ttotal_commissions NUMERIC(18, 4) NOT NULL, \n'
 '\tentry_reason VARCHAR(128) NOT NULL, \n'
 '\texit_reason VARCHAR(128) NOT NULL, \n'
 "\tdetail_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tCONSTRAINT pk_backtest_trades PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_backtest_trades_dedup UNIQUE (run_id, entry_date, option_ticker), \n'
 '\tCONSTRAINT ck_backtest_trades_quantity_positive CHECK (quantity > 0), \n'
 '\tCONSTRAINT ck_backtest_trades_date_order CHECK (entry_date <= exit_date), \n'
 '\tCONSTRAINT ck_backtest_trades_dte_at_open_nonneg CHECK (dte_at_open >= 0), \n'
 '\tCONSTRAINT ck_backtest_trades_holding_period_nonneg CHECK (holding_period_days >= 0), \n'
 '\tCONSTRAINT ck_backtest_trades_holding_trading_days_nonneg CHECK (holding_period_trading_days IS NULL OR '
 'holding_period_trading_days >= 0), \n'
 '\tCONSTRAINT fk_backtest_trades_run_id_backtest_runs FOREIGN KEY(run_id) REFERENCES backtest_runs (id) ON DELETE '
 'CASCADE\n'
 ')',
 'CREATE INDEX ix_backtest_trades_run_entry_date ON backtest_trades (run_id, entry_date)',
 'CREATE INDEX ix_backtest_trades_run_id ON backtest_trades (run_id)',
 'CREATE TABLE export_jobs (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tuser_id CHAR(36) NOT NULL, \n'
 '\tbacktest_run_id CHAR(36), \n'
 '\tmulti_symbol_run_id CHAR(36), \n'
 '\tmulti_step_run_id CHAR(36), \n'
 "\texport_target_kind VARCHAR(24) DEFAULT 'backtest' NOT NULL, \n"
 '\texport_format VARCHAR(16) NOT NULL, \n'
 "\tstatus VARCHAR(32) DEFAULT 'queued' NOT NULL, \n"
 '\tfile_name VARCHAR(255) NOT NULL, \n'
 '\tmime_type VARCHAR(128) NOT NULL, \n'
 "\tsize_bytes BIGINT DEFAULT '0' NOT NULL, \n"
 '\tsha256_hex VARCHAR(64), \n'
 '\tidempotency_key VARCHAR(80), \n'
 '\tcelery_task_id VARCHAR(64), \n'
 '\tcontent_bytes BLOB, \n'
 '\tstorage_key VARCHAR(512), \n'
 '\terror_code VARCHAR(64), \n'
 '\terror_message TEXT, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tlast_heartbeat_at DATETIME, \n'
 '\tdispatch_started_at DATETIME, \n'
 '\tstarted_at DATETIME, \n'
 '\tcompleted_at DATETIME, \n'
 '\texpires_at DATETIME, \n'
 '\tCONSTRAINT pk_export_jobs PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_export_jobs_user_idempotency_key UNIQUE (user_id, idempotency_key), \n'
 "\tCONSTRAINT ck_export_jobs_valid_export_status CHECK (status IN ('queued', 'running', 'succeeded', 'failed', "
 "'cancelled', 'expired')), \n"
 "\tCONSTRAINT ck_export_jobs_valid_target_kind CHECK (export_target_kind IN ('backtest', 'multi_symbol', "
 "'multi_step')), \n"
 '\tCONSTRAINT ck_export_jobs_exactly_one_target CHECK (((CASE WHEN backtest_run_id IS NOT NULL THEN 1 ELSE 0 END) + '
 '(CASE WHEN multi_symbol_run_id IS NOT NULL THEN 1 ELSE 0 END) + (CASE WHEN multi_step_run_id IS NOT NULL THEN 1 ELSE '
 '0 END)) = 1), \n'
 "\tCONSTRAINT ck_export_jobs_succeeded_has_storage CHECK (status != 'succeeded' OR content_bytes IS NOT NULL OR "
 'storage_key IS NOT NULL), \n'
 '\tCONSTRAINT ck_export_jobs_size_bytes_nonneg CHECK (size_bytes >= 0), \n'
 "\tCONSTRAINT ck_export_jobs_valid_export_format CHECK (export_format IN ('csv', 'pdf')), \n"
 '\tCONSTRAINT fk_export_jobs_user_id_users FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_export_jobs_backtest_run_id_backtest_runs FOREIGN KEY(backtest_run_id) REFERENCES backtest_runs (id) '
 'ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_export_jobs_multi_symbol_run_id_multi_symbol_runs FOREIGN KEY(multi_symbol_run_id) REFERENCES '
 'multi_symbol_runs (id) ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_export_jobs_multi_step_run_id_multi_step_runs FOREIGN KEY(multi_step_run_id) REFERENCES '
 'multi_step_runs (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_export_jobs_backtest_run_id ON export_jobs (backtest_run_id)',
 'CREATE INDEX ix_export_jobs_celery_task_id ON export_jobs (celery_task_id)',
 'CREATE INDEX ix_export_jobs_dispatch_started_at ON export_jobs (dispatch_started_at)',
 'CREATE INDEX ix_export_jobs_multi_step_run_id ON export_jobs (multi_step_run_id)',
 'CREATE INDEX ix_export_jobs_multi_symbol_run_id ON export_jobs (multi_symbol_run_id)',
 'CREATE INDEX ix_export_jobs_queued ON export_jobs (created_at)',
 'CREATE INDEX ix_export_jobs_sha256_hex ON export_jobs (sha256_hex)',
 'CREATE INDEX ix_export_jobs_status_celery_created ON export_jobs (status, celery_task_id, created_at)',
 'CREATE INDEX ix_export_jobs_status_expires_at ON export_jobs (status, expires_at)',
 'CREATE INDEX ix_export_jobs_storage_key ON export_jobs (storage_key)',
 'CREATE INDEX ix_export_jobs_user_created_at ON export_jobs (user_id, created_at)',
 'CREATE INDEX ix_export_jobs_user_id ON export_jobs (user_id)',
 'CREATE INDEX ix_export_jobs_user_status ON export_jobs (user_id, status)',
 'CREATE TABLE multi_step_equity_points (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_multi_step_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_step_equity_points_run_date UNIQUE (run_id, trade_date), \n'
 '\tCONSTRAINT fk_multi_step_equity_points_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) '
 'ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_equity_points_run_id ON multi_step_equity_points (run_id)',
 'CREATE TABLE multi_step_run_steps (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\tstep_number INTEGER NOT NULL, \n'
 '\tname VARCHAR(120) NOT NULL, \n'
 '\taction VARCHAR(32) NOT NULL, \n'
 "\ttrigger_json JSON DEFAULT '{}' NOT NULL, \n"
 "\tcontract_selection_json JSON DEFAULT '{}' NOT NULL, \n"
 "\tfailure_policy VARCHAR(32) DEFAULT 'liquidate' NOT NULL, \n"
 "\tstatus VARCHAR(24) DEFAULT 'pending' NOT NULL, \n"
 '\ttriggered_at DATETIME, \n'
 '\texecuted_at DATETIME, \n'
 '\tfailure_reason TEXT, \n'
 '\tCONSTRAINT pk_multi_step_run_steps PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_step_run_steps_run_step_number UNIQUE (run_id, step_number), \n'
 '\tCONSTRAINT fk_multi_step_run_steps_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) ON '
 'DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_run_steps_run_id ON multi_step_run_steps (run_id)',
 'CREATE TABLE multi_step_trades (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\tstep_number INTEGER NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE NOT NULL, \n'
 '\texpiration_date DATE, \n'
 '\tquantity INTEGER NOT NULL, \n'
 '\tdte_at_open INTEGER, \n'
 '\tholding_period_days INTEGER, \n'
 '\tentry_underlying_close NUMERIC(18, 4), \n'
 '\texit_underlying_close NUMERIC(18, 4), \n'
 '\tentry_mid NUMERIC(18, 4), \n'
 '\texit_mid NUMERIC(18, 4), \n'
 "\tgross_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tnet_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tentry_reason VARCHAR(128) NOT NULL, \n'
 '\texit_reason VARCHAR(128) NOT NULL, \n'
 "\tdetail_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tCONSTRAINT pk_multi_step_trades PRIMARY KEY (id), \n'
 '\tCONSTRAINT ck_multi_step_trades_quantity_positive CHECK (quantity > 0), \n'
 '\tCONSTRAINT ck_multi_step_trades_date_order CHECK (entry_date <= exit_date), \n'
 '\tCONSTRAINT fk_multi_step_trades_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) ON '
 'DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_step_trades_run_entry_date ON multi_step_trades (run_id, entry_date)',
 'CREATE INDEX ix_multi_step_trades_run_id ON multi_step_trades (run_id)',
 'CREATE TABLE multi_symbol_equity_points (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_multi_symbol_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_equity_points_run_date UNIQUE (run_id, trade_date), \n'
 '\tCONSTRAINT fk_multi_symbol_equity_points_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs '
 '(id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_equity_points_run_id ON multi_symbol_equity_points (run_id)',
 'CREATE TABLE multi_symbol_run_symbols (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\trisk_per_trade_pct NUMERIC(10, 4) NOT NULL, \n'
 "\tmax_open_positions INTEGER DEFAULT '1' NOT NULL, \n"
 '\tcapital_allocation_pct NUMERIC(10, 4), \n'
 "\ttrade_count INTEGER DEFAULT '0' NOT NULL, \n"
 "\twin_rate NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_roi_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\tmax_drawdown_pct NUMERIC(10, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_net_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tstarting_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tending_equity NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tCONSTRAINT pk_multi_symbol_run_symbols PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_run_symbols_run_symbol UNIQUE (run_id, symbol), \n'
 '\tCONSTRAINT ck_multi_symbol_run_symbols_risk_pct_range CHECK (risk_per_trade_pct > 0 AND risk_per_trade_pct <= '
 '100), \n'
 '\tCONSTRAINT ck_multi_symbol_run_symbols_max_open_positions_positive CHECK (max_open_positions >= 1), \n'
 '\tCONSTRAINT fk_multi_symbol_run_symbols_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs '
 '(id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_run_symbols_run_id ON multi_symbol_run_symbols (run_id)',
 'CREATE TABLE multi_symbol_trade_groups (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE, \n'
 "\tstatus VARCHAR(16) DEFAULT 'open' NOT NULL, \n"
 "\tdetail_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tCONSTRAINT pk_multi_symbol_trade_groups PRIMARY KEY (id), \n'
 "\tCONSTRAINT ck_multi_symbol_trade_groups_status CHECK (status IN ('open', 'closed', 'cancelled')), \n"
 '\tCONSTRAINT fk_multi_symbol_trade_groups_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs '
 '(id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_trade_groups_run_entry_date ON multi_symbol_trade_groups (run_id, entry_date)',
 'CREATE INDEX ix_multi_symbol_trade_groups_run_id ON multi_symbol_trade_groups (run_id)',
 'CREATE TABLE scanner_recommendations (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tscanner_job_id CHAR(36) NOT NULL, \n'
 '\trank INTEGER NOT NULL, \n'
 '\tscore NUMERIC(18, 6) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\trule_set_name VARCHAR(120) NOT NULL, \n'
 '\trule_set_hash VARCHAR(64) NOT NULL, \n'
 '\trequest_snapshot_json JSON NOT NULL, \n'
 '\tsummary_json JSON NOT NULL, \n'
 '\twarnings_json JSON NOT NULL, \n'
 '\ttrades_json JSON NOT NULL, \n'
 '\tequity_curve_json JSON NOT NULL, \n'
 '\thistorical_performance_json JSON NOT NULL, \n'
 '\tforecast_json JSON NOT NULL, \n'
 '\tranking_features_json JSON NOT NULL, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_scanner_recommendations PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_scanner_recommendations_job_rank UNIQUE (scanner_job_id, rank), \n'
 '\tCONSTRAINT ck_scanner_recommendations_rank_positive CHECK (rank >= 1), \n'
 '\tCONSTRAINT ck_scanner_recommendations_symbol_not_empty CHECK (length(symbol) > 0), \n'
 '\tCONSTRAINT fk_scanner_recommendations_scanner_job_id_scanner_jobs FOREIGN KEY(scanner_job_id) REFERENCES '
 'scanner_jobs (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_scanner_recommendations_lookup ON scanner_recommendations (symbol, strategy_type, rule_set_hash)',
 'CREATE INDEX ix_scanner_recommendations_summary_gin ON scanner_recommendations (summary_json)',
 'CREATE TABLE sweep_results (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\tsweep_job_id CHAR(36) NOT NULL, \n'
 '\trank INTEGER NOT NULL, \n'
 '\tscore NUMERIC(18, 6) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tparameter_snapshot_json JSON NOT NULL, \n'
 '\tsummary_json JSON NOT NULL, \n'
 '\twarnings_json JSON NOT NULL, \n'
 '\ttrades_json JSON NOT NULL, \n'
 '\tequity_curve_json JSON NOT NULL, \n'
 '\tcreated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tupdated_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tCONSTRAINT pk_sweep_results PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_sweep_results_job_rank UNIQUE (sweep_job_id, rank), \n'
 '\tCONSTRAINT ck_sweep_results_rank_positive CHECK (rank >= 1), \n'
 '\tCONSTRAINT fk_sweep_results_sweep_job_id_sweep_jobs FOREIGN KEY(sweep_job_id) REFERENCES sweep_jobs (id) ON DELETE '
 'CASCADE\n'
 ')',
 'CREATE INDEX ix_sweep_results_job_id ON sweep_results (sweep_job_id)',
 'CREATE INDEX ix_sweep_results_summary_gin ON sweep_results (summary_json)',
 'CREATE TABLE multi_step_step_events (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\tstep_id CHAR(36), \n'
 '\tstep_number INTEGER NOT NULL, \n'
 '\tevent_type VARCHAR(24) NOT NULL, \n'
 '\tevent_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL, \n'
 '\tmessage TEXT, \n'
 "\tpayload_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tCONSTRAINT pk_multi_step_step_events PRIMARY KEY (id), \n'
 '\tCONSTRAINT fk_multi_step_step_events_run_id_multi_step_runs FOREIGN KEY(run_id) REFERENCES multi_step_runs (id) ON '
 'DELETE CASCADE, \n'
 '\tCONSTRAINT fk_multi_step_step_events_step_id_multi_step_run_steps FOREIGN KEY(step_id) REFERENCES '
 'multi_step_run_steps (id) ON DELETE SET NULL\n'
 ')',
 'CREATE INDEX ix_multi_step_step_events_run_event_at ON multi_step_step_events (run_id, event_at)',
 'CREATE INDEX ix_multi_step_step_events_run_id ON multi_step_step_events (run_id)',
 'CREATE INDEX ix_multi_step_step_events_step_number ON multi_step_step_events (step_number)',
 'CREATE TABLE multi_symbol_symbol_equity_points (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_symbol_id CHAR(36) NOT NULL, \n'
 '\ttrade_date DATE NOT NULL, \n'
 '\tequity NUMERIC(18, 4) NOT NULL, \n'
 '\tcash NUMERIC(18, 4) NOT NULL, \n'
 '\tposition_value NUMERIC(18, 4) NOT NULL, \n'
 '\tdrawdown_pct NUMERIC(10, 4) NOT NULL, \n'
 '\tCONSTRAINT pk_multi_symbol_symbol_equity_points PRIMARY KEY (id), \n'
 '\tCONSTRAINT uq_multi_symbol_symbol_equity_points_symbol_date UNIQUE (run_symbol_id, trade_date), \n'
 '\tCONSTRAINT fk_multi_symbol_symbol_equity_points_run_symbol_id_multi_symbol_run_symbols FOREIGN KEY(run_symbol_id) '
 'REFERENCES multi_symbol_run_symbols (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_symbol_equity_points_run_symbol_id ON multi_symbol_symbol_equity_points (run_symbol_id)',
 'CREATE TABLE multi_symbol_trades (\n'
 '\tid CHAR(36) NOT NULL, \n'
 '\trun_id CHAR(36) NOT NULL, \n'
 '\ttrade_group_id CHAR(36) NOT NULL, \n'
 '\tsymbol VARCHAR(32) NOT NULL, \n'
 '\toption_ticker VARCHAR(64) NOT NULL, \n'
 '\tstrategy_type VARCHAR(48) NOT NULL, \n'
 '\tentry_date DATE NOT NULL, \n'
 '\texit_date DATE NOT NULL, \n'
 '\texpiration_date DATE, \n'
 '\tquantity INTEGER NOT NULL, \n'
 '\tdte_at_open INTEGER, \n'
 '\tholding_period_days INTEGER, \n'
 '\tentry_underlying_close NUMERIC(18, 4), \n'
 '\texit_underlying_close NUMERIC(18, 4), \n'
 '\tentry_mid NUMERIC(18, 4), \n'
 '\texit_mid NUMERIC(18, 4), \n'
 "\tgross_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\tnet_pnl NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 "\ttotal_commissions NUMERIC(18, 4) DEFAULT '0' NOT NULL, \n"
 '\tentry_reason VARCHAR(128) NOT NULL, \n'
 '\texit_reason VARCHAR(128) NOT NULL, \n'
 "\tdetail_json JSON DEFAULT '{}' NOT NULL, \n"
 '\tCONSTRAINT pk_multi_symbol_trades PRIMARY KEY (id), \n'
 '\tCONSTRAINT ck_multi_symbol_trades_quantity_positive CHECK (quantity > 0), \n'
 '\tCONSTRAINT ck_multi_symbol_trades_date_order CHECK (entry_date <= exit_date), \n'
 '\tCONSTRAINT fk_multi_symbol_trades_run_id_multi_symbol_runs FOREIGN KEY(run_id) REFERENCES multi_symbol_runs (id) '
 'ON DELETE CASCADE, \n'
 '\tCONSTRAINT fk_multi_symbol_trades_trade_group_id_multi_symbol_trade_groups FOREIGN KEY(trade_group_id) REFERENCES '
 'multi_symbol_trade_groups (id) ON DELETE CASCADE\n'
 ')',
 'CREATE INDEX ix_multi_symbol_trades_run_entry_date ON multi_symbol_trades (run_id, entry_date)',
 'CREATE INDEX ix_multi_symbol_trades_run_id ON multi_symbol_trades (run_id)',
 'CREATE INDEX ix_multi_symbol_trades_trade_group_id ON multi_symbol_trades (trade_group_id)']
