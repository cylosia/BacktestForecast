import type { BacktestSummaryResponse, EquityCurvePointResponse } from "@backtestforecast/api-client";

export interface MultiSymbolDefinition {
  symbol: string;
  risk_per_trade_pct: number | string;
  max_open_positions: number;
  capital_allocation_pct?: number | string | null;
}

export interface MultiSymbolLegDefinition {
  symbol: string;
  strategy_type: string;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
  quantity_mode: "risk_based" | "fixed_contracts";
  fixed_contracts?: number | null;
}

export interface MultiSymbolStrategyGroup {
  name: string;
  synchronous_entry: true;
  legs: MultiSymbolLegDefinition[];
}

export interface CrossSymbolRule {
  left_symbol: string;
  left_indicator: string;
  operator: string;
  right_symbol?: string | null;
  right_indicator?: string | null;
  threshold?: number | string | null;
  lookback_period?: number | null;
}

export interface CreateMultiSymbolRunRequest {
  name?: string | null;
  symbols: MultiSymbolDefinition[];
  strategy_groups: MultiSymbolStrategyGroup[];
  entry_rules: CrossSymbolRule[];
  exit_rules?: CrossSymbolRule[];
  start_date: string;
  end_date: string;
  account_size: number | string;
  capital_allocation_mode: "equal_weight" | "explicit";
  commission_per_contract: number | string;
  slippage_pct?: number | string;
  idempotency_key?: string | null;
}

export interface MultiSymbolRunSymbolSummaryResponse {
  symbol: string;
  summary: BacktestSummaryResponse;
}

export interface MultiSymbolTradeResponse {
  id: string;
  trade_group_id: string;
  symbol: string;
  option_ticker: string;
  strategy_type: string;
  entry_date: string;
  exit_date: string;
  quantity: number;
  gross_pnl: number | string;
  net_pnl: number | string;
  total_commissions: number | string;
  entry_reason: string;
  exit_reason: string;
  detail_json: Record<string, unknown>;
}

export interface MultiSymbolTradeGroupResponse {
  id: string;
  entry_date: string;
  exit_date?: string | null;
  status: "open" | "closed" | "cancelled";
  trades: MultiSymbolTradeResponse[];
}

export interface MultiSymbolRunDetailResponse {
  id: string;
  name?: string | null;
  status: string;
  start_date: string;
  end_date: string;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  warnings: unknown[];
  error_code?: string | null;
  error_message?: string | null;
  symbols: MultiSymbolDefinition[];
  summary: BacktestSummaryResponse;
  symbol_summaries: MultiSymbolRunSymbolSummaryResponse[];
  trade_groups: MultiSymbolTradeGroupResponse[];
  equity_curve: EquityCurvePointResponse[];
  symbol_equity_curves: Record<string, EquityCurvePointResponse[]>;
}

export interface MultiSymbolRunHistoryItemResponse {
  id: string;
  name?: string | null;
  status: string;
  created_at: string;
  completed_at?: string | null;
  symbols: string[];
  summary: BacktestSummaryResponse;
}

export interface MultiSymbolRunListResponse {
  items: MultiSymbolRunHistoryItemResponse[];
  total: number;
  offset: number;
  limit: number;
  next_cursor?: string | null;
}

export interface MultiSymbolRunStatusResponse {
  id: string;
  status: string;
  started_at?: string | null;
  completed_at?: string | null;
  error_code?: string | null;
  error_message?: string | null;
}

export interface StepTriggerDefinition {
  mode: "date_offset" | "after_expiration" | "rule_match" | "event_and_rule";
  days_after_prior_step?: number | null;
  rules?: unknown[];
  require_prior_step_status?: "filled" | "expired" | "closed" | null;
}

export interface StepContractSelection {
  strategy_type: string;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
}

export interface WorkflowStepDefinition {
  step_number: number;
  name: string;
  action: "open_position" | "sell_premium" | "roll" | "close_position" | "hedge";
  trigger: StepTriggerDefinition;
  contract_selection: StepContractSelection;
  failure_policy: "liquidate";
}

export interface CreateMultiStepRunRequest {
  name?: string | null;
  symbol: string;
  workflow_type: string;
  start_date: string;
  end_date: string;
  account_size: number | string;
  risk_per_trade_pct: number | string;
  commission_per_contract: number | string;
  slippage_pct?: number | string;
  initial_entry_rules: unknown[];
  steps: WorkflowStepDefinition[];
  idempotency_key?: string | null;
}

export interface MultiStepStepOutcomeResponse {
  step_number: number;
  name: string;
  action: string;
  status: string;
  triggered_at?: string | null;
  executed_at?: string | null;
  failure_reason?: string | null;
}

export interface MultiStepEventResponse {
  step_number: number;
  event_type: string;
  event_at: string;
  message?: string | null;
  payload_json: Record<string, unknown>;
}

export interface MultiStepTradeResponse {
  id: string;
  step_number: number;
  option_ticker: string;
  strategy_type: string;
  entry_date: string;
  exit_date: string;
  quantity: number;
  gross_pnl: number | string;
  net_pnl: number | string;
  total_commissions: number | string;
  entry_reason: string;
  exit_reason: string;
  detail_json: Record<string, unknown>;
}

export interface MultiStepRunDetailResponse {
  id: string;
  name?: string | null;
  symbol: string;
  workflow_type: string;
  status: string;
  start_date: string;
  end_date: string;
  created_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  warnings: unknown[];
  error_code?: string | null;
  error_message?: string | null;
  summary: BacktestSummaryResponse;
  steps: MultiStepStepOutcomeResponse[];
  events: MultiStepEventResponse[];
  trades: MultiStepTradeResponse[];
  equity_curve: EquityCurvePointResponse[];
}

export interface MultiStepRunHistoryItemResponse {
  id: string;
  name?: string | null;
  symbol: string;
  workflow_type: string;
  status: string;
  created_at: string;
  completed_at?: string | null;
  summary: BacktestSummaryResponse;
}

export interface MultiStepRunListResponse {
  items: MultiStepRunHistoryItemResponse[];
  total: number;
  offset: number;
  limit: number;
  next_cursor?: string | null;
}

export interface MultiStepRunStatusResponse {
  id: string;
  status: string;
  started_at?: string | null;
  completed_at?: string | null;
  error_code?: string | null;
  error_message?: string | null;
}
