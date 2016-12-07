import csv
import dao
from datetime import datetime
from model import *
from utils import get_start, update_progress, to_float
from click import progressbar
import logging as log

log.basicConfig(format='%(asctime)s %(message)s', level=log.DEBUG)

dao.insert_equities()

row_count = sum(1 for line in open('resource/SHARADAR_SF1_all.csv'))
current_ticker = None
start = get_start()
log.info("Start: %d" % start)


def map_row_to_fundamentals(s, row):
    f = Fundamentals()
    f.balance_sheet = BalanceSheet()
    f.income_statement = IncomeStatement()
    f.cash_flow_statement = CashFlowStatement()
    f.company_reference = CompanyReference()
    f.earnings_ratios = EarningsRatios()
    f.earnings_report = EarningsReport()
    f.financial_statement_filing = FinancialStatementFiling()
    f.operation_ratios = OperationRatios()
    f.share_class_reference = ShareClassReference()
    f.valuation_ratios = ValuationRatios()
    f.valuation = Valuation()
    f.asset_classification = AssetClassification()
    f.symbol = current_ticker
    f.price = to_float(row, 'price')
    f.date = datetime.strptime(row['datekey'], "%Y-%m-%d").date()
    e = s.query(Equity).filter(Equity.ticker == current_ticker).first()
    if e is None:
        rt = s.query(RelatedTicker).filter(RelatedTicker.ticker == current_ticker).first()
        e = rt.equity
        if e is None:
            raise ValueError('No entry found for ticker %s' % current_ticker)
    f.company_reference.country_id = e.country()
    f.company_reference.legal_name = e.name
    exchange = e.exchange
    if exchange == 'DELISTED':
        exchange = e.delisted_from
    f.company_reference.primary_exchange_id = exchange
    f.share_class_reference.currency_id = e.currency
    f.share_class_reference.is_depositary_receipt = e.is_foreign
    f.share_class_reference.symbol = current_ticker
    f.asset_classification.morningstar_industry_code = e.industry
    f.asset_classification.morningstar_sector_code = e.sector
    f.asset_classification.sic = e.sic
    f.balance_sheet.current_assets = to_float(row, 'assetsc')
    f.balance_sheet.current_debt = to_float(row, 'debtc')
    f.balance_sheet.current_liabilities = to_float(row, 'liabilitiesc')
    f.balance_sheet.gains_losses_not_affecting_retained_earnings = to_float(row, 'accoci')
    f.balance_sheet.goodwill_and_other_intangible_assets = to_float(row, 'intangibles')
    f.balance_sheet.inventory = to_float(row, 'inventory')
    f.balance_sheet.invested_capital = to_float(row, 'invcap')
    f.balance_sheet.long_term_debt = to_float(row, 'debtnc')
    f.balance_sheet.long_term_investments = to_float(row, 'investmentsnc')
    f.balance_sheet.net_ppe = to_float(row, 'ppnenet')
    f.balance_sheet.non_current_deferred_revenue = to_float(row, 'deferredrev')
    f.balance_sheet.payables = to_float(row, 'payables')
    f.balance_sheet.receivables = to_float(row, 'receivables')
    f.balance_sheet.retained_earnings = to_float(row, 'retearn')
    f.balance_sheet.short_term_investments_held_to_maturity = to_float(row, 'investmentsc')
    f.balance_sheet.stockholders_equity = to_float(row, 'equity')
    f.balance_sheet.tangible_book_value = to_float(row, 'tangibles')
    f.balance_sheet.tax_assets_total = to_float(row, 'taxassets')
    f.balance_sheet.total_assets = to_float(row, 'assets')
    f.balance_sheet.total_debt = to_float(row, 'debt')
    f.balance_sheet.total_debt = to_float(row, 'debtusd')
    f.balance_sheet.total_investments = to_float(row, 'investments')
    f.balance_sheet.total_liabilities = to_float(row, 'liabilities')
    f.balance_sheet.total_non_current_assets = to_float(row, 'assetsnc')
    f.balance_sheet.total_non_current_liabilities = to_float(row, 'liabilitiesnc')
    f.balance_sheet.total_tax_payable = to_float(row, 'taxliabilities')
    f.balance_sheet.working_capital = to_float(row, 'workingcapital')
    f.cash_flow_statement.capital_expenditure = to_float(row, 'capex')
    f.cash_flow_statement.cash_dividends_paid = to_float(row, 'ncfdiv')
    f.cash_flow_statement.changes_in_cash = to_float(row, 'ncf')
    f.cash_flow_statement.depreciation_amortization_depletion = to_float(row, 'depamor')
    f.cash_flow_statement.effect_of_exchange_rate_changes = to_float(row, 'ncfx')
    f.cash_flow_statement.financing_cash_flow = to_float(row, 'ncff')
    f.cash_flow_statement.free_cash_flow = to_float(row, 'fcf')
    f.cash_flow_statement.investing_cash_flow = to_float(row, 'ncfi')
    f.cash_flow_statement.net_common_stock_issuance = to_float(row, 'ncfcommon')
    f.cash_flow_statement.net_issuance_payments_of_debt = to_float(row, 'ncfdebt')
    f.cash_flow_statement.operating_cash_flow = to_float(row, 'ncfo')
    f.cash_flow_statement.stock_based_compensation = to_float(row, 'sbcomp')
    f.company_reference.primary_symbol = current_ticker
    f.earnings_ratios.basic_average_shares = to_float(row, 'shareswa')
    f.earnings_ratios.diluted_average_shares = to_float(row, 'shareswadil')
    f.earnings_report.basic_eps = to_float(row, 'eps')
    f.earnings_report.diluted_eps = to_float(row, 'epsdil')
    f.earnings_report.dividend_per_share = to_float(row, 'dps')
    f.financial_statement_filing.file_date = datetime.strptime(row['datekey'], "%Y-%m-%d").date()
    f.financial_statement_filing.period_ending_date = datetime.strptime(row['reportperiod'], "%Y-%m-%d").date()
    f.income_statement.cost_of_revenue = to_float(row, 'cor')
    f.income_statement.ebit = to_float(row, 'ebit')
    f.income_statement.ebitda = to_float(row, 'ebitda')
    f.income_statement.gross_profit = to_float(row, 'gp')
    f.income_statement.interest_expense = to_float(row, 'intexp')
    f.income_statement.net_income = to_float(row, 'netinc')
    f.income_statement.net_income_common_stockholders = to_float(row, 'netinccmn')
    f.income_statement.net_income_discontinuous_operations = to_float(row, 'netincdis')
    f.income_statement.operating_expense = to_float(row, 'opex')
    f.income_statement.operating_income = to_float(row, 'opinc')
    f.income_statement.preferred_stock_dividends = to_float(row, 'prefdivis')
    f.income_statement.pretax_income = to_float(row, 'ebt')
    f.income_statement.research_and_development = to_float(row, 'rnd')
    f.income_statement.selling_general_and_administration = to_float(row, 'sgna')
    f.income_statement.tax_provision = to_float(row, 'taxexp')
    f.income_statement.total_revenue = to_float(row, 'revenue')
    f.operation_ratios.current_ratio = to_float(row, 'currentratio')
    f.operation_ratios.ebitda_margin = to_float(row, 'ebitdamargin')
    f.operation_ratios.gross_margin = to_float(row, 'grossmargin')
    f.operation_ratios.net_margin = to_float(row, 'netmargin')
    f.operation_ratios.roa = to_float(row, 'roa')
    f.operation_ratios.roe = to_float(row, 'roe')
    f.operation_ratios.roic = to_float(row, 'roic')
    f.operation_ratios.total_debt_equity_ratio = to_float(row, 'de')
    f.valuation_ratios.book_value_per_share = to_float(row, 'bvps')
    f.valuation_ratios.dividend_yield = to_float(row, 'divyield')
    f.valuation_ratios.ev_to_ebitda = to_float(row, 'evebitda')
    f.valuation_ratios.fcf_per_share = to_float(row, 'fcfps')
    f.valuation_ratios.payout_ratio = to_float(row, 'payoutratio')
    f.valuation_ratios.pb_ratio = to_float(row, 'pb')
    f.valuation_ratios.pe_ratio = to_float(row, 'pe1')
    f.valuation_ratios.ps_ratio = to_float(row, 'ps1')
    f.valuation_ratios.sales_per_share = to_float(row, 'sps')
    f.valuation_ratios.tangible_book_value_per_share = to_float(row, 'tbvps')
    f.valuation.enterprise_value = to_float(row, 'ev')
    f.valuation.market_cap = to_float(row, 'marketcap')
    f.valuation.shares_outstanding = to_float(row, 'sharesbas')

    # Calculated fields
    # In Morningstar the operation_income_growth (aka OperatingIncomeGrowth (10002)) is computed as the percentage
    # growth in the last quarter compared to the same quarter the previous years (aka Year over Year OI Growth %)
    history = s.query(Fundamentals.date, IncomeStatement.operating_income) \
        .join(IncomeStatement) \
        .filter(Fundamentals.symbol == current_ticker) \
        .order_by(Fundamentals.date.desc()) \
        .limit(4) \
        .all()

    if len(history) == 4:
        operation_income_current = to_float(row, 'opinc')
        operation_income_yoy = to_float(history[-1][1])
        if operation_income_current is not None and operation_income_yoy is not None:
            f.operation_ratios.operation_income_growth = (to_float(row, 'opinc') - float(history[-1][1])) / float(
                history[-1][1])

    return f


with open('resource/SHARADAR_SF1_all.csv', 'rb') as csvfile:
    reader = csv.DictReader(csvfile)
    with progressbar(length=row_count) as progress:
        for row in reader:
            if progress.pos < start:
                update_progress(progress)
                continue

            # ARQ: As Reporter Quarterly
            if row['dimension'] != 'ARQ':
                update_progress(progress)
                continue

            if row['ticker'] != current_ticker:
                current_ticker = row['ticker']

            with dao.db_session() as s:
                f = map_row_to_fundamentals(s, row)

                s.add(f)

                update_progress(progress)
