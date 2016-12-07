from model import fundamentals, Equity
import dao
from dao import get_fundamentals, query

df = get_fundamentals(
    query(fundamentals.balance_sheet.total_assets, fundamentals.income_statement.ebit)
        .filter(fundamentals.balance_sheet.total_assets >= 20)
        .filter(fundamentals.income_statement.ebit > 25)
)

print df
print

df = get_fundamentals(
    query(fundamentals.balance_sheet.total_assets, fundamentals.income_statement.ebit)
        .filter(fundamentals.balance_sheet.total_assets >= 20)
)
print df
print

df = get_fundamentals(
    query(fundamentals.balance_sheet.total_assets, fundamentals.income_statement.ebit)
        .filter(fundamentals.balance_sheet.total_assets >= 20)
        .order_by(fundamentals.balance_sheet.total_assets.desc())
)
print df
print

print df.loc['total_assets']
print



