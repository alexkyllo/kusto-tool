# CHANGELOG

## 2022-04-08

- Add math functions
- Add type conversion functions
- Fix bug that was making TableExpr non-idempotent--now it returns an updated copy.
- Catch error for inspecting nonexistent table
- Fix bug from rename of Database.query -> Database.execute

## 2022-04-07

- Add `[start|end]of[day|week|month|year]` functions
- Add `between` operator
- Publish v0.2.1 to PyPI