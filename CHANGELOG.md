# CHANGELOG

## 2023-02-15

- Use `repr()` inside of `quote()` so that escaping is preserved
- Publish v0.4.2 to PyPI

## 2023-02-15

- add `function.function()` for calling arbitrary Kusto functions
- Publish v0.4.1 to PyPI

## 2022-04-08

- Add math functions
- Add type conversion functions
- Fix bug that was making TableExpr non-idempotent--now it returns an updated copy.
- Catch error for inspecting nonexistent table
- Fix bug from rename of Database.query -> Database.execute
- Publish v0.3.1 to PyPI

## 2022-04-07

- Add `[start|end]of[day|week|month|year]` functions
- Add `between` operator
- Publish v0.2.1 to PyPI