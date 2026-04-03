# kalshicast/backfill/__init__.py
"""Historical data backfill package.

Load order: observations → forecasts → errors → kalman → bss.
Entry point: python -m kalshicast.backfill.run
"""