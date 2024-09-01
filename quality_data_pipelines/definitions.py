from dagster import Definitions, load_assets_from_modules

from . import assets
from .assets import (
    check_positive_postage_in_cleaned_sales,
    cleaned_sales_records_state_no_nulls,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    asset_checks=[check_positive_postage_in_cleaned_sales, cleaned_sales_records_state_no_nulls],
)
