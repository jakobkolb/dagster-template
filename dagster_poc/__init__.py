from dagster import load_assets_from_modules
from dagster_poc import assets

assets = load_assets_from_modules([assets])