import pandas as pd
from dagster import AssetCheckResult, AssetCheckSeverity, asset, asset_check
from great_expectations.dataset import PandasDataset

from .data_functions import convert_negative_to_positive, put_in_age_group


@asset
def sales_records() -> pd.DataFrame:
    filepath = "/home/jaymin/Documents/quality-data-pipelines/data/test1.csv"
    df = pd.read_csv(filepath)
    return df


@asset
def cleaned_sales_records(sales_records: pd.DataFrame) -> pd.DataFrame:
    sales_records["postage"] = sales_records["postage"].apply(convert_negative_to_positive)
    return sales_records


@asset
def sales_by_region(cleaned_sales_records: pd.DataFrame) -> pd.DataFrame:
    salesdf = cleaned_sales_records.groupby("State")["item_count"].agg({"sum", "min", "max", "count"}).reset_index()
    return salesdf


@asset
def sales_by_age_group(cleaned_sales_records: pd.DataFrame) -> pd.DataFrame:
    cleaned_sales_records["group"] = cleaned_sales_records["Age"].apply(put_in_age_group)
    grouped_ages_df = (
        cleaned_sales_records.groupby("group")["item_count"].agg({"mean", "median", "count"}).reset_index()
    )

    return grouped_ages_df


@asset
def create_sales_report(sales_by_age_group, sales_by_region) -> None:
    output_path = "/home/jaymin/Documents/quality-data-pipelines/data/super_critical_business-report-2024-Aug-01.xlsx"
    data = {"sales_by_region": sales_by_age_group, "sales_by_age": sales_by_region}
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sheetname, frame in data.items():
            frame.to_excel(writer, sheet_name=sheetname, startrow=2, index=False, header=False)
            header_format = writer.book.add_format({"bold": True, "text_wrap": True, "valign": "top", "border": 1})
            for col_num, value in enumerate(frame.columns.values):
                writer.sheets[sheetname].write(1, col_num, value, header_format)

            # apply autofiltering and freeze first row and column
            writer.sheets[sheetname].freeze_panes(2, 1)


@asset_check(asset=cleaned_sales_records)
def check_positive_postage_in_cleaned_sales(cleaned_sales_records: pd.DataFrame) -> AssetCheckResult:
    # Count number of rows of "postage" that are negative
    negative_postage_count = len(cleaned_sales_records[cleaned_sales_records["postage"] < 0])
    if negative_postage_count > 0:
        return AssetCheckResult(
            passed=False,
            description=f"{negative_postage_count} negative postage values found in cleaned sales records",
            metadata={"negative_postage_count": int(negative_postage_count)},
        )
    return AssetCheckResult(passed=True)


@asset_check(asset=cleaned_sales_records, blocking=True)
def cleaned_sales_records_state_no_nulls(context, cleaned_sales_records: pd.DataFrame) -> AssetCheckResult:
    dataset = PandasDataset(cleaned_sales_records)
    check_outcome = dataset.expect_column_values_to_not_be_null(column="State")
    passed = check_outcome["success"]
    return AssetCheckResult(passed=passed, severity=AssetCheckSeverity.ERROR)
