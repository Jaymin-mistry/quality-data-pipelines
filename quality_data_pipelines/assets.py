import pandas as pd
from dagster import asset

from .data_functions import convert_negative_to_positive, put_in_age_group


@asset
def sales_records() -> pd.DataFrame:
    filepath = "/Users/jayminmistry/Documents/quality-data-pipelines/data/test1.csv"
    df = pd.read_csv(filepath)
    return df


@asset  # (deps=[sales_records])
def cleaned_sales_records(sales_records: pd.DataFrame) -> pd.DataFrame:
    sales_records["postage"] = sales_records["postage"].apply(convert_negative_to_positive)
    return sales_records


@asset  # (deps=[cleaned_sales_records])
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


@asset  # (deps=[sales_by_age_group, sales_by_region])
def create_sales_report(sales_by_age_group, sales_by_region) -> None:
    output_path = (
        "/Users/jayminmistry/Documents/quality-data-pipelines/data/super_critical_business-report-2024-Aug-01.xlsx"
    )
    data = {"sales_by_region": sales_by_age_group, "sales_by_age": sales_by_region}
    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        for sheetname, frame in data.items():
            frame.to_excel(writer, sheet_name=sheetname, startrow=2, index=False, header=False)
            header_format = writer.book.add_format({"bold": True, "text_wrap": True, "valign": "top", "border": 1})
            for col_num, value in enumerate(frame.columns.values):
                writer.sheets[sheetname].write(1, col_num, value, header_format)

            # apply autofiltering and freeze first row and column
            writer.sheets[sheetname].freeze_panes(2, 1)
