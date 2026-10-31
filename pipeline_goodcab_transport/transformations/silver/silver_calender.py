from pyspark import pipelines as dp
from pyspark.sql.functions import *

catalog=spark.conf.get("catalog")

@dp.materialized_view(
    name=f"{catalog}.silver.calender",
    comment="Calendar table for date-related calculations",
    table_properties={
        "quality": "calender",
        "layer": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def calender(
    start_date="2024-01-01",
    end_date="2025-12-31",
    holidays=None
):
    if holidays is None:
        holidays = [
            "2020-01-01", "2020-12-25",
            "2021-01-01", "2021-12-25",
            "2022-01-01", "2022-12-25",
            "2023-01-01", "2023-12-25",
            "2024-01-01", "2024-12-25",
            "2025-01-01", "2025-12-25"
        ]

    # Create holiday DataFrame and convert strings to DATE
    holidays_df = (
        spark.createDataFrame(
            [(h,) for h in holidays],
            ["holiday_date"]
        )
        .withColumn("holiday_date", to_date(col("holiday_date")))
    )

    # Generate calendar dates and their attributes
    df = spark.sql(f"""
        SELECT
            date_sequence AS date,
            date_format(date_sequence, 'ddMMyyyy') AS id,
            year(date_sequence) AS year,
            month(date_sequence) AS month,
            dayofmonth(date_sequence) AS day_of_month,
            date_format(date_sequence, 'EEEE') AS day_name,

            -- ISO weekday: Monday = 1, Sunday = 7
            pmod(dayofweek(date_sequence) + 5, 7) + 1 AS day_of_week,

            date_format(date_sequence, 'MMM') AS month_name,
            weekofyear(date_sequence) AS week_of_year,
            dayofyear(date_sequence) AS day_of_year,
            quarter(date_sequence) AS quarter,

            CASE
                WHEN dayofweek(date_sequence) IN (1, 7)
                THEN true
                ELSE false
            END AS is_weekend

        FROM (
            SELECT explode(
                sequence(
                    to_date('{start_date}'),
                    to_date('{end_date}'),
                    interval 1 day
                )
            ) AS date_sequence
        )
    """)

    # Add holiday information
    df = (
        df.join(
            holidays_df,
            df["date"] == holidays_df["holiday_date"],
            "left"
        )
        .withColumn(
            "is_holiday",
            col("holiday_date").isNotNull()
        )
        .withColumn(
            "holiday_name",
            when(
                col("holiday_date").isNotNull(),
                lit("Holiday")
            ).otherwise(lit(None).cast("string"))
        )
        .drop("holiday_date")
    )

    return df