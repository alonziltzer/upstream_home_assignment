from dagster import asset, Field, Array, String, AssetExecutionContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

DEFAULT_REGEX_LIST = [
    "('(''|[^'])*')|(;)|(\b(ALTER|CREATE|DELETE|DROP|EXEC(UTE){0,1}|"
    "INSERT( +INTO){0,1}|MERGE|SELECT|UPDATE|UNION( +ALL){0,1})\b)"
]


@asset(
    name="sql_injection_report",
    required_resource_keys={"spark_session_resource"},
    metadata={"relative_path": ["Bonus"]},
    config_schema={
        "columns": Field(Array(String), ["vin"]),
        "regex_list": Field(Array(String), DEFAULT_REGEX_LIST),
    },
)
def bonus_asset(context: AssetExecutionContext, Bronze: DataFrame):
    columns = context.op_config["columns"]
    regex_list = context.op_config["regex_list"]

    # Explode all configured columns into rows: (column_name, value)
    exploded_df = Bronze.select(
        F.explode(
            F.array(
                *[
                    F.struct(
                        F.lit(c).alias("original_column"),
                        F.col(c).alias("violating_message"),
                    )
                    for c in columns
                ]
            )
        ).alias("col_struct")
    ).select("col_struct.*")

    combined_regex = "(" + "|".join(regex_list) + ")"

    filtered_df = exploded_df.filter(F.col("violating_message").rlike(combined_regex))

    pattern_match_expr = F.coalesce(
        *[F.when(F.col("violating_message").rlike(r), F.lit(r)) for r in regex_list]
    )

    return filtered_df.withColumn("pattern_matched", pattern_match_expr)
