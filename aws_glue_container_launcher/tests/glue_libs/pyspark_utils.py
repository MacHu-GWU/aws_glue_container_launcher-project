# -*- coding: utf-8 -*-

"""
A sample module that depends on PySpark.
"""

from pyspark.sql import DataFrame, Column

def double_a_column(df: DataFrame, col: Column, col_name: str):
    """
    Double the value of a column.

    Example:

        >>> df.show()
        +-----+-------+
        | id  | value |
        +-----+-------+
        |  a  |   1   |
        |  b  |   2   |
        |  c  |   3   |
        +-----+-------+
        >>> df_new = double_a_column(df, col=df.value, col_name="doubled_value")
        >>> df_new.show()
        +-----+-------+---------------+
        | id  | value | doubled_value |
        +-----+-------+---------------+
        |  a  |   1   |       2       |
        |  b  |   2   |       4       |
        |  c  |   3   |       6       |
        +-----+-------+---------------+
    """
    return df.withColumn(col_name, col * 2)
