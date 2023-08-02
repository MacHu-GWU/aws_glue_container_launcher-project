# -*- coding: utf-8 -*-

"""
A sample module that depends on awsglue lib.
"""

from awsglue import DynamicFrame
from awsglue.gluetypes import DynamicRecord

def double_a_column(gdf: DynamicFrame, col_name: str, trans_ctx: str):
    """
    Double the value of a column.

    Example:

        >>> gdf.toDF().show()
        +-----+
        |value|
        +-----+
        |  1  |
        |  2  |
        |  3  |
        +-----+
        >>> gdf_new = double_a_column(gdf, col_name="value", trans_ctx="double_a_column")
        >>> gdf_new.toDF().show()
        +-----+
        |value|
        +-----+
        |  2  |
        |  4  |
        |  6  |
        +-----+
    """
    def func(row: DynamicRecord) -> DynamicRecord:
        row[col_name] = row[col_name] * 2
        return row
    
    return gdf.map(
        f=func,
        transformation_ctx=trans_ctx,
    )
