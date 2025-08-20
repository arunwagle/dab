from pyspark.sql import functions as F
from pyspark.sql.types import StringType


default_values = ['Facets']

# adding sample udf to udf mapping so that in future we can add more udfs
@F.udf(returnType=StringType())
def sample_udf(val):
    return "sample"

# Map the udf_name string to actual Python UDFs
udf_mapping = {
    "sample_udf": sample_udf
}


def substitute_string_udf(replacement_value: str):
    @F.udf (returnType=StringType())
    def replace_placeholder(path: str) -> str:
        return path.replace("{}", replacement_value)
    
    return replace_placeholder