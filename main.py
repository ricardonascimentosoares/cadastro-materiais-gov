
import data_ingestion
import data_transformation
import os

from data_transformation import pdms_to_delta_bronze
from global_variables import init_spark_session

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'gcp_key_compras_bucket.json'

spark = init_spark_session()


data_ingestion.get_classes_from_api()
data_ingestion.get_grupos_from_api()
data_ingestion.get_pdms_from_api()
data_ingestion.get_all_materials_from_api(spark)

data_transformation.classes_landing_to_bronze(spark)
data_transformation.classes_bronze_to_silver(spark)

data_transformation.pdm_landing_to_bronze(spark)
data_transformation.pdm_bronze_to_silver(spark)

data_transformation.material_landing_to_bronze(spark)
data_transformation.material_bronze_to_silver(spark)

pdms_to_delta_bronze().show()
