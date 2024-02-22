import data_ingestion
import data_transformation
import data_validation
import os

from global_variables import *

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'gcp_key_compras_bucket.json'

spark = init_spark_session()


# data_ingestion.get_classes_from_api()
# data_ingestion.get_grupos_from_api()
# data_ingestion.get_pdms_from_api()
# data_ingestion.get_all_materials_from_api(spark)

data_validation.validade_classes_landing(spark)

# data_transformation.classes_landing_to_bronze(spark)
# data_transformation.classes_bronze_to_silver(spark)

# data_transformation.pdm_landing_to_bronze(spark)
# data_transformation.pdm_bronze_to_silver(spark)

# data_transformation.material_landing_to_bronze(spark)
# data_transformation.material_bronze_to_silver(spark)
# data_transformation.material_pdm_silver_to_gold(spark)

# spark.read.format("delta").load(material_pdm_gold_path).show()