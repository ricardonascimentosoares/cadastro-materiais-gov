from ingestion import (
    classes_ingestion,
    pdm_ingestion,
    material_ingestion,
)
from transformation import (
    classes_transformation,
    pdm_transformation,
    material_transformation,
)
from check import classes_check, material_check, pdm_check
from utils.config import *
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "utils/gcp_key_compras_bucket.json"

spark = init_spark_session()

classes_ingestion.get_classes_from_api()
classes_check.check_landing(spark)
classes_transformation.landing_to_bronze(spark)
classes_transformation.bronze_to_silver(spark)

# pdm_ingestion.get_pdms_from_api()
# pdm_check.check_landing(spark)
# pdm_transformation.landing_to_bronze(spark)
# pdm_transformation.bronze_to_silver(spark)

# material_ingestion.get_all_materials_from_api(spark)
# material_check.check_landing(spark)
# material_transformation.landing_to_bronze(spark)
# material_transformation.bronze_to_silver(spark)
# material_transformation.silver_to_gold_material_agg(spark)
# material_transformation.silver_to_gold_material_char_detail(spark)

# material_transformation.export_data_to_excel(spark)
