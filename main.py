
import data_ingestion
import os

from data_transformation import pdms_to_delta_bronze


# data_ingestion.get_classes_from_api()
# data_ingestion.get_grupos_from_api()
# data_ingestion.get_pdms_from_api()
# data_ingestion.get_all_materials_from_api()

pdms_to_delta_bronze().show()
