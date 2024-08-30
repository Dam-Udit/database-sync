import pandas as pd
from sqlalchemy import create_engine
from data_utils import DataUtils

data_utils = DataUtils()
data_utils.get_newer_records()
data_utils.transform_and_load_data()
