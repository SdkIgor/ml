#!/usr/bin/env python3
from pycaret.regression import RegressionExperiment
from pycaret.classification import *

config = {
    'pipeline_name':'extended_pipeline'
}

s = RegressionExperiment()
model = s.load_model(config['pipeline_name'])
create_api(model, 'ml_api')
