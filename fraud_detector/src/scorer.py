import pandas as pd
import logging
from catboost import CatBoostClassifier, Pool
import pickle

# Настройка логгера
logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

# Import model
model = CatBoostClassifier()
with open('./models/catboost.pickle', 'rb') as handle:
    model = pickle.load(handle)

cat_cols = ['merch', 'cat_id', 'gender', 'street', 'one_city', 'us_state', 'post_code']
text_cols = ['jobs']

# Define optimal threshold
model_th = 0.98
logger.info('Pretrained model imported successfully...')

def make_pred(dt, source_info="kafka"):
    # Calculate score
    pool = Pool(dt, cat_features=cat_cols, text_features=text_cols)
    submission = pd.DataFrame({
        'score':  model.predict_proba(pool)[:, 1],
        'fraud_flag': (model.predict_proba(pool)[:, 1] > model_th) * 1
    })

    logger.info(f'Prediction complete for data from {source_info}')

    # Return proba for positive class
    return submission