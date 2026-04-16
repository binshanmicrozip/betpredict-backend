from .predictor import predict
# If your predictor is inside betapp/ml/, use:
# from betapp.ml.predictor import predict


def run_prediction(cricket: dict, price: dict) -> dict:
    return predict(cricket, price)