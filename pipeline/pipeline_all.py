from pipeline.stages.bronze import bronze_stage
from pipeline.stages.gold import gold_stage
from pipeline.stages.silver import  silver_stage


def data_pipeline():
    bronze_stage()
    silver_stage()
    gold_stage()


if __name__ == "__main__":
    data_pipeline()
