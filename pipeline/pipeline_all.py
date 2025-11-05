from pipeline.stages.bronze import bronze_stage
from pipeline.stages.gold import gold_stage
from pipeline.stages.silver import silver_stage


def pipeline():
    bronze_stage()
    silver_stage()
    gold_stage()
