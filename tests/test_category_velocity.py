"""Verify category aggregation, missing years and zero denominators in Spark."""
import pytest
pytest.importorskip('pyspark', reason='Requires the Spark Python runtime')
from pyspark.sql import SparkSession
from src.analytics.spark_gold import category_citation_velocity


def test_category_velocity():
    spark=SparkSession.builder.master('local[1]').appName('category-velocity-test').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').getOrCreate()
    try:
        articles=spark.createDataFrame([('a','d1','cs.AI'),('b','d2','cs.AI'),('c','d3','cs.LG'),('d','unknown','cs.LG')],['arxiv_id','doi','primary_category'])
        yearly=spark.createDataFrame([('d1',2022,4),('d2',2022,6),('d1',2023,8),('d2',2023,12),('d1',2025,3),('d3',2022,0),('d3',2023,2)],['doi','year','citations'])
        rows={(r.primary_category,r.year):r for r in category_citation_velocity(articles,yearly).collect()}
        ai=rows['cs.AI',2023]
        assert ai.category_citations==20 and ai.observed_articles==2
        assert ai.citations_per_observed_article==10
        assert ai.citation_acceleration==10 and ai.citation_growth_pct==100
        assert rows['cs.AI',2022].citation_acceleration is None
        assert rows['cs.AI',2025].citation_acceleration is None
        assert rows['cs.AI',2025].citation_growth_pct is None
        assert rows['cs.LG',2023].citation_growth_pct is None
        assert rows['cs.LG',2023].observed_articles==1
    finally:
        spark.stop()
