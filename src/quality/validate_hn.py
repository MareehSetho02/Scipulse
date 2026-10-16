"""Les six attentes HN ; les scores nulls des commentaires sont autorisés."""
import os
import sys
import great_expectations as gx
from elasticsearch import Elasticsearch
from src.quality.common import run_suite,print_results
from src.quality.extract_for_validation import extract_hn

def validate_hn_data(df,root='gx'):
    e=gx.expectations
    return run_suite(df,'hn',[
        e.ExpectColumnValuesToNotBeNull(column='hn_id'),
        e.ExpectColumnValuesToNotBeNull(column='type'),
        e.ExpectColumnValuesToBeInSet(column='type',value_set=['story','comment','job','poll','pollopt']),
        e.ExpectColumnValuesToBeBetween(column='score',min_value=0),
        e.ExpectColumnValuesToBeUnique(column='hn_id'),
        e.ExpectTableRowCountToBeBetween(min_value=1)],root)

def main():
    with Elasticsearch(os.getenv('ES_HOST','http://localhost:9200')) as es:df=extract_hn(es)
    _,result=validate_hn_data(df);print_results(result);sys.exit(0 if result.success else 1)
if __name__=='__main__':main()
