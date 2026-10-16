"""Les dix attentes ArXiv du TP 4, adaptées à GX Core 1.x."""
import os
import sys
import great_expectations as gx
from elasticsearch import Elasticsearch
from src.quality.common import run_suite,print_results
from src.quality.extract_for_validation import extract_arxiv,ARXIV_COLUMNS
KNOWN_CS_CATEGORIES=['cs.'+x for x in 'AI AR CC CE CG CL CR CV CY DB DC DL DM DS ET FL GL GR GT HC IR IT LG LO MA MM MS NA NE NI OH OS PF PL RO SC SD SE SI SY'.split()]+['stat.ML','math.IT','math.OC']

def validate_arxiv_data(df,root='gx'):
    e=gx.expectations
    return run_suite(df,'arxiv',[
        e.ExpectColumnValuesToNotBeNull(column='arxiv_id'),
        e.ExpectColumnValuesToNotBeNull(column='title'),
        e.ExpectColumnValuesToNotBeNull(column='abstract',mostly=.99),
        e.ExpectColumnValuesToNotBeNull(column='primary_category'),
        e.ExpectColumnValuesToBeUnique(column='arxiv_id'),
        e.ExpectColumnValueLengthsToBeBetween(column='abstract',min_value=50,mostly=.95),
        e.ExpectColumnValueLengthsToBeBetween(column='title',min_value=10),
        e.ExpectColumnValuesToBeInSet(column='primary_category',value_set=KNOWN_CS_CATEGORIES,mostly=.95),
        e.ExpectTableRowCountToBeBetween(min_value=1,max_value=10000),
        e.ExpectTableColumnsToMatchSet(column_set=ARXIV_COLUMNS,exact_match=False)],root)

def main():
    with Elasticsearch(os.getenv('ES_HOST','http://localhost:9200')) as es:df=extract_arxiv(es)
    _,result=validate_arxiv_data(df);print_results(result);sys.exit(0 if result.success else 1)
if __name__=='__main__':main()
