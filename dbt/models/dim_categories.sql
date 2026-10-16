select distinct primary_category,
       split_part(primary_category, '.', 1) as domain
from {{ ref('stg_arxiv_papers') }}
