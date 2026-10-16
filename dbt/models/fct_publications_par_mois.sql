select date_trunc('month', date_published)::date as month,
       primary_category, count(*) as publications
from {{ ref('stg_arxiv_papers') }}
group by 1, 2
