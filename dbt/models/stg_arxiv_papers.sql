select arxiv_id, nullif(doi, '') as doi, title, primary_category,
       date_published::date as date_published
from {{ source('silver', 'project_arxiv') }}
