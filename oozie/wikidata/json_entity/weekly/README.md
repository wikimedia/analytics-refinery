This directory contains the weekly job converting a newly imported
wikidata entities json dump into parquet.

Wikidata entities json dump are a view of the wikidata knwoledge-graph almost
as how it is stored in wikibase. atomic elements are entities (items
and properties), and each entity contains multiple related values:
 - textual values - labels, description, aliases
 - claims - links to other wikibase entities or values, defined as mainSnak
            (main link) and qualifiers/references (sublinks providing precisions)
 - siteLinks - References from that entity to wikipedia pages (URL)