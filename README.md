# SQLGLot + Vercel

Parse SQL using SQLGLot.

## Endpoints

* AST: `/ast?sql=sql&read=dialect;`
* Transpile (Convert from one dialect to another): `/transpile?sql=sql&read=dialect&write=dialect;`
* CTE table lineage: `/lineage?sql=sql&read=dialect;`

## Dialects

https://sqlglot.com/sqlglot/dialects/dialect.html#Dialect