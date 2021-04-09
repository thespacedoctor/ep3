# Building the `PESSTO_TRAN_CAT` 


## Parsing ATel ePESSTO Classification Phases for `PESSTO_TRAN_CAT`

To grab the content of all PESSTO ATel run the query:

```sql
select distinct a.userText from PESSTO_TRAN_CAT c
INNER JOIN atel_names s
ON c.transientBucketId=s.transientBucketId
INNER JOIN atel_fullcontent a
on s.atelNumber=a.atelNumber
where c.initial_release_number is null and a.userText like "%essto%" ;
```
