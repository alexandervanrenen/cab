
# Queries

Schema and queries are already pre-generated (`schema.sql`, `queries.sql`), but can be recreated using the description below.
The dataset must be recreated, because it is too large (100GB).

## Create Schema

Ready to use file: `schema.sql`
Copied from `https://github.com/fivetran/benchmark/blob/master/300-PopulateSnowflake.sh`.
Modifications to comply with newer TPC_DS standard: rename `c_last_review_date` -> `c_last_review_date_sk`

## Generate Dataset

Generated with `https://github.com/gregrahn/tpcds-kit`:
```shell script
mkdir gen100
cd tools
make OS=MACOS
./dsdgen -DISTRIBUTIONS tpcds.idx -DIR ../gen100 -SCALE 100
```

## Upload Dataset

- Connect to snowflake with ``snowsql -a [account_name]`` (install with ``brew``)
- `[account_name]` is the subdomain of the snowflake URL for your instance: `https://[account_name].snowflakecomputing.com`
- Username is the one from the webinterface (no mail) and usual password
```shell script
snowsql -a [account_name]
```
- List files: ``list @~;``
- Delete files: ``rm @~/staged/orders.tbl.u1.gz;``
```shell script
put file:////tmp/tpcds-kit/gen100/call_center.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/catalog_page.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/catalog_returns.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/catalog_sales.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/customer.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/customer_address.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/customer_demographics.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/date_dim.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/household_demographics.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/income_band.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/inventory.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/item.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/promotion.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/reason.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/ship_mode.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/store.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/store_returns.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/store_sales.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/time_dim.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/warehouse.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/web_page.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/web_returns.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/web_sales.dat @~/staged;
put file:////tmp/tpcds-kit/gen100/web_site.dat @~/staged;
```

## Copy into TABLES

```sql
copy into call_center from @~/staged/call_center.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into catalog_page from @~/staged/catalog_page.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into catalog_returns from @~/staged/catalog_returns.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into catalog_sales from @~/staged/catalog_sales.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into customer from @~/staged/customer.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into customer_address from @~/staged/customer_address.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into customer_demographics from @~/staged/customer_demographics.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into date_dim from @~/staged/date_dim.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into household_demographics from @~/staged/household_demographics.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into income_band from @~/staged/income_band.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into inventory from @~/staged/inventory.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into item from @~/staged/item.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into promotion from @~/staged/promotion.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into reason from @~/staged/reason.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into ship_mode from @~/staged/ship_mode.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into store from @~/staged/store.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into store_returns from @~/staged/store_returns.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into store_sales from @~/staged/store_sales.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into time_dim from @~/staged/time_dim.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into warehouse from @~/staged/warehouse.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into web_page from @~/staged/web_page.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into web_returns from @~/staged/web_returns.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into web_sales from @~/staged/web_sales.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into web_site from @~/staged/web_site.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
```

## Generate Queries

Generated with `https://github.com/gregrahn/tpcds-kit`:
```shell script
mkdir gen100
cd tools
make OS=MACOS
./dsqgen -DIRECTORY ../query_templates -INPUT ../query_templates/templates.lst -VERBOSE Y -QUALIFY Y -SCALE 100 -DIALECT ansi -OUTPUT_DIR ../gen100
```
Required modifications for snowflake sql dialect:
1. `+ 14 days` does not work and needs to be replaced with `+ INTERVAL'14 days'` for all days
2. Query 41 syntax for `limit` needs to be adjusted: move `limit` to the end of the query.
3. In query 72 we need to cast `and d3.d_date > d1.d_date + 5` into a date before doing the addition: `and d3.d_date > d1.d_date::date + 5`
4. Run the following script to insert comments, this allows to inspect the history in snowflake much more easily: `cat queries.sql | awk '{if($0 ~ /-- start query .* in stream 0 using template query.*tpl/) {print "comment on schema tpcds100 is \047query " substr($0, 16, 2) "\047;"} else {print $0}}'`
5. In query 61, change -7 to -6, otherwise there is no output.

# Refresh Functions

## Upload refresh function data

```sql
put file:////tmp/tpcds-kit/update100/delete_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/inventory_delete_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_call_center_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_catalog_order_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_catalog_order_lineitem_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_catalog_page_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_catalog_returns_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_customer_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_customer_address_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_inventory_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_item_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_promotion_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_purchase_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_purchase_lineitem_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_store_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_store_returns_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_warehouse_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_web_order_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_web_order_lineitem_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_web_page_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_web_returns_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_web_site_1.dat @~/staged/;
put file:////tmp/tpcds-kit/update100/s_zip_to_gmt_1.dat @~/staged/;
```

## Create Auxiliary Structures
Ready to use file: `staging_tables.sql` (created using the TPC-DS specification `wlin_coupon_amount` -> `wlin_coupon_amt`)
Ready to use file: `views.sql` (copied from the TPC-DS specification)

## Refresh Function 1: LF_CR

```sql
comment on schema tpcds100 is 'Refresh Function 1: LF_CR';
copy into s_catalog_returns from @~/staged/s_catalog_returns_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into catalog_returns (select * from crv);
```

## Refresh Function 2: LF_CS

```sql
comment on schema tpcds100 is 'Refresh Function 2: LF_CS';
copy into s_catalog_order from @~/staged/s_catalog_order_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into s_catalog_order_lineitem from @~/staged/s_catalog_order_lineitem_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into catalog_sales (select * from csv);
```

## Refresh Function 3: LF_I

```sql
comment on schema tpcds100 is 'Refresh Function 3: LF_I';
copy into s_inventory from @~/staged/s_inventory_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into inventory (select * from iv);
```

## Refresh Function 4: LF_SR

```sql
comment on schema tpcds100 is 'Refresh Function 4: LF_SR';
copy into s_store_returns from @~/staged/s_store_returns_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into store_returns (select * from srv);
```

## Refresh Function 5: LF_SS

```sql
comment on schema tpcds100 is 'Refresh Function 5: LF_SS';
copy into s_purchase from @~/staged/s_purchase_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into s_purchase_lineitem from @~/staged/s_purchase_lineitem_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into store_sales (select * from ssv);
```

## Refresh Function 6: LF_WR

```sql
comment on schema tpcds100 is 'Refresh Function 6: LF_WR';
copy into s_web_returns from @~/staged/s_web_returns_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into web_returns (select * from wrv);
```

## Refresh Function 7: LF_WS

```sql
comment on schema tpcds100 is 'Refresh Function 7: LF_WS';
copy into s_web_order from @~/staged/s_web_order_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into s_web_order_lineitem from @~/staged/s_web_order_lineitem_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into web_sales (select * from wsv);
```

## Refresh Function 8: DF_CS

```sql
comment on schema tpcds100 is 'Refresh Function 8: DF_CS';
delete from catalog_returns where exists (select *
                                          from catalog_sales, date_dim
                                          where cs_item_sk = cr_item_sk and cs_order_number = cr_order_number
                                          and cs_sold_date_sk = d_date_sk
                                          and ((d_date between '2000-05-20'::date and '2000-05-21'::date)
                                            or (d_date between '1999-09-18'::date and '1999-09-19'::date)
                                            or (d_date between '2002-11-12'::date and '2002-11-13'::date))
);
delete from catalog_sales where exists (select *
                                        from date_dim
                                        where cs_sold_date_sk = d_date_sk
                                        and ((d_date between '2000-05-20'::date and '2000-05-21'::date)
                                          or (d_date between '1999-09-18'::date and '1999-09-19'::date)
                                          or (d_date between '2002-11-12'::date and '2002-11-13'::date))
);
```

## Refresh Function 9: DF_SS

```sql
comment on schema tpcds100 is 'Refresh Function 9: DF_SS';
delete from store_returns where exists (select *
                                          from store_sales, date_dim
                                          where ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number
                                          and ss_sold_date_sk = d_date_sk
                                          and ((d_date between '2000-05-20'::date and '2000-05-21'::date)
                                            or (d_date between '1999-09-18'::date and '1999-09-19'::date)
                                            or (d_date between '2002-11-12'::date and '2002-11-13'::date))
);
delete from store_sales where exists (select *
                                        from date_dim
                                        where ss_sold_date_sk = d_date_sk
                                        and ((d_date between '2000-05-20'::date and '2000-05-21'::date)
                                          or (d_date between '1999-09-18'::date and '1999-09-19'::date)
                                          or (d_date between '2002-11-12'::date and '2002-11-13'::date))
);
```

## Refresh Function 10: DF_WS

```sql
comment on schema tpcds100 is 'Refresh Function 10: DF_WS';
delete from web_returns where exists (select *
                                          from web_sales, date_dim
                                          where ws_item_sk = wr_item_sk and ws_order_number = wr_order_number
                                          and ws_sold_date_sk = d_date_sk
                                          and ((d_date between '2000-05-20'::date and '2000-05-21'::date)
                                            or (d_date between '1999-09-18'::date and '1999-09-19'::date)
                                            or (d_date between '2002-11-12'::date and '2002-11-13'::date))
);
delete from web_sales where exists (select *
                                        from date_dim
                                        where ws_sold_date_sk = d_date_sk
                                        and ((d_date between '2000-05-20'::date and '2000-05-21'::date)
                                          or (d_date between '1999-09-18'::date and '1999-09-19'::date)
                                          or (d_date between '2002-11-12'::date and '2002-11-13'::date))
);
```

## Refresh Function 11: DF_I

```sql
comment on schema tpcds100 is 'Refresh Function 11: DF_I';
delete from inventory where exists (select *
                                        from date_dim
                                        where inv_date_sk = d_date_sk
                                        and ((d_date between '2000-05-18'::date and '2000-05-25'::date)
                                          or (d_date between '1999-09-16'::date and '1999-09-23'::date)
                                          or (d_date between '2002-11-14'::date and '2002-11-21'::date))
);
```
