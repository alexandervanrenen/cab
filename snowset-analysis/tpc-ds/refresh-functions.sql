-- Refresh Function 1: LF_CR ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 1: LF_CR';
copy into s_catalog_returns from @~/staged/s_catalog_returns_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into catalog_returns (select * from crv);

-- Refresh Function 2: LF_CS ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 2: LF_CS';
copy into s_catalog_order from @~/staged/s_catalog_order_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into s_catalog_order_lineitem from @~/staged/s_catalog_order_lineitem_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into catalog_sales (select * from csv);

-- Refresh Function 3: LF_I ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 3: LF_I';
copy into s_inventory from @~/staged/s_inventory_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into inventory (select * from iv);

-- Refresh Function 4: LF_SR ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 4: LF_SR';
copy into s_store_returns from @~/staged/s_store_returns_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into store_returns (select * from srv);

-- Refresh Function 5: LF_SS ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 5: LF_SS';
copy into s_purchase from @~/staged/s_purchase_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into s_purchase_lineitem from @~/staged/s_purchase_lineitem_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into store_sales (select * from ssv);

-- Refresh Function 6: LF_WR ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 6: LF_WR';
copy into s_web_returns from @~/staged/s_web_returns_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into web_returns (select * from wrv);

-- Refresh Function 7: LF_WS ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 7: LF_WS';
copy into s_web_order from @~/staged/s_web_order_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
copy into s_web_order_lineitem from @~/staged/s_web_order_lineitem_1.dat file_format = (type = csv field_delimiter = '|' error_on_column_count_mismatch=false VALIDATE_UTF8=false skip_header = 0)  FORCE = TRUE;
insert into web_sales (select * from wsv);

-- Refresh Function 8: DF_CS ------------------------------------------------------------------------------------------

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

-- Refresh Function 9: DF_SS ------------------------------------------------------------------------------------------

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

-- Refresh Function 10: DF_WS ------------------------------------------------------------------------------------------

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

-- Refresh Function 11: DF_I ------------------------------------------------------------------------------------------

comment on schema tpcds100 is 'Refresh Function 11: DF_I';
delete from inventory where exists (select *
                                        from date_dim
                                        where inv_date_sk = d_date_sk
                                        and ((d_date between '2000-05-18'::date and '2000-05-25'::date)
                                          or (d_date between '1999-09-16'::date and '1999-09-23'::date)
                                          or (d_date between '2002-11-14'::date and '2002-11-21'::date))
);
