!hdfs dfs -rm -f -r /tmp/orc_test;
!hdfs dfs -mkdir -p /tmp/orc_test/web_returns_csv;
!hdfs dfs -mkdir -p /tmp/orc_test/web_returns_orc;
!hdfs dfs -copyFromLocal samples/tpcds_web_returns.csv /tmp/orc_test/web_returns_csv;
!hdfs dfs -copyFromLocal tpcds_web_returns.orc /tmp/orc_test/web_returns_orc;

create database if not exists orc_test;
use orc_test;

drop table if exists web_returns_csv;
create external table web_returns_csv
(
    wr_returned_date_sk       int                           ,
    wr_returned_time_sk       int                           ,
    wr_item_sk                int                           ,
    wr_refunded_customer_sk   int                           ,
    wr_refunded_cdemo_sk      int                           ,
    wr_refunded_hdemo_sk      int                           ,
    wr_refunded_addr_sk       int                           ,
    wr_returning_customer_sk  int                           ,
    wr_returning_cdemo_sk     int                           ,
    wr_returning_hdemo_sk     int                           ,
    wr_returning_addr_sk      int                           ,
    wr_web_page_sk            int                           ,
    wr_reason_sk              int                           ,
    wr_order_number           int                           ,
    wr_return_quantity        int                           ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)
)
row format delimited fields terminated by '|'
location '/tmp/orc_test/web_returns_csv';

drop table if exists web_returns_orc;
create external table web_returns_orc
(
    wr_returned_date_sk       int                           ,
    wr_returned_time_sk       int                           ,
    wr_item_sk                int                           ,
    wr_refunded_customer_sk   int                           ,
    wr_refunded_cdemo_sk      int                           ,
    wr_refunded_hdemo_sk      int                           ,
    wr_refunded_addr_sk       int                           ,
    wr_returning_customer_sk  int                           ,
    wr_returning_cdemo_sk     int                           ,
    wr_returning_hdemo_sk     int                           ,
    wr_returning_addr_sk      int                           ,
    wr_web_page_sk            int                           ,
    wr_reason_sk              int                           ,
    wr_order_number           int                           ,
    wr_return_quantity        int                           ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)
)
stored as orc
location '/tmp/orc_test/web_returns_orc';

select count(*) from web_returns_csv;
select count(*) from web_returns_orc;

select avg(wr_fee) from web_returns_csv;
select avg(wr_fee) from web_returns_orc;

select count(*) from web_returns_csv where wr_fee is null;
select count(*) from web_returns_orc where wr_fee is null;

select count(*) from web_returns_csv where wr_refunded_customer_sk is null;
select count(*) from web_returns_orc where wr_refunded_customer_sk is null;

select wr_fee from web_returns_csv limit 10;
select wr_fee from web_returns_orc limit 10;
