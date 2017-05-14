all: tpcds_web_returns.orc
	hive -f load_hive.sql

target/csv-to-orc-1.0-SNAPSHOT-jar-with-dependencies.jar: src/main/java/com/example/orc/CsvToOrc.java
	mvn clean package assembly:single

tpcds_web_returns.orc: target/csv-to-orc-1.0-SNAPSHOT-jar-with-dependencies.jar
	java -jar target/csv-to-orc-1.0-SNAPSHOT-jar-with-dependencies.jar -i samples/tpcds_web_returns.csv -o tpcds_web_returns.orc -s 'struct<wr_returned_date_sk:int,wr_returned_time_sk:int,wr_item_sk:int,wr_refunded_customer_sk:int,wr_refunded_cdemo_sk:int,wr_refunded_hdemo_sk:int,wr_refunded_addr_sk:int,wr_returning_customer_sk:int,wr_returning_cdemo_sk:int,wr_returning_hdemo_sk:int,wr_returning_addr_sk:int,wr_web_page_sk:int,wr_reason_sk:int,wr_order_number:int,wr_return_quantity:int,wr_return_amt:decimal(7,2),wr_return_tax:decimal(7,2),wr_return_amt_inc_tax:decimal(7,2),wr_fee:decimal(7,2),wr_return_ship_cost:decimal(7,2),wr_refunded_cash:decimal(7,2),wr_reversed_charge:decimal(7,2),wr_account_credit:decimal(7,2),wr_net_loss:decimal(7,2)>' -sep '|' -skipcount 0
