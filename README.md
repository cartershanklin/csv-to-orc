# Convert a CSV fle to ORCFile

## Using the standalone JAR
    java -jar bin/csv-to-orc.jar

Supported options:

    usage: CsvToOrc
     -i,--input <arg>         input file path
     -n,--null <arg>          null string
     -o,--output <arg>        output file
     -q,--quote <arg>         quote character (default = ")
     -s,--schema <arg>        schema definition
     -sep,--separator <arg>   field separator (default = ,)
     -skipcount <arg>         number of lines to skip (default = 0)
     -strict                  fail on extra or missing fields

## Example

A schema argument must be provided. Construction of the argument is similar to Hive DDL. Example;

    java
      -jar bin/csv-to-orc.jar
      -i samples/tpcds_web_returns.csv
      -o tpcds_web_returns.orc
      -s 'struct<wr_returned_date_sk:int,wr_returned_time_sk:int,wr_item_sk:int,wr_refunded_customer_sk:int,wr_refunded_cdemo_sk:int,wr_refunded_hdemo_sk:int,wr_refunded_addr_sk:int,wr_returning_customer_sk:int,wr_returning_cdemo_sk:int,wr_returning_hdemo_sk:int,wr_returning_addr_sk:int,wr_web_page_sk:int,wr_reason_sk:int,wr_order_number:int,wr_return_quantity:int,wr_return_amt:decimal(7,2),wr_return_tax:decimal(7,2),wr_return_amt_inc_tax:decimal(7,2),wr_fee:decimal(7,2),wr_return_ship_cost:decimal(7,2),wr_refunded_cash:decimal(7,2),wr_reversed_charge:decimal(7,2),wr_account_credit:decimal(7,2),wr_net_loss:decimal(7,2)>'
      -sep '|'
      -skipcount 0

## Building
    mvn clean package assembly:single

## End-to-end example (requires make)

This example builds the source, converts a file, copies into HDFS and runs queries against both CSV and ORC to compare outputs.

    make
