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

## Building
    mvn clean package assembly:single

## End-to-end example (requires make)

This example builds the source, converts a file, copies into HDFS and runs queries against both CSV and ORC to compare outputs.

    make
