package com.example.orc;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class CsvToOrc {
  public static void main(String[] args) throws IOException, InterruptedException {
    // Determine the input and output file names.
    Options options = new Options();
    Option input = new Option("i", "input", true, "input file path");
    input.setRequired(true);
    options.addOption(input);
    Option nullStringOption = new Option("n", "null", true, "null string");
    options.addOption(nullStringOption);
    Option output = new Option("o", "output", true, "output file");
    output.setRequired(true);
    options.addOption(output);
    Option quoteOption = new Option("q", "quote", true, "quote character (default = \")");
    options.addOption(quoteOption);
    Option schema = new Option("s", "schema", true, "schema definition");
    schema.setRequired(true);
    options.addOption(schema);
    Option separatorOption = new Option("sep", "separator", true, "field separator (default = ,)");
    options.addOption(separatorOption);
    Option skipcountOption = new Option("skipcount", true, "number of lines to skip (default = 0)");
    options.addOption(skipcountOption);
    Option strictOption = new Option("strict", false, "fail on extra or missing fields");
    options.addOption(strictOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      formatter.printHelp("CsvToOrc", options);
      System.exit(1);
      return;
    }
    String inputFilePath    = cmd.getOptionValue("input");
    String nullString       = cmd.getOptionValue("null", "");
    String outputFilePath   = cmd.getOptionValue("output");
    String quote            = cmd.getOptionValue("quote", "\"");
    String schemaDefinition = cmd.getOptionValue("schema");
    String separator        = cmd.getOptionValue("separator", ",");
    int skipCount           = Integer.parseInt(cmd.getOptionValue("skipcount", "0"));
    boolean strict          = cmd.hasOption("strict");

    TypeDescription schemaDescription = TypeDescription.fromString(schemaDefinition);
    Configuration conf = new Configuration();
    Writer writer = OrcFile.createWriter(new Path(outputFilePath),
      OrcFile.writerOptions(conf).setSchema(schemaDescription));

    CSVReader reader = new CSVReader(new FileReader(inputFilePath), separator.charAt(0), quote.charAt(0));
    String[] nextLine;
    VectorizedRowBatch batch = schemaDescription.createRowBatch();
    final int BATCH_SIZE = batch.getMaxSize();

    // Skip lines, if needed.
    int lineNumber = 0;
    while (lineNumber < skipCount) {
      reader.readNext();
      lineNumber++;
    }

    // Read the CSV rows and place them into the column vectors.
    while ((nextLine = reader.readNext()) != null) {
      lineNumber++;
      for (int j = 0; j < nextLine.length; j++) {
        if (j >= batch.cols.length) {
          if (strict) {
            System.out.println(String.format("Too many columns on line %d and strict mode is on", lineNumber));
            System.exit(1);
          } else {
            break;
          }
        }
        if (batch.cols[j] instanceof BytesColumnVector) {
          if (nullString.equals(nextLine[j])) {
            batch.cols[j].isNull[batch.size] = true;
            batch.cols[j].noNulls = false;
          } else {
            ((BytesColumnVector) batch.cols[j]).setVal(batch.size, nextLine[j].getBytes());
          }
        } else if (batch.cols[j] instanceof DecimalColumnVector) {
          if (nullString.equals(nextLine[j])) {
            batch.cols[j].isNull[batch.size] = true;
            batch.cols[j].noNulls = false;
          } else {
            ((DecimalColumnVector) batch.cols[j]).vector[batch.size] = new HiveDecimalWritable(nextLine[j]);
          }
        } else if (batch.cols[j] instanceof DoubleColumnVector) {
          if (nullString.equals(nextLine[j])) {
            batch.cols[j].isNull[batch.size] = true;
            batch.cols[j].noNulls = false;
          } else {
            ((DoubleColumnVector) batch.cols[j]).vector[batch.size] = Double.parseDouble(nextLine[j]);
          }
        } else if (batch.cols[j] instanceof LongColumnVector) {
          if (nullString.equals(nextLine[j])) {
            batch.cols[j].isNull[batch.size] = true;
            batch.cols[j].noNulls = false;
          } else {
            ((LongColumnVector) batch.cols[j]).vector[batch.size] = Long.parseLong(nextLine[j]);
          }
        } else if (batch.cols[j] instanceof TimestampColumnVector) {
          if (nullString.equals(nextLine[j])) {
            batch.cols[j].isNull[batch.size] = true;
            batch.cols[j].noNulls = false;
          } else {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
            final ZonedDateTime dateTime = ZonedDateTime.parse(nextLine[j], dateTimeFormatter.withZone(ZoneId.of("UTC")));
            ((TimestampColumnVector) batch.cols[j]).set(batch.size, new Timestamp(dateTime.toInstant().toEpochMilli()));
          }
        }
      }

      // Fill missing fields with nulls.
      for (int j = nextLine.length; j < batch.cols.length; j++) {
        if (strict) {
          System.out.println(String.format("Missing fields on line %d and strict mode is on", lineNumber));
          System.exit(1);
        } else {
          batch.cols[j].isNull[batch.size] = true;
          batch.cols[j].noNulls = false;
        }
      }

      // Check the batch size.
      batch.size++;
      if (batch.size == BATCH_SIZE) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size > 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
  }
}
