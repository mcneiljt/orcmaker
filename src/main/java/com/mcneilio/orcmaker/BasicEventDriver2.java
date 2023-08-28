package com.mcneilio.orcmaker;

import com.mcneilio.orcmaker.columns.ColumnInitializer;
import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.storage.StorageDriver;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;


public class BasicEventDriver2 implements EventDriver {
  Orcer[] orcers;

  String[] colNames;
  VectorizedRowBatch batch;
  TypeDescription schema;
  HashMap<String, ColumnVector> columns;
  String eventName;
  String eventDate;
  String fileName;
  String statsdEnv;
  Configuration conf = new Configuration();
  Writer writer = null;
  StatsDClient statsd;
  StorageDriver storageDriver;
  ColumnVector[] columnVectors;


  public BasicEventDriver2(Properties config, TypeDescription typeDescription, StorageDriver storageDriver, StatsDClient statsd) {
    this.eventName = config.getProperty("event.name");
    this.eventDate = config.getProperty("event.date");

    this.schema = typeDescription;
    this.storageDriver = storageDriver;
    this.statsd = statsd;

    this.batch = this.schema.createRowBatch(
      Integer.parseInt(
        config.getProperty("orcBatchSize", "1024")
      )
    );

    this.columnVectors = this.batch.cols;
    this.colNames = schema.getFieldNames().toArray(new String[0]);
    this.orcers = new Orcer[this.batch.numCols];
    this.columns = new HashMap<>();

    ColumnInitializer.initColumns(this.columns, this.columnVectors, this.colNames, this.schema, this.orcers);
    ColumnInitializer.nullColumns(this.columns);
  }

  @Override
  public void addMessage(JSONObject message) {
    long t = Instant.now().toEpochMilli();
    int batchPosition = batch.size++;

    for(int colId =0;colId<colNames.length;colId++){
      String key=colNames[colId];

      if(!message.has(key)) {
        continue;
      }
      Object value = message.get(key);

      if(orcers[colId]!=null){
        orcers[colId].addObject(columnVectors[colId], batchPosition, value);
      }
    }
    ((LongColumnVector) columns.get("date")).vector[batchPosition] = LocalDate.parse(eventDate).toEpochDay(); // should date be mandatory? can it be configurable?
    columns.get("date").isNull[batchPosition] = false;
//    statsd.count("message.count", 1, "env:"+ statsdEnv);
    if (batch.size == batch.getMaxSize()) {
      write();
    }
//    statsd.histogram("eventDriver.addMessage.ms", Instant.now().toEpochMilli() - t, "env:"+statsdEnv);
  }

  @Override
  public String flush(boolean deleteFile) {
    long t = Instant.now().toEpochMilli();
    if (batch.size != 0) {
      write();
    }
    String writtenFileName = this.fileName;

    if(writer != null) {
      System.out.println("Flushing: " + this.eventName);
      try {
        writer.close();
        writer = null;
        if (storageDriver != null){
          storageDriver.addFile(eventDate,  eventName, fileName, Paths.get(fileName));
        }
        //TODO: create hive partition
        if(deleteFile)
          new File(fileName).delete();
        this.fileName = null;
      }
      catch (IOException e) {
        System.out.println("Error closing orc file: " + e);
      }
    }
    statsd.histogram("eventDriver.flush.ms", Instant.now().toEpochMilli() - t,
      "env:"+statsdEnv);
    return writtenFileName;
  }

  private void write() {
    long t = Instant.now().toEpochMilli();
    try {
      if(this.writer == null) {
        this.fileName = this.eventName + "_" + Instant.now().toEpochMilli() + "_" + UUID.randomUUID() + ".orc";
        this.writer = OrcFile.createWriter(new Path(fileName),
          OrcFile.writerOptions(conf).setSchema(this.schema));
      }
      this.writer.addRowBatch(batch);
      batch.reset();
      ColumnInitializer.nullColumns(columns);
    }
    catch (IOException e) {
      System.out.println("Error writing orc file");
      e.printStackTrace();
    }

    statsd.histogram("eventDriver.write.ms", Instant.now().toEpochMilli() - t,
      "env:"+statsdEnv);
  }
}
