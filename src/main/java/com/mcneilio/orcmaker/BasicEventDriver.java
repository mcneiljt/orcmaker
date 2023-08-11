package com.mcneilio.shokuyoku.driver;

import com.example.orcmaker.EventDriver;
import com.example.orcmaker.orcer.Orcer;
import com.example.orcmaker.orcer.json.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

/**
 * BasicEventDriver takes raw JSONObjects and adds them to an ORC file. On an interval or if a certain size
 * is reached, the ORC file is sent to the storageDriver.
 */
public class BasicEventDriver implements EventDriver {

    private interface JSONToOrc {
        void addObject(ColumnVector columnVector, int idx, Object obj);
    }

    public BasicEventDriver(String eventName, String date, TypeDescription typeDescription, StorageDriver storageDriver) {
        this.eventName = eventName;
        this.date = date;
        this.storageDriver = storageDriver;
        this.schema = typeDescription;

        // TODO This env vars should probably be pulled out.
        this.batch = this.schema.createRowBatch(System.getenv("ORC_BATCH_SIZE") != null ? Integer.parseInt(System.getenv("ORC_BATCH_SIZE")) : 1000);
        setColumns();
        nullColumns();
        this.statsd = Statsd.getInstance();
    }

    private Orcer[] orcers;

    @Override
    public void addMessage(JSONObject msg2) {
        long t = Instant.now().toEpochMilli();
        int batchPosition = batch.size++;

        for(int colId =0;colId<colNames.length;colId++){
            String key=colNames[colId];

            if(!msg2.has(key)) {
                continue;
            }
            Object value = msg2.get(key);

            if(orcers[colId]!=null){
                orcers[colId].addObject(columnVectors[colId], batchPosition, value);
            }
        }
        ((LongColumnVector) columns.get("date")).vector[batchPosition] = LocalDate.parse(date).toEpochDay();
        columns.get("date").isNull[batchPosition] = false;
        statsd.count("message.count", 1, new String[]{"env:"+System.getenv("STATSD_ENV")});
        if (batch.size == batch.getMaxSize()) {
            write();
        }
        statsd.histogram("eventDriver.addMessage.ms", Instant.now().toEpochMilli() - t,
                new String[] {"env:"+System.getenv("STATSD_ENV")});
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
                    storageDriver.addFile(date,  eventName, fileName, Paths.get(fileName));
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
                new String[] {"env:"+System.getenv("STATSD_ENV")});
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
            nullColumns();
        }
        catch (IOException e) {
            System.out.println("Error writing orc file");
            e.printStackTrace();
        }

        statsd.histogram("eventDriver.write.ms", Instant.now().toEpochMilli() - t,
                new String[] {"env:"+System.getenv("STATSD_ENV")});
    }

    private void nullColumns() {
        columns.forEach( (key, value) -> {
            value.noNulls = false;

            if(value instanceof LongColumnVector) {
                Arrays.fill(((LongColumnVector) value).vector, LongColumnVector.NULL_VALUE);
                Arrays.fill(value.isNull, true);
            }
            else if(value instanceof BytesColumnVector) {
                Arrays.fill(((BytesColumnVector) value).vector, null);
                Arrays.fill(value.isNull, true);
            }
            //array and timestamp columnVectors don't provide fillWithNulls
            //array and timestamp columnVectors appear to work with null values
        });
    }

    /**
     * The goal here is to tie column vectors to the keys, so they can be easily referenced
     *
     */
    private void setColumns() {
        HashMap<String, ColumnVector> columns = new HashMap<>();
        colNames = schema.getFieldNames().toArray(new String[0]);
        columnVectors = this.batch.cols;
        orcers = new Orcer[batch.numCols];

        for(int i = 0; i < colNames.length; i++) {
            columns.put(colNames[i], columnVectors[i]);

            TypeDescription typeDescription =schema.getChildren().get(i);
            if(typeDescription.toString().equals("date")) {
                orcers[i] = DateOrcer.getInstance();
            } else if(typeDescription.toString().equals("string")) {
                orcers[i] = StringOrcer.getInstance();
            }else if(typeDescription.toString().equals("boolean")) {
                orcers[i] = BooleanOrcer.getInstance();
            } else if( typeDescription.toString().equals("smallint")) {
                orcers[i] = SmallIntOrcer.getInstance();
            } else if(typeDescription.toString().equals("tinyint") ) {
                orcers[i] = TinyIntOrcer.getInstance();
            } else if(typeDescription.toString().equals("int")) {
                orcers[i] = IntOrcer.getInstance();
            } else if( typeDescription.toString().equals("bigint")) {
                orcers[i] = BigIntOrcer.getInstance();
            }   else if(typeDescription.toString().startsWith("decimal(")) {
                orcers[i] = DecimalOrcer.getInstance();
            } else if(typeDescription.toString().equals("double") || typeDescription.toString().equals("float")) {
                orcers[i] = DoubleOrcer.getInstance();
            } else if(typeDescription.toString().equals("binary")) {
                orcers[i] = BinaryOrcer.getInstance();
            }  else if(typeDescription.toString().equals("timestamp")) {
                orcers[i] = TimestampOrcer.getInstance();
            }else if(typeDescription.toString().equals("array<boolean>")) {
                orcers[i] = ArrayBooleanOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<float>")) {
                orcers[i] = ArrayFloatOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<date>")) {
                orcers[i] = ArrayDateOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<string>")) {
                orcers[i] = ArrayStringOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<tinyint>")) {
                orcers[i] = ArrayTinyIntOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<double>")) {
                orcers[i] = ArrayDoubleOrcer.getInstance();
            } else if(typeDescription.toString().startsWith("array<decimal>")) {
                orcers[i] = ArrayDecimalOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<int>")) {
                orcers[i] = ArrayIntOrcer.getInstance();
            }  else if(typeDescription.toString().equals("array<timestamp>")) {
                orcers[i] = ArrayTimestampOrcer.getInstance();
            } else if(typeDescription.toString().equals("array<smallint>")) {
                orcers[i] = ArraySmallIntOrcer.getInstance();
            }  else if(typeDescription.toString().equals("array<bigint>")) {
                orcers[i] = ArrayBigIntOrcer.getInstance();
            }   else {
                System.out.println("Unsupported Column Type: " + typeDescription.toString());
            }
        }
        this.columns = columns;
    }

    public String getFileName(){
        return fileName;
    }

    String[] colNames;
    VectorizedRowBatch batch;
    TypeDescription schema;
    HashMap<String, ColumnVector> columns;
    String eventName, fileName, date;
    Configuration conf = new Configuration();
    Writer writer = null;
    StatsDClient statsd;
    StorageDriver storageDriver;
    ColumnVector[] columnVectors;
}
