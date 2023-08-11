package com.mcneilio.orcmaker;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.orcer.json.*;
import com.mcneilio.orcmaker.storage.LocalStorageDriver;
import com.mcneilio.orcmaker.storage.S3StorageDriver;
import com.mcneilio.orcmaker.storage.StorageDriver;
import com.mcneilio.orcmaker.utils.Statsd;
import com.timgroup.statsd.StatsDClient;
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
import java.util.Properties;
import java.util.UUID;

/**
 * BasicEventDriver takes raw JSONObjects and adds them to an ORC file. On an interval or if a certain size
 * is reached, the ORC file is sent to the storageDriver.
 */
public class BasicEventDriver implements EventDriver {
    /**
     * Initializes an EventDriver designed to take JSONObjects and add them to an ORC file.
     * Available properties for the config:
     * <ul>
     *     <li>event.name - Name of the event</li>
     *     <li>event.date - Date of the event</li>
     *     <li>orcBatchSize - Size of the ORC batch</li>
     *     <li>storage.driver - Type of storage driver to use (s3 or local)</li>
     *     <li>storage.s3.bucket - Bucket to use for S3 storage driver</li>
     *     <li>storage.s3.prefix - Prefix to use for S3 storage driver</li>
     *     <li>storage.s3.region - Region to use for S3 storage driver</li>
     *     <li>storage.local.path - Path to use for local storage driver</li>
     *     <li>statsd.prefix - Prefix to use for statsd</li>
     *     <li>statsd.host - Host to use for statsd</li>
     *     <li>statsd.port - Port to use for statsd</li>
     *     <li>statsd.flushMS - Flush interval to use for statsd</li>
     *     <li>statsd.env - Environment to use for statsd</li>
     * </ul>
     *
     * @param config         Configuration properties
     * @param typeDescription TypeDescription of the ORC file
     */
    public BasicEventDriver(Properties config, TypeDescription typeDescription) {
        this.eventName = config.getProperty("event.name");
        this.date = config.getProperty("event.date");
        this.statsdEnv = config.getProperty("statsd.env");

        // Initialize storage driver
        String storageDriverType = config.getProperty("storage.driver");
        if (storageDriverType.equals("s3")) {
            this.storageDriver = new S3StorageDriver(config);
        } else if (storageDriverType.equals("local")) {
            this.storageDriver = new LocalStorageDriver(config);
        } else {
            throw new RuntimeException("Unknown storage driver type: " + storageDriverType);
        }

        // Initialize batch
        this.schema = typeDescription;
        this.batch = this.schema.createRowBatch(Integer.parseInt(config.getProperty("orcBatchSize", "1000")));

        // Initialize columns
        setColumns();
        nullColumns();

        // Initialize statsd
        if (config.getProperty("statsd.prefix") == null || config.getProperty("statsd.host") == null || config.getProperty("statsd.port") == null)
            throw new RuntimeException("Missing statsd configuration");
        Statsd.createInstance(config.getProperty("statsd.prefix"), config.getProperty("statsd.host"), Integer.parseInt(config.getProperty("statsd.port")),
                Integer.parseInt(config.getProperty("statsd.flushMS", "1000")));
        this.statsd = Statsd.getInstance();
    }


    private Orcer[] orcers;

    /**
     * Adds a JSONObject to the ORC batch. If batch size is reached, the batch is written to the ORC file.
     * Assumes date is a column of the ORC file and is set to the date of processing.
     */
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
        ((LongColumnVector) columns.get("date")).vector[batchPosition] = LocalDate.parse(date).toEpochDay();
        columns.get("date").isNull[batchPosition] = false;
        statsd.count("message.count", 1, new String[]{"env:"+ statsdEnv});
        if (batch.size == batch.getMaxSize()) {
            write();
        }
        statsd.histogram("eventDriver.addMessage.ms", Instant.now().toEpochMilli() - t,
                new String[] {"env:"+statsdEnv});
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
                new String[] {"env:"+statsdEnv});
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
                new String[] {"env:"+statsdEnv});
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

    String[] colNames;
    VectorizedRowBatch batch;
    TypeDescription schema;
    HashMap<String, ColumnVector> columns;
    String eventName, date, statsdEnv, fileName;
    Configuration conf = new Configuration();
    Writer writer = null;
    StatsDClient statsd;
    StorageDriver storageDriver;
    ColumnVector[] columnVectors;
    Properties props;
}
