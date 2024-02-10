package com.mcneilio.orcmaker;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mcneilio.orcmaker.orcer.JsonReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class BufferedOrcWriter {

    /**
     * Instantiate a new BufferedOrcWriter
     */
    public BufferedOrcWriter(TypeDescription schema, Path path) {
        this.schema = schema;
        this.batch = schema.createRowBatch();
        this.path = path;
        this.writerConfiguration = new Configuration();
        this.dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    }

    public BufferedOrcWriter(TypeDescription schema, Path path, Configuration writerConfiguration) {
        this.schema = schema;
        this.batch = schema.createRowBatch();
        this.path = path;
        this.writerConfiguration = writerConfiguration;
        this.dateTimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
    }

    public BufferedOrcWriter(TypeDescription schema, Path path, Configuration writerConfiguration, DateTimeFormatter dateTimeFormatter) {
        this.schema = schema;
        this.batch = schema.createRowBatch();
        this.path = path;
        this.writerConfiguration = writerConfiguration;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    /**
     * Write a message to the buffer
     * @param message The message to write
     */
    public void write(String message) {
        JsonElement element;
        try {
            element = JsonParser.parseString(message);
            buffer.add(element);
        } catch (Exception e) {
            System.out.println("Invalid JSON; continuing to avoid data loss.");
            e.printStackTrace();
        }

    }

    /**
     * Flush the buffer to the storage driver
     */
    public void flush() throws IOException {
        OrcFile.WriterOptions writerOpts = OrcFile.writerOptions(writerConfiguration)
                .setSchema(schema);
        Writer writer = OrcFile.createWriter(path, writerOpts);
        JsonReader reader = new JsonReader(buffer.iterator(), schema, dateTimeFormatter);
        while (reader.nextBatch(batch)) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }

    ArrayList<JsonElement> buffer = new ArrayList<JsonElement>();
    TypeDescription schema;
    VectorizedRowBatch batch;
    Path path;
    Configuration writerConfiguration;
    DateTimeFormatter dateTimeFormatter;
}
