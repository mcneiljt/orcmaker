package com.mcneilio.orcmaker;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class BufferedOrcWriter {

    public BufferedOrcWriter(TypeDescription schema, Path path) {
        this(schema, path, new Configuration());
    }

    public BufferedOrcWriter(TypeDescription schema, Path path, Configuration writerConfiguration) {
        this(schema, path, writerConfiguration, DateTimeFormatter.ISO_DATE_TIME);
    }

    public BufferedOrcWriter(TypeDescription schema, Path path, DateTimeFormatter dateTimeFormatter) {
        this(schema, path, new Configuration(), dateTimeFormatter);
    }

    public BufferedOrcWriter(TypeDescription schema, Path path, Configuration writerConfiguration, DateTimeFormatter dateTimeFormatter) {
        this.schema = schema;
        this.batch = schema.createRowBatch();
        this.path = path;
        this.writerConfiguration = writerConfiguration;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    /**
     * Write a string to the buffer after converting it to json
     * If you have already converted to json it will be faster to use the write(JsonElement) method
     * @param message Message to convert to json and write to the buffer
     */
    public void write(String message) {
        JsonElement element;
        try {
            element = JsonParser.parseString(message);
            write(element);
        } catch (Exception e) {
            System.out.println("Invalid JSON; continuing to avoid data loss.");
            e.printStackTrace();
        }
    }

    /**
     * Write a JsonElement to the buffer
     * @param element JsonElement to write to the buffer
     */
    public void write(JsonElement element) {
        buffer.add(element);
    }

    /**
     * Flush the buffer to the configured path
     * @param allowStringify Allow the writer to stringify non-string fields
     * @throws IOException If there is an error writing to the file
     */
    public void flush(boolean allowStringify) throws IOException {
        OrcFile.WriterOptions writerOpts = OrcFile.writerOptions(writerConfiguration)
                .setSchema(schema);
        Writer writer = OrcFile.createWriter(path, writerOpts);
        JsonOrcer reader = new JsonOrcer(buffer.iterator(), schema, dateTimeFormatter, allowStringify);
        while (reader.nextBatch(batch)) {
            writer.addRowBatch(batch);
        }
        writer.close();
    }

    /**
     * Flush the buffer to the configured path
     * If you would like to allow the writer to stringify non-string fields, use the flush(true) method
     * @throws IOException If there is an error writing to the file
     */
    public void flush() throws IOException {
        flush(false);
    }

    ArrayList<JsonElement> buffer = new ArrayList<JsonElement>();
    TypeDescription schema;
    VectorizedRowBatch batch;
    Path path;
    Configuration writerConfiguration;
    DateTimeFormatter dateTimeFormatter;
}
