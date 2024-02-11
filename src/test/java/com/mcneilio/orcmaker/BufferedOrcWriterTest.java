package com.mcneilio.orcmaker;

import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class BufferedOrcWriterTest {
    @Test
    void write() throws Exception{
        BufferedOrcWriter bufferedOrcWriter = new BufferedOrcWriter(TypeDescription.fromString("struct<name:string,age:int,car:string>"), new Path("test-" + timestamp + ".orc"));
        bufferedOrcWriter.write("{\"name\":\"John\",\"age\":30,\"car\":null}");
        bufferedOrcWriter.write("{\"name\":\"Jane\",\"age\":25,\"car\":\"Honda\"}");
        bufferedOrcWriter.flush();
    }

    @Test
    void badJsonMessage() throws Exception {
        BufferedOrcWriter bufferedOrcWriter = new BufferedOrcWriter(TypeDescription.fromString("struct<name:string,age:int,car:string>"), new Path("badJson-" + timestamp + ".orc"));
        bufferedOrcWriter.write("{\"name\":\"John\",\"age\":30,\"car\":null");
        bufferedOrcWriter.write("{\"name\":\"Jane\",\"age\":25,\"car\":\"Honda\"}");
        bufferedOrcWriter.flush();
    }

    @Test
    void mixedJsonMessage() throws Exception {
        BufferedOrcWriter bufferedOrcWriter = new BufferedOrcWriter(TypeDescription.fromString("struct<name:string,age:int,car:string>"), new Path("mixedJson-" + timestamp + ".orc"));
        // The `id` field will be ignored because it is not in the schema
        bufferedOrcWriter.write("{\"id\":99,\"name\":\"John\",\"age\":30,\"car\":null}");
        bufferedOrcWriter.write("{\"name\":\"Jane\",\"age\":25,\"car\":\"Honda\"}");
        bufferedOrcWriter.flush();
    }

    @Test
    void objectInsteadOfString() throws Exception {
        BufferedOrcWriter bufferedOrcWriter = new BufferedOrcWriter(
                TypeDescription.fromString("struct<name:string,age:int,car:string>"),
                new Path("objectInsteadOfString-" + timestamp + ".orc"));
        // The `car` field is expected to be a string, but we provide an object
        bufferedOrcWriter.write("{\"name\":\"John\",\"age\":30,\"car\":{\"model\":\"Honda\",\"year\":2020}}");
        bufferedOrcWriter.write("{\"name\":\"Jane\",\"age\":25,\"car\":\"Toyota\"}");
        bufferedOrcWriter.flush(true);
    }

    private String timestamp = String.valueOf(Instant.now().toEpochMilli());
}