package com.mcneilio.orcmaker;

import org.apache.hadoop.fs.Path;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

class BufferedOrcWriterTest {
    @Test
    void write() throws Exception{
        BufferedOrcWriter bufferedOrcWriter = new BufferedOrcWriter(TypeDescription.fromString("struct<name:string,age:int,car:string>"), new Path("test.orc"));
        bufferedOrcWriter.write("{\"name\":\"John\",\"age\":30,\"car\":null}");
        bufferedOrcWriter.write("{\"name\":\"Jane\",\"age\":25,\"car\":\"Honda\"}");
        bufferedOrcWriter.flush();
    }
}