package com.mcneilio.orcmaker;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mcneilio.orcmaker.JsonOrcer;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonReaderTest {
    private JsonOrcer jsonReader;
    private TypeDescription schema;

    @BeforeEach
    public void setup() {
        schema = TypeDescription.fromString("struct<x:int,y:int,z:double,a:string,b:boolean,c:date,d:timestamp>");
        String json = "[{\"x\": 1, \"y\": 2, \"z\": 2.5, \"a\": \"test\", \"b\": true, \"c\": \"2022-01-01\", \"d\": \"2022-01-01T00:00:00Z\"}]";
        JsonArray jsonArray = JsonParser.parseString(json).getAsJsonArray();
        Iterator<JsonElement> iterator = jsonArray.iterator();
        jsonReader = new JsonOrcer(iterator, schema);
    }

    @Test
    public void testNextBatch() {
        VectorizedRowBatch batch = schema.createRowBatch();
        boolean result = jsonReader.nextBatch(batch);

        assertEquals(true, result);
        assertEquals(1, batch.size);
        assertEquals(1, ((LongColumnVector) batch.cols[0]).vector[0]);
        assertEquals(2, ((LongColumnVector) batch.cols[1]).vector[0]);
        assertEquals(2.5, ((DoubleColumnVector) batch.cols[2]).vector[0], 0.001);
        assertEquals("test", ((BytesColumnVector) batch.cols[3]).toString(0));
        assertEquals(1, ((LongColumnVector) batch.cols[4]).vector[0]);
        assertEquals(18993, ((LongColumnVector) batch.cols[5]).vector[0]); // 2022-01-01 is the 18628th day since 1970-01-01
        assertEquals(1640995200000L, ((TimestampColumnVector) batch.cols[6]).time[0]); // 2022-01-01T00:00:00Z in milliseconds since 1970-01-01T00:00:00Z
    }
}