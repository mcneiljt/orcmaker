package com.mcneilio.orcmaker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.mcneilio.orcmaker.storage.LocalStorageDriver;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.TypeDescription;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class BasicEventDriver2Test {

  private BasicEventDriver2 driver;
  private TypeDescription schema;

  @BeforeEach
  void setUp() {
    Properties config = new Properties();
    config.setProperty("event.name", "test_event");
    config.setProperty("event.date", "2023-08-27");
    config.setProperty("storage.driver", "local");
    config.setProperty("storage.local.path", "/tmp/orc");

    schema = TypeDescription.fromString(
      "struct<name:string,flag:boolean,bignum:bigint,num:int,tinynum:tinyint,date:date>"
    );

    LocalStorageDriver storageDriver = new LocalStorageDriver(config);

    driver = new BasicEventDriver2(config, schema, storageDriver);

  }

  @Test
  void testConstructor() {
    assertEquals("test_event", driver.eventName);
    assertEquals("2023-08-27", driver.eventDate);
    assertNotNull(driver.batch);
    assertEquals(schema.getFieldNames().size(), driver.batch.numCols);
    assertEquals(driver.orcers.length, schema.getFieldNames().size());
  }

  @Test
  void testAddSingleMessage() {
    JSONObject message = new JSONObject();
    message.put("name", "nameval");
    message.put("flag", true);
    message.put("bignum", 12345678912345L);
    message.put("num", 12345L);
    message.put("tinynum", 123L);
    message.put("date", "2023-01-01");

    driver.addMessage(message);

    assertEquals(1, driver.batch.size);
    assertEquals("nameval", new String(((BytesColumnVector) driver.columns.get("name")).vector[0]));
    assertEquals(1L, ((LongColumnVector) driver.columns.get("flag")).vector[0]);
    assertEquals(12345L, ((LongColumnVector) driver.columns.get("num")).vector[0]);
    assertEquals(123L, ((LongColumnVector) driver.columns.get("tinynum")).vector[0]);
    assertEquals(12345678912345L, ((LongColumnVector) driver.columns.get("bignum")).vector[0]);
    assertEquals(19596L, ((LongColumnVector) driver.columns.get("date")).vector[0]);
  }

//  @Test
//  void testFlush() throws IOException {
//    Properties config = new Properties();
//    config.setProperty("event.name", "test_event");
//    config.setProperty("event.date", "2023-08-27");
//    TypeDescription typeDescription = new TypeDescription();
//    StorageDriver storageDriver = new StorageDriver();
//    StatsDClient statsd = new StatsDClient();
//    BasicEventDriver2 driver = new BasicEventDriver2(config, typeDescription, storageDriver, statsd);
//    JSONObject message1 = new JSONObject();
//    message1.put("key1", "value1");
//    message1.put("key2", 1234);
//    driver.addMessage(message1);
//    driver.flush(true);
//    assertTrue(new File(driver.fileName).delete());
//  }
}
