# Orc Maker
A library for creating Orc files from buffered JSON input.

## Assumptions
Add message assumes JSON input.
S3 uses the date as a partition key when writing.

## Usage
Instantiate `BasicEventDriver` e.g.
```java
Properties props = new Properties();
props.setProperty("event.name", "myEvent");
props.setProperty("event.date", "2019-01-01");
props.setProperty("orcBatchSize", "1000");
props.setProperty("storage.driver", "local");
props.setProperty("storage.local.path", "/tmp/orc");
props.setProperty("statsd.prefix", "myPrefix");
props.setProperty("statsd.host", "localhost");
props.setProperty("statsd.port", "8125");

TypeDescription schema = TypeDescription.fromString("struct<name:string,date:string>");

BasicEventDriver driver = new BasicEventDriver(props, schema);
```

To add events to the orcfile call `addMessage` e.g.
```java
driver.addMessage("{\"name\":\"myName\",\"date\":\"2019-01-01\"}");
```