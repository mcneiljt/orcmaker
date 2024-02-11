# Orc Maker
A library for creating Orc files from buffered JSON input.

## Usage
Instantiate `BasicEventDriver` e.g.
```java
BufferedOrcWriter bufferedOrcWriter = new BufferedOrcWriter(TypeDescription.fromString("struct<name:string,age:int,car:string>"), new Path("test-" + timestamp + ".orc"));
```

To add events to the orcfile call `addMessage` e.g.
```java
bufferedOrcWriter.write("{\"name\":\"John\",\"age\":30,\"car\":null}");
bufferedOrcWriter.write("{\"name\":\"Jane\",\"age\":25,\"car\":\"Honda\"}");
```

Finally flush the buffer to the orcfile with `flush` e.g.
```java
bufferedOrcWriter.flush();
```