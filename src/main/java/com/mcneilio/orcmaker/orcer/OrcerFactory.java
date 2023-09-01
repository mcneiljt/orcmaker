package com.mcneilio.orcmaker.orcer;

import com.mcneilio.orcmaker.orcer.json.*;

public class OrcerFactory {

  public static Orcer getOrcer(String type) {
    return switch (type) {
      case "date" -> DateOrcer.getInstance();
      case "string" -> StringOrcer.getInstance();
      case "boolean" -> BooleanOrcer.getInstance();
      case "smallint" -> SmallIntOrcer.getInstance();
      case "tinyint" -> TinyIntOrcer.getInstance();
      case "int" -> IntOrcer.getInstance();
      case "bigint" -> BigIntOrcer.getInstance();
      case "decimal(" -> DecimalOrcer.getInstance(); // does the '(' belong in there?
      case "double", "float" -> DoubleOrcer.getInstance();
      case "binary" -> BinaryOrcer.getInstance();
      case "timestamp" -> TimestampOrcer.getInstance();
      case "array<bigint>" -> ArrayBigIntOrcer.getInstance();
      case "array<boolean>" -> ArrayBooleanOrcer.getInstance();
      case "array<date>" -> ArrayDateOrcer.getInstance();
      case "array<decimal>" -> ArrayDecimalOrcer.getInstance();
      case "array<double>" -> ArrayDoubleOrcer.getInstance();
      case "array<float>" -> ArrayFloatOrcer.getInstance();
      case "array<int>" -> ArrayIntOrcer.getInstance();
      case "array<string>" -> ArrayStringOrcer.getInstance();
      case "array<tinyint>" -> ArrayTinyIntOrcer.getInstance();
      case "array<timestamp>" -> ArrayTimestampOrcer.getInstance();
      case "array<smallint>" -> ArraySmallIntOrcer.getInstance();
      default -> null; // should this throw?
    };
  }
}
