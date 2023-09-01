package com.mcneilio.orcmaker.columns;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.orcer.OrcerFactory;
import com.mcneilio.orcmaker.orcer.json.*;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.Arrays;
import java.util.HashMap;

public class ColumnInitializer {

  public static void initColumns(HashMap<String, ColumnVector> columns, ColumnVector[] columnVectors, String[] colNames, TypeDescription schema, Orcer[] orcers) {

    for(int i = 0; i < colNames.length; i++) {
      columns.put(colNames[i], columnVectors[i]);
      TypeDescription typeDescription = schema.getChildren().get(i);
      orcers[i] = OrcerFactory.getOrcer(typeDescription.toString());
    }
  }

  public static void nullColumns(HashMap<String, ColumnVector> columns) {
    /*
      array and timestamp columnVectors don't provide fillWithNulls
      array and timestamp columnVectors appear to work with NULL values
    */
    columns.forEach( (key, value) -> {
      value.noNulls = false;

      if (value instanceof LongColumnVector) {
        Arrays.fill(((LongColumnVector) value).vector, LongColumnVector.NULL_VALUE);
        Arrays.fill(value.isNull, true);
      }
      else if (value instanceof BytesColumnVector) {
        Arrays.fill(((BytesColumnVector) value).vector, null);
        Arrays.fill(value.isNull, true);
      }
    });
  }

}
