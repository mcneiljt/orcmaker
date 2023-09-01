package com.mcneilio.orcmaker.columns;

import static org.mockito.Mockito.*;

import com.mcneilio.orcmaker.BasicEventDriver2;
import com.mcneilio.orcmaker.storage.StorageDriver;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

public class ColumnInitializerTest {

//  @Test
//  public void testNullColumns() {
//
//    // Mock column vectors
//    ColumnVector col1 = mock(LongColumnVector.class);
//    ColumnVector col2 = mock(BytesColumnVector.class);
//
//    // Mock batch and schema
//    VectorizedRowBatch batch = mock(VectorizedRowBatch.class);
//    when(batch.cols).thenReturn(new ColumnVector[] {col1, col2});
//
//    TypeDescription schema = mock(TypeDescription.class);
//    when(schema.getFieldNames()).thenReturn(List.of(new String[]{"col1", "col2"}));
//
//    // Initialize driver with mocks
//    BasicEventDriver2 driver = new BasicEventDriver2(
//      mock(Properties.class),
//      schema,
//      mock(StorageDriver.class),
//      mock(StatsDClient.class)
//    );
//
//    // Assert null columns
////    verify(col1).noNulls = false;
////    verify(col1.vector).fill(LongColumnVector.NULL_VALUE);
////    verify(col1.isNull).fill(true);
////
////    verify(col2).noNulls = false;
////    verify(col2.vector).fill(null);
////    verify(col2.isNull).fill(true);
//
//  }
}
