package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

public class TimestampOrcer implements Orcer {

            public static TimestampOrcer getInstance() {
                if (instance == null) {
                    instance = new TimestampOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
                if(obj instanceof Long) {
                    timestampColumnVector.time[columnIndex] = (Long)obj;
                    timestampColumnVector.isNull[columnIndex]=false;
                }else{
                    timestampColumnVector.isNull[columnIndex]=true;

                }
            }

            private static TimestampOrcer instance = null;
}
