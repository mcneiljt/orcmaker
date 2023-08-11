package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

public class BinaryOrcer implements Orcer {

        public static BinaryOrcer getInstance() {
            if (instance == null) {
                instance = new BinaryOrcer();
            }
            return instance;
        }
        @Override
        public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
            bytesColumnVector.isNull[columnIndex]=true;
        }

        private static BinaryOrcer instance = null;
}
