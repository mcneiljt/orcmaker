package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.utils.Helpers;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

public class StringOrcer implements Orcer {

        public static StringOrcer getInstance() {
            if (instance == null) {
                instance = new StringOrcer();
            }
            return instance;
        }
        @Override
        public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
            BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
            byte[] bytes= Helpers.bytesForObject(obj);//null;

            bytesColumnVector.setRef(columnIndex, bytes, 0,bytes.length);
            bytesColumnVector.isNull[columnIndex]=false;
        }

        private static StringOrcer instance = null;
}
