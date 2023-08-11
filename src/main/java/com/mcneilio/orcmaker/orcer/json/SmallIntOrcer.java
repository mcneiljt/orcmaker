package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public class SmallIntOrcer implements Orcer {

        public static SmallIntOrcer getInstance() {
            if (instance == null) {
                instance = new SmallIntOrcer();
            }
            return instance;
        }
        @Override
        public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
            LongColumnVector longColumnVector = (LongColumnVector) columnVector;
            if(obj instanceof Integer) {
                longColumnVector.vector[columnIndex] = ((Integer)obj).shortValue();
                longColumnVector.isNull[columnIndex] = false;
            }else if(obj instanceof Long) {
                longColumnVector.vector[columnIndex] = ((Long)obj).shortValue();
                longColumnVector.isNull[columnIndex] = false;
            }else{
                longColumnVector.isNull[columnIndex]=true;
            }
        }

        private static SmallIntOrcer instance = null;
}
