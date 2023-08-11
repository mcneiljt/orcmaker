package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.math.BigDecimal;

public class DecimalOrcer implements Orcer {

        public static DecimalOrcer getInstance() {
            if (instance == null) {
                instance = new DecimalOrcer();
            }
            return instance;
        }
        @Override
        public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
            DecimalColumnVector decimalColumnVector = (DecimalColumnVector) columnVector;
            
            if(obj instanceof Integer) {
                decimalColumnVector.vector[columnIndex]= new HiveDecimalWritable(HiveDecimal.create((Integer)obj));
                decimalColumnVector.isNull[columnIndex]=false;

            } else if(obj instanceof BigDecimal) {
                decimalColumnVector.vector[columnIndex]= new HiveDecimalWritable(HiveDecimal.create((BigDecimal)obj));
                decimalColumnVector.isNull[columnIndex]=false;

            } else if(obj instanceof Long) {
                decimalColumnVector.vector[columnIndex]= new HiveDecimalWritable(HiveDecimal.create((Long)obj));
                decimalColumnVector.isNull[columnIndex]=false;

            } else{
                decimalColumnVector.isNull[columnIndex]=true;
            }
        }

        private static DecimalOrcer instance = null;
}
