package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;

import java.math.BigDecimal;

public class DoubleOrcer implements Orcer {

        public static DoubleOrcer getInstance() {
            if (instance == null) {
                instance = new DoubleOrcer();
            }
            return instance;
        }
        @Override
        public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
            DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
            byte[] bytes= null;
            if(obj instanceof Integer) {
                doubleColumnVector.vector[columnIndex]=(Integer)obj;
                doubleColumnVector.isNull[columnIndex]=false;

            } else if(obj instanceof Long) {
                doubleColumnVector.vector[columnIndex]=(Long)obj;
                doubleColumnVector.isNull[columnIndex]=false;

            } else if(obj instanceof BigDecimal) {
                doubleColumnVector.vector[columnIndex]=((BigDecimal)obj).doubleValue();
                doubleColumnVector.isNull[columnIndex]=false;
            } else{
                doubleColumnVector.isNull[columnIndex]=true;
            }
        }

        private static DoubleOrcer instance = null;
}
