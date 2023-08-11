package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayFloatOrcer implements Orcer {

            public static ArrayFloatOrcer getInstance() {
                if (instance == null) {
                    instance = new ArrayFloatOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector floatColumnVector = (ListColumnVector) columnVector;
                //byte[] bytes= null;
                if(obj instanceof String || obj instanceof Boolean) {
                    //timestampColumnVector.time[idx] = (Long)obj;
                    floatColumnVector.isNull[columnIndex]=true;

                } else if(obj instanceof BigDecimal) {

                    int offset = floatColumnVector.childCount;
                    floatColumnVector.isNull[columnIndex] = false;
                    floatColumnVector.offsets[columnIndex] = offset;
                    floatColumnVector.lengths[columnIndex] = 1;
                    floatColumnVector.childCount += 1;
                    ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal)obj).doubleValue();


                }else if(obj instanceof Integer) {

                    int offset = floatColumnVector.childCount;
                    floatColumnVector.isNull[columnIndex] = false;
                    floatColumnVector.offsets[columnIndex] = offset;
                    floatColumnVector.lengths[columnIndex] = 1;
                    floatColumnVector.childCount += 1;
                    ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=(Integer)obj;


                }else if(obj instanceof Long) {

                    int offset = floatColumnVector.childCount;
                    floatColumnVector.isNull[columnIndex] = false;
                    floatColumnVector.offsets[columnIndex] = offset;
                    floatColumnVector.lengths[columnIndex] = 1;
                    floatColumnVector.childCount += 1;
                    ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=(Long)obj;


                }else if(obj instanceof JSONArray) {
                    if(hasCommonType((JSONArray) obj, (a) -> a instanceof Float || a instanceof Integer  || a instanceof Long|| a instanceof BigDecimal)){
                        JSONArray msgArray = (JSONArray) obj;
                        int offset = floatColumnVector.childCount;
                        floatColumnVector.offsets[columnIndex] = offset;
                        floatColumnVector.lengths[columnIndex] = msgArray.length();
                        floatColumnVector.childCount += msgArray.length();
                        floatColumnVector.child.ensureSize(floatColumnVector.childCount, true);
                        for(int i=0; i<msgArray.length(); i++) {
                            ((DoubleColumnVector) floatColumnVector.child).vector[offset+i] = msgArray.getFloat(i);
                        }
                    }else {
                        floatColumnVector.isNull[columnIndex]=true;
                    }
                }else{
                    floatColumnVector.isNull[columnIndex]=true;
                }
            }

            private static ArrayFloatOrcer instance = null;
}
