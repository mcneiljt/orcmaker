package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayTinyIntOrcer implements Orcer {

            public static ArrayTinyIntOrcer getInstance() {
                if (instance == null) {
                    instance = new ArrayTinyIntOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector timestampColumnVector = (ListColumnVector) columnVector;
                //byte[] bytes= null;
                if(obj instanceof String || obj instanceof Boolean) {
                    timestampColumnVector.isNull[columnIndex]=true;
                } else if(obj instanceof BigDecimal) {
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.isNull[columnIndex] = false;
                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal)obj).longValue();
                } else if(obj instanceof Integer) {
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.isNull[columnIndex] = false;
                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer)obj);
                } else if(obj instanceof Long) {
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.isNull[columnIndex] = false;
                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long)obj);
                } else if(obj instanceof JSONArray) {
                    if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal)){
                        JSONArray msgArray = (JSONArray) obj;
                        int offset = timestampColumnVector.childCount;
                        timestampColumnVector.offsets[columnIndex] = offset;
                        timestampColumnVector.lengths[columnIndex] = msgArray.length();
                        timestampColumnVector.childCount += msgArray.length();
                        timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                        for(int i=0; i<msgArray.length(); i++) {
                            ((LongColumnVector) timestampColumnVector.child).vector[i] = msgArray.getInt(i);
                        }
                    }else {
                        timestampColumnVector.isNull[columnIndex]=true;
                    }
                }else{
                    timestampColumnVector.isNull[columnIndex]=true;
                }
            }

            private static ArrayTinyIntOrcer instance = null;
}
