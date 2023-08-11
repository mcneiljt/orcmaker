package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayIntOrcer implements Orcer {

            public static ArrayIntOrcer getInstance() {
                if (instance == null) {
                    instance = new ArrayIntOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector intListColumnVector = (ListColumnVector) columnVector;
                //byte[] bytes= null;
                if(obj instanceof String || obj instanceof Boolean) {
                    //   timestampColumnVector.time[idx] = (Long)obj;
                    intListColumnVector.isNull[columnIndex]=true;

                } else if (obj instanceof BigDecimal) {
                    int offset = intListColumnVector.childCount;
                    intListColumnVector.isNull[columnIndex] = false;
                    intListColumnVector.offsets[columnIndex] = offset;
                    intListColumnVector.lengths[columnIndex] = 1;
                    intListColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal) obj).longValue();
                } else if (obj instanceof Integer) {
                    int offset = intListColumnVector.childCount;
                    intListColumnVector.isNull[columnIndex] = false;
                    intListColumnVector.offsets[columnIndex] = offset;
                    intListColumnVector.lengths[columnIndex] = 1;
                    intListColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer) obj);
                }else if (obj instanceof Long) {
                    int offset = intListColumnVector.childCount;
                    intListColumnVector.isNull[columnIndex] = false;
                    intListColumnVector.offsets[columnIndex] = offset;
                    intListColumnVector.lengths[columnIndex] = 1;
                    intListColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long) obj).intValue();
                }else if(obj instanceof JSONArray) {
                    if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal) ){
                        JSONArray msgArray = (JSONArray) obj;
                        int offset = intListColumnVector.childCount;
                        intListColumnVector.offsets[columnIndex] = offset;
                        intListColumnVector.lengths[columnIndex] = msgArray.length();
                        intListColumnVector.childCount += msgArray.length();
                        intListColumnVector.child.ensureSize(intListColumnVector.childCount, true);
                        for(int i=0; i<msgArray.length(); i++) {
                            ((LongColumnVector) intListColumnVector.child).vector[offset+i] = msgArray.getInt(i);
                        }
                    }else {
                        intListColumnVector.isNull[columnIndex]=true;
                    }
                }else{
                    intListColumnVector.isNull[columnIndex]=true;

                }
            }

            private static ArrayIntOrcer instance = null;
}
