package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.utils.Helpers;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

public class ArraySmallIntOrcer implements Orcer {

            public static ArraySmallIntOrcer getInstance() {
                if (instance == null) {
                    instance = new ArraySmallIntOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector smallintListColumnVector = (ListColumnVector) columnVector;
                if(obj instanceof String || obj instanceof Boolean) {
                    smallintListColumnVector.isNull[columnIndex]=true;

                } else if(obj instanceof JSONArray) {
                    if(Helpers.hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal)){
                        JSONArray msgArray = (JSONArray) obj;
                        int offset = smallintListColumnVector.childCount;
                        smallintListColumnVector.offsets[columnIndex] = offset;
                        smallintListColumnVector.lengths[columnIndex] = msgArray.length();
                        smallintListColumnVector.childCount += msgArray.length();
                        smallintListColumnVector.child.ensureSize(smallintListColumnVector.childCount, true);
                        for(int i=0; i<msgArray.length(); i++) {
                            ((LongColumnVector) smallintListColumnVector.child).vector[offset+i] = (short)msgArray.getInt(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                        }
                    }else {
                        smallintListColumnVector.isNull[columnIndex]=true;
                    }
                }else if(obj instanceof BigDecimal) {
                    int offset = smallintListColumnVector.childCount;
                    smallintListColumnVector.isNull[columnIndex] = false;
                    smallintListColumnVector.offsets[columnIndex] = offset;
                    smallintListColumnVector.lengths[columnIndex] = 1;
                    smallintListColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal) obj).shortValue();
                }else if(obj instanceof Integer) {
                    int offset = smallintListColumnVector.childCount;
                    smallintListColumnVector.isNull[columnIndex] = false;
                    smallintListColumnVector.offsets[columnIndex] = offset;
                    smallintListColumnVector.lengths[columnIndex] = 1;
                    smallintListColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer) obj).shortValue();
                }else if(obj instanceof Long) {
                    int offset = smallintListColumnVector.childCount;
                    smallintListColumnVector.isNull[columnIndex] = false;
                    smallintListColumnVector.offsets[columnIndex] = offset;
                    smallintListColumnVector.lengths[columnIndex] = 1;
                    smallintListColumnVector.childCount += 1;
                    ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long) obj).shortValue();
                }else{
                    smallintListColumnVector.isNull[columnIndex]=true;
                }
            }

            private static ArraySmallIntOrcer instance = null;
}
