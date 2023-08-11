package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.json.JSONArray;

import java.math.BigDecimal;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayDecimalOrcer implements Orcer {

            public static ArrayDecimalOrcer getInstance() {
                if (instance == null) {
                    instance = new ArrayDecimalOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector timestampColumnVector = (ListColumnVector) columnVector;
                //byte[] bytes= null;
                if(obj instanceof String) {
                    timestampColumnVector.isNull[columnIndex]=true;
                } else if(obj instanceof BigDecimal) {
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.isNull[columnIndex] = false;
                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    ((DecimalColumnVector)((ListColumnVector) columnVector).child).vector[offset]= new HiveDecimalWritable(HiveDecimal.create((BigDecimal)obj));
                }else if(obj instanceof Integer) {
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.isNull[columnIndex] = false;
                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    ((DecimalColumnVector)((ListColumnVector) columnVector).child).vector[offset]= new HiveDecimalWritable(HiveDecimal.create((Integer)obj));
                }else if(obj instanceof Long) {
                    //timestampColumnVector.time[idx] = (Long)obj;
                    //    timestampColumnVector.isNull[idx]=true;
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.isNull[columnIndex] = false;
                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    ((DecimalColumnVector)((ListColumnVector) columnVector).child).vector[offset]= new HiveDecimalWritable(HiveDecimal.create((Long)obj));
                } else if(obj instanceof JSONArray) {
                    if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long || a instanceof BigDecimal)){
                        JSONArray msgArray = (JSONArray) obj;
                        int offset = timestampColumnVector.childCount;
                        timestampColumnVector.offsets[columnIndex] = offset;
                        timestampColumnVector.lengths[columnIndex] = msgArray.length();
                        timestampColumnVector.childCount += msgArray.length();
                        timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                        for(int i=0; i<msgArray.length(); i++) {
                            ((DecimalColumnVector) timestampColumnVector.child).vector[offset+i] = new HiveDecimalWritable(HiveDecimal.create(msgArray.getDouble(i)));
                        }
                    }else {
                        timestampColumnVector.isNull[columnIndex]=true;
                    }
                }else{
                    timestampColumnVector.isNull[columnIndex]=true;

                }
            }

            private static ArrayDecimalOrcer instance = null;
}
