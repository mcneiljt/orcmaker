package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayTimestampOrcer implements Orcer {

                public static ArrayTimestampOrcer getInstance() {
                    if (instance == null) {
                        instance = new ArrayTimestampOrcer();
                    }
                    return instance;
                }
                @Override
                public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                    ListColumnVector timestampListColumnVector = (ListColumnVector) columnVector;
                    if(obj instanceof String) {
                        timestampListColumnVector.isNull[columnIndex]=true;

                    } else if(obj instanceof BigDecimal){
                        // don't try and make a timestamp out of a bigdecimal
                        timestampListColumnVector.isNull[columnIndex]=true;

                    } else if(obj instanceof Integer){
                        // don't try and make a timestamp out of a Integer
                        timestampListColumnVector.isNull[columnIndex]=true;

                    } else if(obj instanceof Boolean){
                        // don't try and make a timestamp out of a Boolean
                        timestampListColumnVector.isNull[columnIndex]=true;

                    } else if(obj instanceof Long) {

                        int offset = timestampListColumnVector.childCount;
                        timestampListColumnVector.isNull[columnIndex] = false;
                        timestampListColumnVector.offsets[columnIndex] = offset;
                        timestampListColumnVector.lengths[columnIndex] = 1;
                        timestampListColumnVector.childCount += 1;
                        ((TimestampColumnVector)((ListColumnVector) columnVector).child).time[offset]=((Long) obj);

                    }else if(obj instanceof JSONArray) {
                        if(hasCommonType((JSONArray) obj, (a) -> a instanceof Long)){
                            JSONArray msgArray = (JSONArray) obj;
                            int offset = timestampListColumnVector.childCount;
                            timestampListColumnVector.offsets[columnIndex] = offset;
                            timestampListColumnVector.lengths[columnIndex] = msgArray.length();
                            timestampListColumnVector.childCount += msgArray.length();
                            timestampListColumnVector.child.ensureSize(timestampListColumnVector.childCount, true);
                            for(int i=0; i<msgArray.length(); i++) {
                                ((TimestampColumnVector) timestampListColumnVector.child).time[offset+i]= msgArray.getLong(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                            }
                        }else {
                            timestampListColumnVector.isNull[columnIndex]=true;
                        }
                    }else{
                        timestampListColumnVector.isNull[columnIndex]=true;

                    }
                }

                private static ArrayTimestampOrcer instance = null;
}
