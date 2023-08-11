package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.json.JSONArray;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayDateOrcer implements Orcer {

            public static ArrayDateOrcer getInstance() {
                if (instance == null) {
                    instance = new ArrayDateOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector dateListColumnVector = (ListColumnVector) columnVector;
                if(obj instanceof String) {
                    try {
                        int offset = dateListColumnVector.childCount;
                        long tmp = LocalDate.parse((String) obj).toEpochDay();

                        dateListColumnVector.isNull[columnIndex] = false;

                        dateListColumnVector.offsets[columnIndex] = offset;
                        dateListColumnVector.lengths[columnIndex] = 1;
                        dateListColumnVector.childCount += 1;
                        dateListColumnVector.child.ensureSize(dateListColumnVector.childCount, true);

                        ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=tmp;

                    }catch (DateTimeParseException ex) {
                        dateListColumnVector.isNull[columnIndex] = true;
                    }

                }else if(obj instanceof JSONArray) {
                    if(hasCommonType((JSONArray) obj, (a) -> a instanceof String)){
                        try {
                            JSONArray msgArray = (JSONArray) obj;
                            long[] longs = new long[msgArray.length()];
                            for(int i=0; i<msgArray.length(); i++) {
                                longs[i] = LocalDate.parse(msgArray.getString(i)).toEpochDay();
                            }
                            int offset = dateListColumnVector.childCount;
                            dateListColumnVector.offsets[columnIndex] = offset;
                            dateListColumnVector.lengths[columnIndex] = msgArray.length();
                            dateListColumnVector.childCount += msgArray.length();
                            dateListColumnVector.child.ensureSize(dateListColumnVector.childCount, true);
                            for(int i=0; i<longs.length; i++) {
                                ((LongColumnVector) dateListColumnVector.child).vector[offset+i] = longs[i];
                            }
                            dateListColumnVector.isNull[columnIndex] = false;

                        }catch (DateTimeParseException ex) {
                            dateListColumnVector.isNull[columnIndex] = true;
                        }
                    }else {
                        dateListColumnVector.isNull[columnIndex]=true;
                    }
                }else{
                    dateListColumnVector.isNull[columnIndex]=true;
                }
            }

            private static ArrayDateOrcer instance = null;
}
