package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.utils.Helpers;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.json.JSONArray;

public class ArrayStringOrcer implements Orcer {

            public static ArrayStringOrcer getInstance() {
                if (instance == null) {
                    instance = new ArrayStringOrcer();
                }
                return instance;
            }
            @Override
            public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
                ListColumnVector timestampColumnVector = (ListColumnVector) columnVector;

                if (obj instanceof JSONArray){
                    JSONArray msgArray = (JSONArray) obj;
                    byte[][] bytes = new byte[msgArray.length()][];
                    long total=0;
                    for(int i=0; i<msgArray.length(); i++) {
                        bytes[i] = Helpers.bytesForObject(msgArray.get(i));
                        total+=bytes[i].length;
                    }
                    int offset = timestampColumnVector.childCount;
                    timestampColumnVector.lengths[columnIndex] = msgArray.length();
                    timestampColumnVector.childCount += msgArray.length();
                    timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                    for(int i=0; i<bytes.length; i++) {
                        ((BytesColumnVector) timestampColumnVector.child).isNull[offset+i]=false;
                        ((BytesColumnVector) timestampColumnVector.child).setRef(offset+i,bytes[i],0,bytes[i].length);
                    }
                    timestampColumnVector.isNull[columnIndex] = false;

                }else  {
                    byte[] bytes = Helpers.bytesForObject(obj);
                    int offset = timestampColumnVector.childCount;


                    timestampColumnVector.offsets[columnIndex] = offset;
                    timestampColumnVector.lengths[columnIndex] = 1;
                    timestampColumnVector.childCount += 1;
                    timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                    ((BytesColumnVector) timestampColumnVector.child).setRef(offset,bytes,0,bytes.length);
                    timestampColumnVector.isNull[columnIndex] = false;
                }
            }

            private static ArrayStringOrcer instance = null;
}
