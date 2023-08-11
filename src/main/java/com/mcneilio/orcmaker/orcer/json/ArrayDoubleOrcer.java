package com.example.orcmaker.orcer.json;

import com.example.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

import static com.example.orcmaker.utils.Helpers.hasCommonType;

public class ArrayDoubleOrcer implements Orcer {

        public static ArrayDoubleOrcer getInstance() {
            if (instance == null) {
                instance = new ArrayDoubleOrcer();
            }
            return instance;
        }
        @Override
        public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
            ListColumnVector doubleListColumnVector = (ListColumnVector) columnVector;
            if(obj instanceof String || obj instanceof Boolean) {
                doubleListColumnVector.isNull[columnIndex]=true;
            } else if(obj instanceof BigDecimal) {
                int offset = doubleListColumnVector.childCount;
                doubleListColumnVector.isNull[columnIndex] = false;
                doubleListColumnVector.offsets[columnIndex] = offset;
                doubleListColumnVector.lengths[columnIndex] = 1;
                doubleListColumnVector.childCount += 1;
                ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal)obj).doubleValue();
            }  else if(obj instanceof Integer) {
                int offset = doubleListColumnVector.childCount;
                doubleListColumnVector.isNull[columnIndex] = false;
                doubleListColumnVector.offsets[columnIndex] = offset;
                doubleListColumnVector.lengths[columnIndex] = 1;
                doubleListColumnVector.childCount += 1;
                ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer)obj);
            }  else if(obj instanceof Long) {
                int offset = doubleListColumnVector.childCount;
                doubleListColumnVector.isNull[columnIndex] = false;
                doubleListColumnVector.offsets[columnIndex] = offset;
                doubleListColumnVector.lengths[columnIndex] = 1;
                doubleListColumnVector.childCount += 1;
                ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long)obj);
            } else if(obj instanceof JSONArray) {
                if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long || a instanceof BigDecimal)){
                    JSONArray msgArray = (JSONArray) obj;
                    int offset = doubleListColumnVector.childCount;
                    doubleListColumnVector.offsets[columnIndex] = offset;
                    doubleListColumnVector.lengths[columnIndex] = msgArray.length();
                    doubleListColumnVector.childCount += msgArray.length();
                    doubleListColumnVector.child.ensureSize(doubleListColumnVector.childCount, true);
                    for(int i=0; i<msgArray.length(); i++) {
                        ((DoubleColumnVector) doubleListColumnVector.child).vector[offset+i] = msgArray.getDouble(i);
                    }
                }else {
                    doubleListColumnVector.isNull[columnIndex]=true;
                }
            }else{
                doubleListColumnVector.isNull[columnIndex]=true;
            }
        }

        private static ArrayDoubleOrcer instance = null;

}
