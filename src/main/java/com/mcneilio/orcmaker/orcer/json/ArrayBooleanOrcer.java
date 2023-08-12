package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.utils.Helpers;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

/**
 * Add an array of booleans to the column vector
 */
public class ArrayBooleanOrcer implements Orcer {

    /**
     * Get the singleton instance
     *
     * @return the singleton instance
     */
    public static ArrayBooleanOrcer getInstance() {
        if (instance == null) {
            instance = new ArrayBooleanOrcer();
        }
        return instance;
    }

    /**
     * Add an array of booleans to the column vector
     *
     * @param columnVector the column vector
     * @param columnIndex  the column index
     * @param obj          the object to add
     */
    @Override
    public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
        ListColumnVector booleanListColumnVector = (ListColumnVector) columnVector;

        if(obj instanceof String || obj instanceof BigDecimal) {
            booleanListColumnVector.isNull[columnIndex]=true;
        } else if(obj instanceof Boolean) {
            booleanListColumnVector.isNull[columnIndex]=false;
            int offset = booleanListColumnVector.childCount;
            booleanListColumnVector.offsets[columnIndex] = offset;
            booleanListColumnVector.lengths[columnIndex] = 1;
            booleanListColumnVector.childCount += 1;
            booleanListColumnVector.child.ensureSize(booleanListColumnVector.childCount, true);
            ((LongColumnVector) booleanListColumnVector.child).vector[offset]= (Boolean)obj ? 1 : 0;
        } else if(obj instanceof JSONArray) {
            if(Helpers.hasCommonType((JSONArray) obj, (a) -> a instanceof Boolean)){
                JSONArray msgArray = (JSONArray) obj;
                int offset = booleanListColumnVector.childCount;
                booleanListColumnVector.offsets[columnIndex] = offset;
                booleanListColumnVector.lengths[columnIndex] = msgArray.length();
                booleanListColumnVector.childCount += msgArray.length();
                booleanListColumnVector.child.ensureSize(booleanListColumnVector.childCount, true);
                for(int i=0; i<msgArray.length(); i++) {
                    ((LongColumnVector) booleanListColumnVector.child).vector[offset+i]= msgArray.getBoolean(columnIndex) ? 1 : 0;
                }
            } else {
                booleanListColumnVector.isNull[columnIndex]=true;
            }
        }else if(obj instanceof Integer || obj instanceof Long) {
            booleanListColumnVector.isNull[columnIndex]=true;
        }else{
            booleanListColumnVector.isNull[columnIndex]=true;
        }
    }

    private static ArrayBooleanOrcer instance = null;
}
