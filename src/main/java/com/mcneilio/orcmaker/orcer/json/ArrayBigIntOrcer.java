package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import com.mcneilio.orcmaker.utils.Helpers;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.json.JSONArray;

import java.math.BigDecimal;

/**
 * Adds a JSON array of bigints to a column vector
 */
public class ArrayBigIntOrcer implements Orcer {

    private static ArrayBigIntOrcer instance = null;

    /**
     * Get the singleton instance
     *
     * @return the singleton instance
     */
    public static ArrayBigIntOrcer getInstance() {
        if (instance == null) {
            instance = new ArrayBigIntOrcer();
        }
        return instance;
    }

    @Override
    public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
        ListColumnVector bigintListColumnVector = (ListColumnVector) columnVector;
        if (obj instanceof String || obj instanceof Boolean) {
            bigintListColumnVector.isNull[columnIndex] = true;

        } else if (obj instanceof BigDecimal) {
            int offset = bigintListColumnVector.childCount;
            bigintListColumnVector.isNull[columnIndex] = false;
            bigintListColumnVector.offsets[columnIndex] = offset;
            bigintListColumnVector.lengths[columnIndex] = 1;
            bigintListColumnVector.childCount += 1;
            ((LongColumnVector) ((ListColumnVector) columnVector).child).vector[offset] = ((BigDecimal) obj).longValue();
        } else if (obj instanceof Integer) {
            int offset = bigintListColumnVector.childCount;
            bigintListColumnVector.isNull[columnIndex] = false;
            bigintListColumnVector.offsets[columnIndex] = offset;
            bigintListColumnVector.lengths[columnIndex] = 1;
            bigintListColumnVector.childCount += 1;
            ((LongColumnVector) ((ListColumnVector) columnVector).child).vector[offset] = ((Integer) obj);
        } else if (obj instanceof Long) {
            int offset = bigintListColumnVector.childCount;
            bigintListColumnVector.isNull[columnIndex] = false;
            bigintListColumnVector.offsets[columnIndex] = offset;
            bigintListColumnVector.lengths[columnIndex] = 1;
            bigintListColumnVector.childCount += 1;
            ((LongColumnVector) ((ListColumnVector) columnVector).child).vector[offset] = ((Long) obj);
        } else if (obj instanceof Integer) {
            if (Helpers.hasCommonType((JSONArray) obj, (a) -> a instanceof Boolean)) {
                JSONArray msgArray = (JSONArray) obj;
                int offset = bigintListColumnVector.childCount;
                bigintListColumnVector.offsets[columnIndex] = offset;
                bigintListColumnVector.lengths[columnIndex] = msgArray.length();
                bigintListColumnVector.childCount += msgArray.length();
                bigintListColumnVector.child.ensureSize(bigintListColumnVector.childCount, true);
                for (int i = 0; i < msgArray.length(); i++) {
                    ((LongColumnVector) bigintListColumnVector.child).vector[offset + i] = msgArray.getLong(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                }
            } else {
                bigintListColumnVector.isNull[columnIndex] = true;
            }
        } else if (obj instanceof JSONArray) {
            if (Helpers.hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long || a instanceof BigDecimal)) {
                JSONArray msgArray = (JSONArray) obj;
                int offset = bigintListColumnVector.childCount;
                bigintListColumnVector.offsets[columnIndex] = offset;
                bigintListColumnVector.lengths[columnIndex] = msgArray.length();
                bigintListColumnVector.childCount += msgArray.length();
                bigintListColumnVector.child.ensureSize(bigintListColumnVector.childCount, true);
                for (int i = 0; i < msgArray.length(); i++) {
                    ((LongColumnVector) bigintListColumnVector.child).vector[offset + i] = msgArray.getLong(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                }
            } else {
                bigintListColumnVector.isNull[columnIndex] = true;
            }
        } else {
            bigintListColumnVector.isNull[columnIndex] = true;
        }
    }
}
