package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

public class BooleanOrcer implements Orcer {

    public static BooleanOrcer getInstance() {
        if (instance == null) {
            instance = new BooleanOrcer();
        }
        return instance;
    }
    @Override
    public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
        byte[] bytes= null;
        if(obj instanceof Boolean) {
            if(((Boolean) obj).booleanValue()){
                longColumnVector.vector[columnIndex] = 1;
                longColumnVector.isNull[columnIndex] = false;
            }else {
                longColumnVector.vector[columnIndex] = 0;
                longColumnVector.isNull[columnIndex] = false;
            }
        }else{
            longColumnVector.isNull[columnIndex]=true;
        }
    }

    private static BooleanOrcer instance = null;
}
