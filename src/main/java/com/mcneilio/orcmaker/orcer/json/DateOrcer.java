package com.mcneilio.orcmaker.orcer.json;

import com.mcneilio.orcmaker.orcer.Orcer;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

public class DateOrcer implements Orcer {

    public static DateOrcer getInstance() {
        if (instance == null) {
            instance = new DateOrcer();
        }
        return instance;
    }
    @Override
    public void addObject(ColumnVector columnVector, int columnIndex, Object obj) {
        LongColumnVector longColumnVector = (LongColumnVector) columnVector;

        if (obj instanceof String) {
            try {
                longColumnVector.vector[columnIndex] = LocalDate.parse((String) obj).toEpochDay();
                longColumnVector.isNull[columnIndex] = false;
            }catch (DateTimeParseException ex) {
                longColumnVector.isNull[columnIndex] = true;
            }
        } else {
            longColumnVector.isNull[columnIndex] = true;
        }
    }

    private static DateOrcer instance = null;
}
