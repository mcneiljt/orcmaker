package com.mcneilio.orcmaker.orcer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

public interface Orcer {

    /**
     * Add an object to the column vector
     * @param columnVector
     * @param columnIndex
     * @param obj
     */
    void addObject(ColumnVector columnVector, int columnIndex, Object obj);
}
