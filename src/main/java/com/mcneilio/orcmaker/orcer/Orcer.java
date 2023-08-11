package com.example.orcmaker.orcer;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;

public interface Orcer {
    void addObject(ColumnVector columnVector, int columnIndex, Object obj);
}
