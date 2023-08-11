package com.example.orcmaker.utils;

import org.json.JSONArray;

import java.math.BigDecimal;

public class Helpers {
    public static boolean hasCommonType(JSONArray arr, BoolFunction type) {
        for (int idx = 0; idx < arr.length(); idx++) {
            if (!type.run(arr.get(idx))) return false;
        }
        return true;
    }

    public static byte[] bytesForObject(Object obj){
        if(obj instanceof String) {
            return ((String)obj).getBytes();
        }else if(obj instanceof Integer) {
            return ((Integer)obj).toString().getBytes();
        }else if(obj instanceof BigDecimal) {
            return ((BigDecimal)obj).toString().getBytes();
        }else if(obj instanceof Long) {
            return ((Long)obj).toString().getBytes();
        }else if(obj instanceof Boolean) {
            return ((Boolean)((Boolean) obj).booleanValue() ? "true" : "false").toString().getBytes();
        } else if(obj instanceof JSONArray) {
            return ((JSONArray)obj).toString().getBytes();
        }else {
            System.out.println("ASD");
            // TODO: Are there other possible types?
        }
        return null;
    }

    public interface BoolFunction {
        boolean run(Object str);
    }
}
