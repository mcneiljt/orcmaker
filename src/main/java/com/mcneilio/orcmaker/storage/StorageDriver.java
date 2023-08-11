package com.mcneilio.orcmaker.storage;
import java.nio.file.Path;

public interface StorageDriver {
    void addFile(String date,String eventName, String fileName, Path path);
}