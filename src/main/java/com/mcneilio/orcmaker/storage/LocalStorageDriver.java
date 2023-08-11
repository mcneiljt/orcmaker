package com.mcneilio.orcmaker.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

public class LocalStorageDriver implements StorageDriver {
    private final String basePath;

    /**
     *
     * @param basePath This is where files are copied to.
     */
    public LocalStorageDriver(String basePath) {
        if(basePath==null)
            throw new RuntimeException("Missing local storage configuration");
        this.basePath = basePath.endsWith("/") ? basePath : basePath + '/';
    }

    public LocalStorageDriver(Properties config) {
        this(config.getProperty("storage.local.path"));
    }

    @Override
    public void addFile(String date, String eventName, String fileName, Path path) {
        try {
            Files.copy(path, Path.of(basePath + fileName), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
