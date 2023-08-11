package com.mcneilio.orcmaker.storage;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class S3StorageDriverTest {

        @org.junit.jupiter.api.Test
        void missingConfig() {
            Properties config = new Properties();
            RuntimeException exception = assertThrows(RuntimeException.class, () -> new S3StorageDriver(config));
            assertEquals("Missing S3 configuration", exception.getMessage());
        }

}