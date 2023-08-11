package com.mcneilio.orcmaker.storage;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class S3StorageDriver implements StorageDriver {
    /**
     *
     * @param s3bucket
     * @param s3prefix
     * @param s3region
     */
    public S3StorageDriver(String s3bucket, String s3prefix, String s3region) {
        if(s3bucket==null || s3prefix==null || s3region==null)
            throw new RuntimeException("Missing S3 configuration");
        this.s3Bucket=s3bucket;
        this.prefix = s3prefix.endsWith("/") ? s3prefix : s3prefix+"/";
        this.s3 = S3Client.builder().region(Region.of(s3region)).build();
    }

    public S3StorageDriver(Properties config) {
        this(config.getProperty("storage.s3.bucket"), config.getProperty("storage.s3.prefix"), config.getProperty("storage.s3.region"));
    }

    @Override
    public void addFile(String date, String eventName, String fileName, Path path) {
        PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(s3Bucket)
                .key(prefix + eventName + "/date=" + date + "/" + fileName)
                .build();

        s3.putObject(putOb, Paths.get(fileName));
    }
    String s3Bucket, prefix;
    final S3Client s3;
}
