package com.mmiholdings.dsog.faaspoc;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.InputStream;

public class Function implements RequestHandler<com.amazonaws.services.lambda.runtime.events.S3Event, String> {

    public String handleRequest(com.amazonaws.services.lambda.runtime.events.S3Event s3Event, Context context) {
        context.getLogger().log("Handling event " + s3Event.toString());
        AmazonS3 s3 = AmazonS3Client.builder().build();
        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();

        context.getLogger().log("Retrieving S3 file " + srcBucket + "/" + srcKey);
        InputStream is = s3.getObject(srcBucket, srcKey).getObjectContent();

        String destBucket = context.getClientContext().getEnvironment().get("destBucket");
        ObjectMetadata metadata = new ObjectMetadata();
        context.getLogger().log("Writing S3 file " + destBucket + "/" + srcKey);
        s3.putObject(destBucket, srcKey, is, metadata);
        context.getLogger().log("Function completed successfully");
        return "OK";
    }
}
