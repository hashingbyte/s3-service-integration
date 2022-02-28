package com.hashingbyte.s3service;

import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.utils.IoUtils;

import java.awt.image.AreaAveragingScaleFilter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class S3Client {

    private static final String USER_ACCESS_ID = "";
    private static final String USER_SECRET_KEY = "";
    private static final String RESOURCE_ARN = "";
    private static final Region region = Region.AP_SOUTH_1;

    public software.amazon.awssdk.services.s3.S3Client getAWSConnection() {
        AwsCredentials awsCredentials = AwsBasicCredentials.create(USER_ACCESS_ID, USER_SECRET_KEY);
        StsClient stsClient = StsClient.builder()
                .region(region)
                .credentialsProvider(() -> awsCredentials)
                .build();

        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(RESOURCE_ARN)
                .roleSessionName("SOME_SESSION_NAME")
                .build();

        AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(assumeRoleRequest);

        Credentials credentials = assumeRoleResponse.credentials();
        AwsCredentials awsCredentials1 = AwsSessionCredentials.create(credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken());
        return software.amazon.awssdk.services.s3.S3Client.builder()
                .region(region)
                .credentialsProvider(() -> awsCredentials1)
                .build();
    }

    public void downloadObject() {
        software.amazon.awssdk.services.s3.S3Client awsConnection = getAWSConnection();
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket("Bucket-name")  // s3 bucket name
                .key("key")  // name of file in s3 bucket
                .build();

        ResponseInputStream<GetObjectResponse> object = awsConnection.getObject(getObjectRequest);
        String path = "/tmp/";
        try(FileWriter fileWriter = new FileWriter(path)) {
            IOUtils.copy(object, fileWriter, StandardCharsets.UTF_8);
        }catch (IOException e) {

        }
    }

    public void uploadObject(File f) {
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket("")
                .key("")
                .build();

        getAWSConnection().putObject(putObjectRequest, RequestBody.fromFile(f));
    }


}
