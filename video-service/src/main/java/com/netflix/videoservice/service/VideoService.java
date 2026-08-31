package com.netflix.videoservice.service;

import java.io.IOException;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoService {

    private final S3Client s3Client;
    private final KafkaTemplate<String, VideoUploadedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName; 

    private static final String VIDEO_UPLOADED_EVENT = "video.uploaded";

    /**
     * Upload video to AWS S3 and publish Video upload event to kafka
     * 
     * FLOW:
     * 1. Recieve multipart file
     * 2. Generate unique S3
     * 3. Upload to S3
     * 4. Publish Video upload event to
     * 5. Encoding Service picks up and start FFmpeg
     * @throws IOException 
     * @throws SdkClientException 
     * @throws AwsServiceException 
     * @throws S3Exception 
     * 
     */

    public String uploadVideo(String movieId, MultipartFile file) throws S3Exception, AwsServiceException, SdkClientException, IOException{
        log.info("Started video upload for movie: {} file: {}", movieId, file.getOriginalFilename());

        // Generate unique S3 key for raw video
        // Format : raw/movieId/uuid_filename

        // S3 bucket upload setup
        String videoKey = "raw/"+ movieId + "/" + UUID.randomUUID() + file.getOriginalFilename();

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .key(videoKey)
        .bucket(bucketName)
        .contentType(file.getContentType())
        .contentLength(file.getSize())
        .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(file.getInputStream(), file.getSize()));

        log.info("Video uploaded to S3 successfully!, Key : {}", videoKey);

        // Kafka setup
        VideoUploadedEvent videoUploadedEvent = VideoUploadedEvent.builder()
        .movieId(movieId)
        .originalFileName(file.getOriginalFilename())
        .fileSizeBytes(file.getSize())
        .videoKey(videoKey)
        .bucketName(bucketName)
        .build();

        kafkaTemplate.send(VIDEO_UPLOADED_EVENT, movieId, videoUploadedEvent);
        log.info("Event published successfully");

        return videoKey;
    }

}
