package com.netflix.encodingservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Event published to kafka when a video is uploaded to S3
 * Encoding service consume this to start ffmpeg process
 * 
 * TOPIC: video.upload
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class VideoUploadedEvent {

    private String movieId;
    private String videoKey;
    private String bucketName;
    private String originalFileName;
    private long fileSizeBytes;

}
