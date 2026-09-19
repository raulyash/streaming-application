package com.netflix.streamingservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Consumed from kafka topic: video.encoded
 * Published by encoding service after ffmpeg processing
 */
@Data 
@AllArgsConstructor 
@NoArgsConstructor 
@Builder 
public class VideoEncodedEvent {

    private String movieId; 
    private boolean success;
    private String hlsUrl; // master playlist url for streaming
    private String masterPlayListKey; // master key of m3u8
    private String errorMessage; // If encoding fail

}
