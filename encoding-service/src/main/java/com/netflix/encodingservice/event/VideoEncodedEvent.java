package com.netflix.encodingservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
