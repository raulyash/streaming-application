package com.netflix.streamingservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@NoArgsConstructor 
@AllArgsConstructor 
@Builder 
public class StreamingResponse {

    private String movieId;
    private String steamingUrl;     // presigned hls master playlist url
    private String quality;         // get available quality
    private long expiresInMinutes;  // time taken by url to expire

}
