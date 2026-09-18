package com.netflix.streamingservice.service;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.dto.StreamingResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

@RequiredArgsConstructor
@Slf4j
@Service
public class StreamingService {

    private final S3Presigner presigner;

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${aws.s3.presigned-url-expiry}")
    private long presignedUrlExpiry;

    // Redis key for caching streaming URLs
    private final static String STREAMING_URL_CACHE_PREFIX = "streaming:url:";

    /**
     * Get streaming url for our movie
     * 
     * FLOWW:
     * 1. Check redis cache for existing presigned url
     * 2. If cached - return immediately
     * 3. If not cached - generate new presigned url from S3
     * 4. Cache the URL in redis
     * 5. Return presigned streaming url
     * 
     * Why presigned URL?
     * - S3 bucket is a private locker room - videos are not publicly accessible
     * - Presigned url gives temperory access for X minutes
     * - Prevents unauthorized video downloads
     */

    public StreamingResponse getStreamingUrl(String movieId, String playlistKey) {
        log.info("Getting streaming url for movie: {}", movieId);

        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;

        // Check redis cache first
        String cachedUrl = redisTemplate.opsForValue().get(cacheKey);

        if (cachedUrl != null) {
            log.info("returning cached streaming url for movie: {}", movieId);
            return StreamingResponse.builder()
                    .movieId(movieId)
                    .steamingUrl(cachedUrl)
                    .quality("1080p, 720p, 480p, 360p")
                    .expiresInMinutes(presignedUrlExpiry)
                    .build();
        }

        // Generate new presigned URL from S3
        log.info("generating new presigned url for movie: {}", movieId);
        String presignedUrl = generatePresignedUrl(playlistKey);

        // Cache in redis for 55 mins
        // (5 mins less than actual expiry to avoid edge cases)
        redisTemplate.opsForValue()
                .set(
                        cacheKey,
                        presignedUrl,
                        Duration.ofMinutes(55));

        log.info("presigned url generated and cached for movie: {}", movieId);

        return StreamingResponse.builder()
        .movieId(movieId)
        .steamingUrl(presignedUrl)
        .expiresInMinutes(presignedUrlExpiry)
        .quality("1080p, 720p, 480p, 360p")
        .build();
    }

/*     public String getSignedPlaylist(String movieId, String playlistPath){

    }*/
   
    /**
     * Generate a presigned URL for S3 object
     * URL expires after configured time
     */
    private String generatePresignedUrl(String key){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build();

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
        .signatureDuration(Duration.ofMinutes(presignedUrlExpiry))
        .getObjectRequest(getObjectRequest)
        .build();

        return presigner.presignGetObject(presignRequest)
        .url().toString();

    }

    public void invalidateCache(String movieId){
        String cacheKey = STREAMING_URL_CACHE_PREFIX + movieId;
        redisTemplate.delete(cacheKey);

        log.info("Cache invalidated for movie: {}", movieId);
    }
}
