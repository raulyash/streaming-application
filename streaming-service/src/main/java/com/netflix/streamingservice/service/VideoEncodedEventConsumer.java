package com.netflix.streamingservice.service;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.streamingservice.event.VideoEncodedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class VideoEncodedEventConsumer {

    private final RedisTemplate<String, String> redisTemplate;

    private static final String MASTER_PLAYLIST_KEY_PREFIX = "streaming:playlist";

    /**
     * Listens to video.encoded kafka topic.
     * Stores master playlist key in Redis when encoding is complete.
     * This allows StreamingService to quickly find the playlist key by movieId
     */

    @KafkaListener (
        topics = "video.encoded",
        groupId = "streaming-service-group"
    )
    public void consumeVideoEncodedEvent(VideoEncodedEvent videoEncodedEvent){
        log.info("Consumed videoEncodedEvent for movie: {} sucess: {}",
            videoEncodedEvent.getMovieId(), videoEncodedEvent.isSuccess()
        );

        if(videoEncodedEvent.isSuccess()){
            // Store master playlist key in redis
            String cacheKey = MASTER_PLAYLIST_KEY_PREFIX + videoEncodedEvent.getMovieId();
            redisTemplate.opsForValue().set(cacheKey, videoEncodedEvent.getMasterPlayListKey());
            log.info("Master playlist key stored in Redis for movie: {}", videoEncodedEvent.getMovieId());
        } else {
            log.error("Encoding failed for movie: {} - {}",
                videoEncodedEvent.getMovieId(), videoEncodedEvent.getErrorMessage()
            );
        }
    }

}
