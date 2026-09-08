package com.netflix.encodingservice.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoEventConsumer {

    private final EncodingService encodingService;

    /**
     * Listens to vide.uploaded kafka topic
     * Triggered when video services uploads a raw video to s3
     * 
     * FLOW
     * 
     * Video Service -> s3 upload -> kafka (vidoe.uploaded)
     *                            -> This consumer
     *                            -> Encoding service -> ffmpeg -> S3
     *                            -> kafka video.encoded 
     */

    @KafkaListener(
        topics = "video.uploaded",
        groupId = "encoding-service-group"
    )
    public void consumeVideoUploadedEvent(VideoUploadedEvent event){
        log.info("Consumed uploaded event for movie: {} file : {}"
            , event.getMovieId(), event.getOriginalFileName()
        );

        try{
            encodingService.encodeEvent(event);
        } catch (Exception e){
            log.error("Failed to process encoding for movie : {} - {}",
                event.getMovieId(), e.getMessage()
            );
        }
    }

}
