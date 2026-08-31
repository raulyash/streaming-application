package com.netflix.videoservice.controller;

import java.io.IOException;
import java.net.http.HttpResponse;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.netflix.videoservice.service.VideoService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
@RequestMapping("/api/v1/videos")
@RequiredArgsConstructor
public class VideoController {

    private final VideoService videoService;

    /*
    * Api to post video files to S3
    * Accepts multipart request
    * 
    * POST /api/v1/videos/upload/{movideId}
    */

    @PostMapping("/upload/{movieId}")
    public ResponseEntity<String> uploadVideos(
        @PathVariable String movieId, @RequestParam("File") MultipartFile file
    ) throws IOException{
        log.info("Video upload request for movie : {} file size : {}MB", movieId, file.getSize()/(1024*1024));

        if(file.isEmpty()){
            ResponseEntity.badRequest().body("File is empty");
        }

        String videoKey = videoService.uploadVideo(movieId, file);
        return ResponseEntity.ok(
            "Video uploaded successfully! Key : " + videoKey + " - Encoding started by kafka automatically"
        );
    }


}
