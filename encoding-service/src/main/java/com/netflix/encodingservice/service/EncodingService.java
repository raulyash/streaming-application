package com.netflix.encodingservice.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.netflix.encodingservice.event.VideoEncodedEvent;
import com.netflix.encodingservice.event.VideoUploadedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {

    private final S3Client s3Client;

    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;

    @Value("${aws.S3.bucket-name}")
    private String bucketName;

    @Value("${ffmpeg.path}")
    private String ffmpegPath;

    @Value("${encoding.base-path}")
    private String basePath;

    private static final String VIDEO_ENCODED_TOPIC = "video.encoded";

    // Video qualities to encode
    // Format: resolution, bitrate, height
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            new int[] { 1920, 5000, 1080 }, // 1080p - 5000kbps bitrate
            new int[] { 1200, 2800, 720 }, // 720p - 2800kbps bitrate
            new int[] { 854, 1200, 480 }, // 480p - 1200kbps bitrate
            new int[] { 640, 800, 360 } // 360p - 800kbps bitrate
    );

    /**
     * Video Encoding process
     * 1. Download raw video event from S3
     * 2. Encode to multiple video qualities using ffmpeg
     * 3. Generate HLS playlist (.m3u8) for each qualtiy
     * 4. Create master playlist
     * 5. Uploaded all encoded files to S3
     * 6. Publish video encoded event to Kafka
     * 
     * @throws IOException
     */
    public void encodeEvent(VideoUploadedEvent event) throws IOException {
        log.info("starting encoding platform for movie : {}", event.getMovieId());

        // Create a unique path for a movie
        String jobPath = basePath + "/" + event.getMovieId();

        try {
            // Create temp directories
            Files.createDirectories(Paths.get(jobPath));
            Files.createDirectories(Paths.get(jobPath + "/encoded"));

            // Step 1 : Download raw video from S3
            String localVideoPath = jobPath + "raw_video.mp4";
            downloadFromS3(event.getVideoKey(), localVideoPath);
            log.info("Video downloaded to path : {}", localVideoPath);

            // Step 2 & 3 : Encode to multiple qualities and generate HLS playlist (.m3u8)
            for(int[] qualities : VIDEO_QUALITIES){

                int width = qualities[0];
                int bitrate = qualities[1];
                int height = qualities[2];

                String qualityDir = jobPath + "/encoded" + height + "p";
                Files.createDirectories(Paths.get(qualityDir));
                encodeToHls(localVideoPath, qualityDir, width, height, bitrate);
                log.info("Encoded {}p video.", height);   
            }

            // Step 4. Create master playlist
            String masterPlayListPath = jobPath + "/encoded/master.m3u8";
            generateMasterPlayList(masterPlayListPath);
            log.info("Master File generated.");

            // Step 5. Uploaded all encoded files to S3
            String encodedPrefix = "encoded/" + event.getMovieId() + "/";
            uploadEncodedFileToS3(jobPath + "/encoded", encodedPrefix);
            log.info("All encoded files uploaded to S3");

            // Step 6. Publish Video Encoded successful event
            String masterPlayListKey = encodedPrefix + "master.m8u8";
            String hlsUrl = "https://" + bucketName + "s3.amazonsws.com/" + masterPlayListKey;

            VideoEncodedEvent videoEncodedEvent = VideoEncodedEvent.builder()
            .movieId(event.getMovieId())
            .success(true)
            .hlsUrl(hlsUrl)
            .masterPlayListKey(masterPlayListKey)
            .errorMessage(null)
            .build();

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, videoEncodedEvent.getMovieId(), videoEncodedEvent);
            log.info("videoEncodedEvent published successully for movie : {}", videoEncodedEvent.getMovieId());

        } catch (Exception e) {
            log.info("encoding event failed for movie : {}", event.getMovieId(), e.getMessage());

            // creating a failure event
            VideoEncodedEvent videoEncodedEvent = VideoEncodedEvent.builder()
            .movieId(event.getMovieId())
            .errorMessage(e.getMessage())
            .success(false)
            .hlsUrl(null)
            .hlsUrl(null)
            .build();

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), videoEncodedEvent);
            log.info("Failure event published successfully!");
        } finally {
            // clean up temp files
            cleanUpTempFile(jobPath);
        }

    }

    private void uploadEncodedFileToS3(String localDir, String s3prefix) {
        File dir = new File(localDir);
        uploadDirectoryToS3(dir, localDir, s3prefix);
    }
    
    /**
     * Uploaded local file to S3 directory
     * @param dir
     * @param baseDir
     * @param s3prefix
     */
    private void uploadDirectoryToS3(File dir, String baseDir, String s3prefix) {
        for(File file : dir.listFiles()){
            if(file.isDirectory()){
                uploadDirectoryToS3(dir, baseDir, s3prefix);
            } else {
                String relativePath = file.getAbsolutePath()
                .substring(baseDir.length() + 1)
                .replace("\\", "/");

                String s3Key = s3prefix + relativePath;

                String contentType = file.getName().endsWith(".m3u8")
                ? "application/x-mpegURL"
                : "video/MP21";

                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType(contentType)
                .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));
                log.debug("uploaded : " + s3Key);
            }
        }
    }

    /**
     * Download file from s3 to local path
     * 
     */
    private void downloadFromS3(String s3Key, String localPath){
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
        .bucket(bucketName)
        .key(s3Key)
        .build();

        s3Client.getObject(getObjectRequest, Paths.get(localPath));
    }

    /**
     * Encoding video file to ffmpeg
     * 
     * Ffmpeg command created.
     *  - Segment .ts files created with 10 seconds duration each
     *  - A .m3u8 file for different qualities is created
     */
    private void encodeToHls(String inputPath, String outputDir, int width, int height, int bitrate) throws IOException, InterruptedException{
        String playlistPath = outputDir + "/playlist.m3u8";
        String segmentPath = outputDir + "/segment_%03d.ts";

        List<String> commands = Arrays.asList(
            ffmpegPath, 
            "-i", inputPath,    // input
            "-vf", "scale=" + width + ":" + height,    // video format
            "-c:v", "libx264",    // video codec
            "-b:v", bitrate + "k",    // video bitrate
            "-c:a", "aac",    // audio codec
            "-b:a", "128k",   // audio bitrate
            "-hls_time", "10",    // hls video segment
            "-hls_list_size", "0",  // 
            "-hls_segment_file_name", segmentPath,  // segment file name
            "-f", "hls",    // format to hls
            playlistPath    // playlist path
        );

        ProcessBuilder processBuilder = new ProcessBuilder(commands);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        int exitCode = process.waitFor();

        if(exitCode != 0){
            throw new RuntimeException("Ffmpeg processing failed with exitCode : " + exitCode);
        }

        

    }

    /**
     * Generate master playlist that references all quality playlist
     * This is the file, the video player downloads first
     */
    private void generateMasterPlayList(String masterPlayListPath) throws IOException{

        StringBuilder master = new StringBuilder();
        master.append("#EXTM3U\n");
        master.append("#EXT-X-VERSION:3\n\n");

        // Add each quality to master playlist

        int[][] qualities = {{ 1920, 5000, 1080 },
        { 1200, 2800, 720 },
        { 854, 1200, 480 },
        { 640, 800, 360 }};

        for(int[] q : qualities){

            int width = q[0];
            int bitrate = q[1];
            int height = q[2];
            master.append("#EXT-X-STREAM-INF:BANDWIDTH=")
            .append(bitrate*1000)
            .append(", RESOLUTION=").append(width)
            .append("x").append(height)
            .append(",CODECS=\"avc1.42e01e,mp4a.40.2\"\n");
            master.append(height).append("p/playlist.m3u8\n\n");          
        }

        Files.writeString(Paths.get(masterPlayListPath), master.toString()); 
    }

    private void cleanUpTempFile(String jobPath){
        try{
            Path dirPath = Paths.get(jobPath);
            if(Files.exists(dirPath)){
                Files.walk(dirPath)
                .sorted(java.util.Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
                log.info("TempFiles cleaned up for jobs : {}", jobPath);
            }
        } catch (IOException e){
            log.warn("Failed to delete temp files : {}", e.getMessage());
        }
    }



}
