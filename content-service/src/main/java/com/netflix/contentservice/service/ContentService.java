package com.netflix.contentservice.service;

import java.util.List;
import org.springframework.stereotype.Service;
import com.netflix.contentservice.dto.MovieRequest;
import com.netflix.contentservice.dto.MovieResponse;
import com.netflix.contentservice.model.Genre;
import com.netflix.contentservice.model.Movie;
import com.netflix.contentservice.model.VideoStatus;
import com.netflix.contentservice.repository.MovieRepository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@AllArgsConstructor
@Slf4j
public class ContentService {

    private final MovieRepository movieRepository;

    public MovieResponse addMovie(MovieRequest movieRequest) {
        log.info("Adding new movie : {}", movieRequest.getTitle());

        Movie movie = Movie.builder()
                .title(movieRequest.getTitle())
                .description(movieRequest.getDescription())
                .director(movieRequest.getDirector())
                .genre(movieRequest.getGenre())
                .cast(movieRequest.getCast())
                .releaseYear(movieRequest.getReleaseYear())
                .thumbnailUrl(movieRequest.getThumbnailUrl())
                .durationMinutes(movieRequest.getDurationMinutes())
                .videoStatus(VideoStatus.PENDING)
                .build();

        Movie savedMovie = movieRepository.save(movie);

        log.info("saved movie : {}", savedMovie.getId());

        return mapToResponse(savedMovie);
    }

    private MovieResponse mapToResponse(Movie savedMovie) {
        MovieResponse movieResponse = MovieResponse.builder()
                .id(savedMovie.getId())
                .title(savedMovie.getTitle())
                .description(savedMovie.getDescription())
                .genre(savedMovie.getGenre())
                .director(savedMovie.getDirector())
                .cast(savedMovie.getCast())
                .releaseYear(savedMovie.getReleaseYear())
                .rating(savedMovie.getRating())
                .thumbnailUrl(savedMovie.getThumbnailUrl())
                .durationMinutes(savedMovie.getDurationMinutes())
                .videoKey(savedMovie.getVideoKey())
                .hlsUrl(savedMovie.getHlsUrl())
                .videoStatus(savedMovie.getVideoStatus())
                .createdAt(savedMovie.getCreatedAt())
                .build();

        return movieResponse;
    }

    /*
     * Get all movie list
     */
    public List<MovieResponse> getAllMovies() {
        return movieRepository.findAll()
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    public MovieResponse getMovieById(String movieId) {
        Movie movie = movieRepository.findById(movieId)
                .orElseThrow(() -> new RuntimeException("Movie not found : " + movieId));

        return mapToResponse(movie);
    }

    public List<MovieResponse> getAllMoviesByGenre(Genre genre) {
        return movieRepository.getAllByGenre(genre)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    public List<MovieResponse> searchMovies(String title) {
        return movieRepository.findByTitleContainingIgnoreCase(title)
                .stream()
                .map(this::mapToResponse)
                .toList();
    }

    // Update S3 video key
    public void updateVideoKey(String movieId, String videoKey){
        log.info("Adding video key for movie : {}", movieId);
        Movie movie = movieRepository.findById(movieId)
        .orElseThrow(() -> new RuntimeException("Movie id not found : " + movieId));

        movie.setVideoKey(videoKey);
        movie.setVideoStatus(VideoStatus.UPLOADED);
        movieRepository.save(movie);
    }

    // Updating HLS url
    public void updateHlsUrl(String movieId, String hlsUrl){
        log.info("Adding hls url for movie : {}", movieId);
        Movie movie = movieRepository.findById(movieId)
        .orElseThrow(() -> new RuntimeException("Movie id not found : " + movieId));

        movie.setHlsUrl(hlsUrl);
        movie.setVideoStatus(VideoStatus.READY);
        movieRepository.save(movie);
        log.info("Movie {}, is ready for streaming..", movieId);
    }

    /**
     * Method to uploaded video status of the uploaded movie
     */
    public void updateVideoStatus(String movieId, VideoStatus videoStatus){
        Movie movie = movieRepository.findById(movieId).orElseThrow(
            ()-> new RuntimeException("Movie not found: " + movieId));
        movie.setVideoStatus(videoStatus);
        movieRepository.save(movie);
    }

}
