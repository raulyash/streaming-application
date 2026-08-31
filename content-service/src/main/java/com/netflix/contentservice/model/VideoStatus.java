package com.netflix.contentservice.model;

/**
 * Tracks video status life cycle
 * FLOW
 * PENDING -> UPLOADED -> ENCODING -> READY
 *                                 -> FAILED
 */
public enum VideoStatus {
    PENDING, // movie added but not uploaded yet
    UPLOADED, // raw video uploaded to s3
    ENCODING, // FFmpeg is encoding the video 
    ENCODED, // Encoding complete
    READY, // Hls playlist ready - can be streamed
    FAILED // Encoding Failed
}
