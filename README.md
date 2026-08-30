# 🎬 Streaming Application

A scalable, event-driven media streaming platform built with **Spring Boot**, leveraging **Apache Kafka** for real-time event processing, **Redis** for high-performance caching, and **AWS S3** for durable, scalable media storage.

---

## 📖 Overview

This application provides a robust backend for a media streaming service, designed with a microservice-friendly, event-driven architecture. It handles content delivery, real-time event processing, caching, and scalable file storage — making it suitable for high-throughput streaming platforms.

---

## ✨ Features

- 🚀 **Spring Boot REST APIs** for content delivery, user management, and playback control
- 📨 **Apache Kafka** for asynchronous event streaming — playback tracking, notifications, and analytics
- ⚡ **Redis Caching** for session management and low-latency access to frequently requested data
- ☁️ **AWS S3 Integration** for scalable storage of video/audio assets, with support for multipart uploads and pre-signed URLs
- 🔄 **Event-Driven Architecture** enabling loose coupling and independent scalability of services
- 📈 Built for **horizontal scalability** and high concurrent throughput

---

## 🛠️ Tech Stack

| Layer            | Technology         |
|-------------------|---------------------|
| Backend           | Spring Boot (Java)  |
| Messaging Queue   | Apache Kafka        |
| Caching           | Redis               |
| Object Storage    | AWS S3              |
| Build Tool        | Maven / Gradle      |

---
