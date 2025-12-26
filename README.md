# StoreReviewRecommendationSystem
This project is a high-concurrency store exploration and recommendation system.
It supports store blog publishing, location-based store recommendations, and implements a high-performance coupon seckill (flash sale) mechanism optimized with Redis and message queues.
The system is designed for high availability, high concurrency, and data consistency in a clustered environment.

Tech Stack Summary
Backend: Java, Spring Boot
Database: MySQL
Cache / MQ: Redis, Redis Stream
Load Balancer: Nginx
Distributed Lock: Redisson
Testing: JMeter

Designed a high-concurrency coupon seckill system using Redis Lua scripts.
Optimized flash sale performance from 450ms to 110ms average response time.
Replaced blocking queues with Redis Stream message queue for asynchronous order processing.
Used distributed locking and transactional AOP to ensure data consistency.

