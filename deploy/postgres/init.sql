-- Database-per-service: one logical database (and in production, one set of
-- credentials) per microservice. No service can read another one's tables.
CREATE DATABASE auth;
CREATE DATABASE users;
CREATE DATABASE tweets;
CREATE DATABASE timeline;
CREATE DATABASE engagement;
CREATE DATABASE notifications;
CREATE DATABASE sagas;
