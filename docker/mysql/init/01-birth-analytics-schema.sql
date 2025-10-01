CREATE DATABASE IF NOT EXISTS birth_analytics_db;
CREATE USER IF NOT EXISTS 'birth_analytics'@'%' IDENTIFIED BY 'birth_analytics';
GRANT ALL PRIVILEGES ON birth_analytics_db.* TO 'birth_analytics'@'%';

USE birth_analytics_db;

CREATE TABLE IF NOT EXISTS birth_stats (
    reg_id BIGINT PRIMARY KEY,
    dob DATE,
    town VARCHAR(50),
    UNIQUE KEY unique_name_dob (reg_id, dob, town)
) ENGINE=InnoDB;