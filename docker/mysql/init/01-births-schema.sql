-- Batch  database
create database births;
CREATE USER 'births'@'%' IDENTIFIED BY 'births';
GRANT ALL PRIVILEGES ON births.* TO 'births'@'%';

CREATE TABLE IF NOT EXISTS births.births
(
    reg_id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(100),
    dob DATE,
    town VARCHAR(50),
    tm TIMESTAMP,
    weight  DECIMAL(3,1),
    UNIQUE KEY unique_name_dob (name, dob, town)

) ENGINE = InnoDB;