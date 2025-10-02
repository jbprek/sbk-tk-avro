-- Batch  database
create database births;
CREATE USER 'births'@'%' IDENTIFIED BY 'births';
GRANT ALL PRIVILEGES ON births.* TO 'births'@'%';

CREATE TABLE IF NOT EXISTS births.births
(
    reg_id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dob DATE NOT NULL,
    town VARCHAR(50) NOT NULL,
    reg_tm TIMESTAMP,
    weight  DECIMAL(3,1)

) ENGINE = InnoDB;