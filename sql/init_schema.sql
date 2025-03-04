-- Table to store user information
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username VARCHAR(256) NOT NULL,
    email VARCHAR(256) NOT NULL,
    first_name VARCHAR(256),
    last_name VARCHAR(256)
);
-- Table to store keyword information (original table)
CREATE TABLE IF NOT EXISTS keywords (
    keyword_id BIGINT PRIMARY KEY,
    keyword_name VARCHAR(255) NOT NULL
);
-- Table to store user subscription information
CREATE TABLE IF NOT EXISTS users_subscription (
    subscription_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT,
    keyword_id BIGINT,
    subscription_type ENUM('HOURLY', 'DAILY') NOT NULL,
    start_time DATE NOT NULL,
    end_time DATE NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
);
-- Table to store hourly search volume data (original table)
CREATE TABLE IF NOT EXISTS keyword_search_volume (
    keyword_id BIGINT,
    created_datetime DATETIME,
    search_volume BIGINT,
    PRIMARY KEY (keyword_id, created_datetime),
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
);
-- Table to store daily search volume data (9:00 AM or nearest)
CREATE TABLE IF NOT EXISTS keyword_search_volume_daily (
    keyword_id BIGINT,
    created_date DATE,
    anchor_datetime DATETIME,
    search_volume BIGINT,
    PRIMARY KEY (keyword_id, created_date),
    FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
);
-- Table to store remove hourly search volume data to make exception
CREATE TABLE IF NOT EXISTS rm_keyword_search_volume (
    keyword_id BIGINT,
    created_datetime DATETIME,
    search_volume BIGINT
);