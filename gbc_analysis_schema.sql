CREATE TABLE `url`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `url` CHAR(255) NOT NULL,
    `url_country` CHAR(255),
    `url_coordinates` CHAR(255),
    `wayback_url` CHAR(255),

    UNIQUE(`url`)
);

CREATE TABLE `connection_status`(
    `url_id` BIGINT UNSIGNED NOT NULL,
    `status` VARCHAR(255) NOT NULL,
    `date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `is_online` BOOLEAN NOT NULL DEFAULT 0,
    `is_latest` BOOLEAN NOT NULL DEFAULT 1,

    PRIMARY KEY(`url_id`, `date`),
    FOREIGN KEY(`url_id`) REFERENCES url(`id`)
);

CREATE TABLE `grant_agency`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name` VARCHAR(255) NOT NULL,
    `parent_agency_id` BIGINT UNSIGNED,

    UNIQUE(`name`)
);

CREATE TABLE `grant`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `ext_grant_id` VARCHAR(255) NOT NULL,
    `grant_agency_id` BIGINT UNSIGNED NOT NULL,
    
    UNIQUE(`ext_grant_id`),
    FOREIGN KEY(`grant_agency_id`) REFERENCES grant_agency(`id`)
);


CREATE TABLE `prediction`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `name` VARCHAR(255) NOT NULL,
    `date` DATETIME NOT NULL,
    `user` VARCHAR(255) NOT NULL DEFAULT 'user()',
    `additional_metadata` JSON, -- Include additional details/scores from your prediction in a more loosely structured object

    UNIQUE(`name`, `date`)
);


CREATE TABLE `resource`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `short_name` CHAR(255) NOT NULL,
    `common_name` CHAR(255) NOT NULL,
    `full_name` CHAR(255) NOT NULL,
    `url_id` BIGINT UNSIGNED NOT NULL,
    `prediction_id` BIGINT UNSIGNED NOT NULL,
    `prediction_metadata` JSON,
    `is_gcbr` BOOLEAN NOT NULL,
    `is_latest` BOOLEAN NOT NULL,

    UNIQUE(`short_name`, `url_id`, `prediction_id`),
    FOREIGN KEY(`url_id`) REFERENCES url(`id`),
    FOREIGN KEY(`prediction_id`) REFERENCES prediction(`id`)
);

CREATE TABLE `publication`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `title` VARCHAR(255) NOT NULL,
    `pubmed_id` BIGINT NOT NULL,
    `pmc_id` VARCHAR(255),
    `publication_date` DATE NOT NULL,
    `authors` VARCHAR(255) NOT NULL,
    `affiliation` VARCHAR(255) NOT NULL,
    `affiliation_countries` VARCHAR(255),
    `citation_count` INT NOT NULL,
    `keywords` VARCHAR(255),

    UNIQUE(`pubmed_id`)
);

CREATE TABLE `accession`(
    `accession` BIGINT UNSIGNED NOT NULL,
    `publication_id` BIGINT UNSIGNED NOT NULL,
    `prediction_id` BIGINT UNSIGNED NOT NULL,
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `prediction_metadata` JSON,

    PRIMARY KEY(`accession`, `publication_id`),
    FOREIGN KEY(`resource_id`) REFERENCES resource(`id`),
    FOREIGN KEY(`prediction_id`) REFERENCES prediction(`id`),
    FOREIGN KEY(`publication_id`) REFERENCES publication(`id`)
);

CREATE TABLE `resource_publication`(
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `publication_id` BIGINT UNSIGNED NOT NULL,
    
    PRIMARY KEY(`resource_id`, `publication_id`),
    FOREIGN KEY(`resource_id`) REFERENCES resource(`id`), 
    FOREIGN KEY(`publication_id`) REFERENCES publication(`id`)
);

CREATE TABLE `resource_grant`(
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `grant_id` BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY(`resource_id`, `grant_id`),
    FOREIGN KEY(`resource_id`) REFERENCES resource(`id`),
    FOREIGN KEY(`grant_id`) REFERENCES `grant`(`id`)
);

