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
    `name` VARCHAR(500) NOT NULL,
    `country` VARCHAR(255),
    `parent_agency_id` BIGINT UNSIGNED,
    `representative_agency_id` BIGINT UNSIGNED,

    UNIQUE(`name`)
    KEY `representative_agency_id` (`representative_agency_id`),
    KEY `parent_agency_id` (`parent_agency_id`),
    CONSTRAINT `grant_agency_ibfk_1` FOREIGN KEY (`representative_agency_id`) REFERENCES `grant_agency` (`id`) ON DELETE SET NULL,
    CONSTRAINT `grant_agency_ibfk_2` FOREIGN KEY (`parent_agency_id`) REFERENCES `grant_agency` (`id`) ON DELETE SET NULL
);

CREATE TABLE `grant`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `ext_grant_id` VARCHAR(255),
    `grant_agency_id` BIGINT UNSIGNED NOT NULL,

    UNIQUE(`ext_grant_id`, `grant_agency_id`),
    FOREIGN KEY(`grant_agency_id`) REFERENCES grant_agency(`id`)
);


CREATE TABLE `version`(
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
    `common_name` CHAR(255),
    `full_name` CHAR(255),
    `url_id` BIGINT UNSIGNED NOT NULL,
    `version_id` BIGINT UNSIGNED NOT NULL,
    `prediction_metadata` JSON,
    `is_gcbr` BOOLEAN NOT NULL,
    `is_latest` BOOLEAN NOT NULL,
    `has_commercial_terms` BOOLEAN NOT NULL DEFAULT 0,

    UNIQUE(`short_name`, `url_id`, `version_id`),
    FOREIGN KEY(`url_id`) REFERENCES url(`id`),
    FOREIGN KEY(`version_id`) REFERENCES version(`id`)
);

CREATE TABLE `publication`(
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `title` VARCHAR(1200) NOT NULL,
    `pubmed_id` BIGINT,
    `pmc_id` VARCHAR(12),
    `publication_date` DATE,
    `authors` TEXT NOT NULL,
    `affiliation` TEXT,
    `affiliation_countries` VARCHAR(800),
    `citation_count` INT NOT NULL,
    `keywords` TEXT,
    `email` TEXT,

    UNIQUE(`pubmed_id`),
    UNIQUE(`pmc_id`)
);

CREATE TABLE `accession`(
    `accession` VARCHAR(255) NOT NULL,
    `version_id` BIGINT UNSIGNED NOT NULL,
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `url` TEXT,
    `prediction_metadata` JSON,

    PRIMARY KEY(`accession`, `version_id`, `resource_id`),
    FOREIGN KEY(`resource_id`) REFERENCES resource(`id`),
    FOREIGN KEY(`version_id`) REFERENCES version(`id`),
);

CREATE TABLE `accession_publication`(
    `accession` VARCHAR(255) NOT NULL,
    `publication_id` BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY(`accession`, `publication_id`),
    FOREIGN KEY(`accession`) REFERENCES accession(`accession`),
    FOREIGN KEY(`publication_id`) REFERENCES publication(`id`)
)

CREATE TABLE `resource_publication`(
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `publication_id` BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY(`resource_id`, `publication_id`),
    FOREIGN KEY(`resource_id`) REFERENCES resource(`id`) ON DELETE CASCADE,
    FOREIGN KEY(`publication_id`) REFERENCES publication(`id`) ON DELETE CASCADE
);

CREATE TABLE `resource_grant`(
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `grant_id` BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY(`resource_id`, `grant_id`),
    FOREIGN KEY(`resource_id`) REFERENCES resource(`id`) ON DELETE CASCADE,
    FOREIGN KEY(`grant_id`) REFERENCES `grant`(`id`) ON DELETE CASCADE
);

CREATE TABLE `publication_grant`(
    `publication_id` BIGINT UNSIGNED NOT NULL,
    `grant_id` BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY(`publication_id`, `grant_id`),
    FOREIGN KEY(`publication_id`) REFERENCES publication(`id`) ON DELETE CASCADE,
    FOREIGN KEY(`grant_id`) REFERENCES `grant`(`id`) ON DELETE CASCADE
);

CREATE TABLE `resource_mention` (
    `publication_id` BIGINT UNSIGNED NOT NULL,
    `resource_id` BIGINT UNSIGNED NOT NULL,
    `matched_alias` VARCHAR(255) NOT NULL,
    `match_count` INT,
    `mean_confidence` DECIMAL(5,4),
    `version_id` BIGINT UNSIGNED NOT NULL,

    PRIMARY KEY (`publication_id`, `resource_id`, `matched_alias`),

    FOREIGN KEY (`publication_id`) REFERENCES `publication`(`id`) ON DELETE CASCADE,
    FOREIGN KEY (`resource_id`) REFERENCES `resource`(`id`) ON DELETE CASCADE,
    FOREIGN KEY (`version_id`) REFERENCES `version`(`id`) ON DELETE CASCADE
);

CREATE TABLE `long_text` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `text` MEDIUMTEXT NOT NULL,
    PRIMARY KEY (`id`)
);


CREATE TABLE `wildsi` (
    `country` varchar(255) NOT NULL,
    `in_country_use` int DEFAULT NULL,
    `world_use` int DEFAULT NULL,
    `out_of_country_use` int DEFAULT NULL,
    PRIMARY KEY (`country`)
);

CREATE TABLE `open_letter` (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    organisation VARCHAR(255) NOT NULL,
    country VARCHAR(255),
    funding_organisation VARCHAR(255),
    sector VARCHAR(255),
    source VARCHAR(255),
    city VARCHAR(255),
    lat_lon VARCHAR(255),
    sign_date DATE,

    UNIQUE(`email`, `title`)
);

