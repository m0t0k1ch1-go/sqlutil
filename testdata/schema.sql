CREATE TABLE `user` (
  `id` BIGINT UNSIGNED NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `task` (
  `id` BIGINT UNSIGNED NOT NULL,
  `user_id` BIGINT UNSIGNED NOT NULL,
  `title` VARCHAR(255) NOT NULL,
  `is_completed` BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`)
);
