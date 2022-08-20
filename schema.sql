CREATE TABLE `rooms` (
  `id` int NOT NULL,
  `name` varchar(10) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `rooms_id_idx` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE `students` (
  `birthday` datetime NOT NULL,
  `id` int NOT NULL,
  `name` varchar(50) NOT NULL,
  `room` int NOT NULL,
  `sex` varchar(1) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `room_idx` (`room`),
  CONSTRAINT `room` FOREIGN KEY (`room`) REFERENCES `rooms` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
