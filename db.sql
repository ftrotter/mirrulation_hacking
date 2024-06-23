

--
-- Table structure for table `comment`
--

CREATE TABLE `comment` (
  `id` int(11) NOT NULL DEFAULT 0,
  `commentId` varchar(100) NOT NULL,
  `comment_on_documentId` varchar(50) NOT NULL,
  `comment_date_text` varchar(50) NOT NULL,
  `comment_date` datetime DEFAULT NULL,
  `comment_text` longtext NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `comment_full`
--

CREATE TABLE `comment_full` (
  `id` int(11) NOT NULL,
  `commentId` varchar(100) NOT NULL,
  `comment_on_documentId` varchar(50) NOT NULL,
  `comment_date_text` varchar(50) NOT NULL,
  `comment_date` datetime DEFAULT NULL,
  `comment_text` longtext NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `llm_reply_per_comment`
--

CREATE TABLE `llm_reply_per_comment` (
  `id` int(11) NOT NULL,
  `question_id` int(11) NOT NULL,
  `answer` varchar(1000) NOT NULL,
  `commentId` varchar(100) NOT NULL,
  `chatbot_id` int(11) NOT NULL DEFAULT 1,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime NOT NULL DEFAULT current_timestamp()
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `questions_for_llm`
--

CREATE TABLE `questions_for_llm` (
  `id` int(11) NOT NULL,
  `question_text` varchar(1000) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `comment_full`
--
ALTER TABLE `comment_full`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `commentId` (`commentId`);
ALTER TABLE `comment_full` ADD FULLTEXT KEY `full_on_comment_index` (`comment_text`);

--
-- Indexes for table `llm_reply_per_comment`
--
ALTER TABLE `llm_reply_per_comment`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `questions_for_llm`
--
ALTER TABLE `questions_for_llm`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `comment_full`
--
ALTER TABLE `comment_full`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `llm_reply_per_comment`
--
ALTER TABLE `llm_reply_per_comment`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
