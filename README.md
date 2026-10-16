-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1
-- Generation Time: Oct 16, 2026 at 11:33 AM
-- Server version: 10.4.32-MariaDB
-- PHP Version: 8.2.12

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `ufaa_db`
--

-- --------------------------------------------------------

--
-- Table structure for table `activity_logs`
--

CREATE TABLE `activity_logs` (
  `id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `username` varchar(100) NOT NULL DEFAULT 'system',
  `action` varchar(100) NOT NULL,
  `description` text DEFAULT NULL,
  `record_id` int(11) DEFAULT NULL,
  `ip_address` varchar(45) NOT NULL DEFAULT '',
  `created_at` datetime NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `admin_settings`
--

CREATE TABLE `admin_settings` (
  `setting_key` varchar(100) NOT NULL,
  `setting_value` text NOT NULL,
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `admin_users`
--

CREATE TABLE `admin_users` (
  `id` int(11) NOT NULL,
  `username` varchar(100) NOT NULL,
  `fullname` varchar(250) NOT NULL DEFAULT '',
  `email` varchar(200) NOT NULL DEFAULT '',
  `password_hash` varchar(255) NOT NULL,
  `role` varchar(100) NOT NULL DEFAULT 'compliance_officer',
  `is_active` tinyint(1) NOT NULL DEFAULT 1,
  `failed_login_attempts` int(11) NOT NULL DEFAULT 0,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `last_login` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `duplicate_notes`
--

CREATE TABLE `duplicate_notes` (
  `id` int(11) NOT NULL,
  `group_key` varchar(500) NOT NULL COMMENT 'account_number___id___last_transaction___compilation_date',
  `note` text NOT NULL DEFAULT '',
  `noted_by` varchar(255) NOT NULL DEFAULT '',
  `reviewed` tinyint(1) NOT NULL DEFAULT 0,
  `reviewed_by` varchar(255) DEFAULT NULL,
  `reviewed_at` datetime DEFAULT NULL,
  `noted_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `sandbox_drafts`
--

CREATE TABLE `sandbox_drafts` (
  `id` int(11) NOT NULL,
  `draft_name` varchar(255) NOT NULL,
  `created_by` varchar(100) NOT NULL,
  `last_edited_by` varchar(100) NOT NULL,
  `rows_data` longtext NOT NULL,
  `presence_data` text DEFAULT NULL,
  `viewer_data` text DEFAULT NULL,
  `status` enum('active','submitted') NOT NULL DEFAULT 'active',
  `flagged_reason` text DEFAULT NULL,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `updated_at` datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `unclaimed_assets`
--

CREATE TABLE `unclaimed_assets` (
  `record_id` int(11) NOT NULL,
  `owner_name` varchar(500) DEFAULT NULL,
  `id_passport_no` varchar(500) DEFAULT NULL,
  `date_of_birth` date DEFAULT NULL,
  `account_number` varchar(500) DEFAULT NULL,
  `last_transaction` text DEFAULT NULL,
  `due_amount` text DEFAULT NULL,
  `compilation_date` date DEFAULT NULL,
  `status` varchar(50) DEFAULT 'Unclaimed',
  `letter_generated` varchar(10) DEFAULT 'No',
  `letter_generated_date` datetime DEFAULT NULL,
  `letter_received` varchar(10) DEFAULT 'No',
  `letter_date` text DEFAULT NULL,
  `letter_file_path` text DEFAULT NULL,
  `stamped_file_path` varchar(500) DEFAULT NULL,
  `uploaded_at` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `uploaded_files`
--

CREATE TABLE `uploaded_files` (
  `id` int(11) NOT NULL,
  `file_name` varchar(500) NOT NULL,
  `uploaded_at` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `upload_sessions`
--

CREATE TABLE `upload_sessions` (
  `id` int(11) NOT NULL,
  `uploaded_by` varchar(100) NOT NULL DEFAULT 'system',
  `file_name` varchar(500) NOT NULL,
  `record_count` int(11) NOT NULL DEFAULT 0,
  `status` enum('success','partial','failed') NOT NULL DEFAULT 'success',
  `protection_password` varchar(100) DEFAULT NULL,
  `notes` text DEFAULT NULL,
  `uploaded_at` datetime NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `activity_logs`
--
ALTER TABLE `activity_logs`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_action` (`action`),
  ADD KEY `idx_created` (`created_at`),
  ADD KEY `idx_user_id` (`user_id`);

--
-- Indexes for table `admin_settings`
--
ALTER TABLE `admin_settings`
  ADD PRIMARY KEY (`setting_key`);

--
-- Indexes for table `admin_users`
--
ALTER TABLE `admin_users`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `username` (`username`);

--
-- Indexes for table `duplicate_notes`
--
ALTER TABLE `duplicate_notes`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uq_group_key` (`group_key`(400));

--
-- Indexes for table `sandbox_drafts`
--
ALTER TABLE `sandbox_drafts`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `unclaimed_assets`
--
ALTER TABLE `unclaimed_assets`
  ADD PRIMARY KEY (`record_id`),
  ADD KEY `idx_lookup` (`owner_name`(100),`id_passport_no`(100)),
  ADD KEY `idx_owner_name` (`owner_name`(191)),
  ADD KEY `idx_id_passport` (`id_passport_no`(191)),
  ADD KEY `idx_account_no` (`account_number`(191)),
  ADD KEY `idx_status` (`status`),
  ADD KEY `idx_compilation_date` (`compilation_date`);
ALTER TABLE `unclaimed_assets` ADD FULLTEXT KEY `ft_owner_name` (`owner_name`);

--
-- Indexes for table `uploaded_files`
--
ALTER TABLE `uploaded_files`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `file_name` (`file_name`);

--
-- Indexes for table `upload_sessions`
--
ALTER TABLE `upload_sessions`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_uploaded_at` (`uploaded_at`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `activity_logs`
--
ALTER TABLE `activity_logs`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `admin_users`
--
ALTER TABLE `admin_users`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `duplicate_notes`
--
ALTER TABLE `duplicate_notes`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `sandbox_drafts`
--
ALTER TABLE `sandbox_drafts`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `unclaimed_assets`
--
ALTER TABLE `unclaimed_assets`
  MODIFY `record_id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `uploaded_files`
--
ALTER TABLE `uploaded_files`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `upload_sessions`
--
ALTER TABLE `upload_sessions`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
