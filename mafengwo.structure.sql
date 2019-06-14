/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50717
 Source Host           : localhost
 Source Database       : mafengwo

 Target Server Type    : MySQL
 Target Server Version : 50717
 File Encoding         : utf-8

 Date: 04/30/2019 11:16:00 AM
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
--  Table structure for `poi`
-- ----------------------------
DROP TABLE IF EXISTS `poi`;
CREATE TABLE `poi` (
  `poi_id` int(11) NOT NULL,
  `name` varchar(128) DEFAULT NULL,
  `image` varchar(512) DEFAULT NULL,
  `link` varchar(512) DEFAULT NULL,
  `lat` float DEFAULT NULL,
  `lng` float DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `is_cnmain` int(11) DEFAULT NULL,
  `country_mddid` int(11) DEFAULT NULL,
  PRIMARY KEY (`poi_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------
--  Table structure for `poi_detail`
-- ----------------------------
DROP TABLE IF EXISTS `poi_detail`;
CREATE TABLE `poi_detail` (
  `poi_id` int(11) NOT NULL,
  `name` varchar(128) DEFAULT NULL,
  `mdd` varchar(128) DEFAULT NULL,
  `enName` varchar(256) DEFAULT NULL,
  `commentCount` varchar(128) DEFAULT NULL,
  `description` text,
  `tel` varchar(128) DEFAULT NULL,
  `site` varchar(256) DEFAULT NULL,
  `time` varchar(128) DEFAULT NULL,
  `traffic` text,
  `ticket` text,
  `openingTime` text,
  `location` varchar(256) DEFAULT NULL,
  PRIMARY KEY (`poi_id`),
  CONSTRAINT `poi_id` FOREIGN KEY (`poi_id`) REFERENCES `poi` (`poi_id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
