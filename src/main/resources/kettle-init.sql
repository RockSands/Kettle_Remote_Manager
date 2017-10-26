/*
Navicat MySQL Data Transfer

Source Server         : KT-测试(10.23.13.184)
Source Server Version : 50166
Source Host           : 10.23.13.184:3306
Source Database       : kettle

Target Server Type    : MYSQL
Target Server Version : 50166
File Encoding         : 65001

Date: 2017-10-17 10:58:30
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for R_CLUSTER
-- ----------------------------
DROP TABLE IF EXISTS `R_CLUSTER`;
CREATE TABLE `R_CLUSTER` (
  `ID_CLUSTER` bigint(20) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `BASE_PORT` varchar(255) DEFAULT NULL,
  `SOCKETS_BUFFER_SIZE` varchar(255) DEFAULT NULL,
  `SOCKETS_FLUSH_INTERVAL` varchar(255) DEFAULT NULL,
  `SOCKETS_COMPRESSED` tinyint(1) DEFAULT NULL,
  `DYNAMIC_CLUSTER` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_CLUSTER`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_CLUSTER
-- ----------------------------

-- ----------------------------
-- Table structure for R_CLUSTER_SLAVE
-- ----------------------------
DROP TABLE IF EXISTS `R_CLUSTER_SLAVE`;
CREATE TABLE `R_CLUSTER_SLAVE` (
  `ID_CLUSTER_SLAVE` bigint(20) NOT NULL,
  `ID_CLUSTER` int(11) DEFAULT NULL,
  `ID_SLAVE` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID_CLUSTER_SLAVE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_CLUSTER_SLAVE
-- ----------------------------

-- ----------------------------
-- Table structure for R_CONDITION
-- ----------------------------
DROP TABLE IF EXISTS `R_CONDITION`;
CREATE TABLE `R_CONDITION` (
  `ID_CONDITION` bigint(20) NOT NULL,
  `ID_CONDITION_PARENT` int(11) DEFAULT NULL,
  `NEGATED` tinyint(1) DEFAULT NULL,
  `OPERATOR` varchar(255) DEFAULT NULL,
  `LEFT_NAME` varchar(255) DEFAULT NULL,
  `CONDITION_FUNCTION` varchar(255) DEFAULT NULL,
  `RIGHT_NAME` varchar(255) DEFAULT NULL,
  `ID_VALUE_RIGHT` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID_CONDITION`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_CONDITION
-- ----------------------------

-- ----------------------------
-- Table structure for R_DATABASE
-- ----------------------------
DROP TABLE IF EXISTS `R_DATABASE`;
CREATE TABLE `R_DATABASE` (
  `ID_DATABASE` bigint(20) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `ID_DATABASE_TYPE` int(11) DEFAULT NULL,
  `ID_DATABASE_CONTYPE` int(11) DEFAULT NULL,
  `HOST_NAME` varchar(255) DEFAULT NULL,
  `DATABASE_NAME` mediumtext,
  `PORT` int(11) DEFAULT NULL,
  `USERNAME` varchar(255) DEFAULT NULL,
  `PASSWORD` varchar(255) DEFAULT NULL,
  `SERVERNAME` varchar(255) DEFAULT NULL,
  `DATA_TBS` varchar(255) DEFAULT NULL,
  `INDEX_TBS` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_DATABASE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_DATABASE
-- ----------------------------

-- ----------------------------
-- Table structure for R_DATABASE_ATTRIBUTE
-- ----------------------------
DROP TABLE IF EXISTS `R_DATABASE_ATTRIBUTE`;
CREATE TABLE `R_DATABASE_ATTRIBUTE` (
  `ID_DATABASE_ATTRIBUTE` bigint(20) NOT NULL,
  `ID_DATABASE` int(11) DEFAULT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `VALUE_STR` mediumtext,
  PRIMARY KEY (`ID_DATABASE_ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_DATABASE_ATTRIBUTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_DATABASE_CONTYPE
-- ----------------------------
DROP TABLE IF EXISTS `R_DATABASE_CONTYPE`;
CREATE TABLE `R_DATABASE_CONTYPE` (
  `ID_DATABASE_CONTYPE` bigint(20) NOT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_DATABASE_CONTYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_DATABASE_CONTYPE
-- ----------------------------
INSERT INTO `R_DATABASE_CONTYPE` VALUES ('1', 'Native', 'Native (JDBC)');
INSERT INTO `R_DATABASE_CONTYPE` VALUES ('2', 'ODBC', 'ODBC');
INSERT INTO `R_DATABASE_CONTYPE` VALUES ('3', 'OCI', 'OCI');
INSERT INTO `R_DATABASE_CONTYPE` VALUES ('4', 'Plugin', 'Plugin specific access method');
INSERT INTO `R_DATABASE_CONTYPE` VALUES ('5', 'JNDI', 'JNDI');
INSERT INTO `R_DATABASE_CONTYPE` VALUES ('6', ',', 'Custom');

-- ----------------------------
-- Table structure for R_DATABASE_TYPE
-- ----------------------------
DROP TABLE IF EXISTS `R_DATABASE_TYPE`;
CREATE TABLE `R_DATABASE_TYPE` (
  `ID_DATABASE_TYPE` bigint(20) NOT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_DATABASE_TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_DATABASE_TYPE
-- ----------------------------
INSERT INTO `R_DATABASE_TYPE` VALUES ('1', 'INGRES', 'Ingres');
INSERT INTO `R_DATABASE_TYPE` VALUES ('2', 'INTERBASE', 'Borland Interbase');
INSERT INTO `R_DATABASE_TYPE` VALUES ('3', 'IMPALA', 'Impala');
INSERT INTO `R_DATABASE_TYPE` VALUES ('4', 'INFOBRIGHT', 'Infobright');
INSERT INTO `R_DATABASE_TYPE` VALUES ('5', 'HIVE2', 'Hadoop Hive 2');
INSERT INTO `R_DATABASE_TYPE` VALUES ('6', 'ORACLE', 'Oracle');
INSERT INTO `R_DATABASE_TYPE` VALUES ('7', 'EXTENDB', 'ExtenDB');
INSERT INTO `R_DATABASE_TYPE` VALUES ('8', 'MSACCESS', 'MS Access');
INSERT INTO `R_DATABASE_TYPE` VALUES ('9', 'SYBASE', 'Sybase');
INSERT INTO `R_DATABASE_TYPE` VALUES ('10', 'PALO', 'Palo MOLAP Server');
INSERT INTO `R_DATABASE_TYPE` VALUES ('11', 'INFORMIX', 'Informix');
INSERT INTO `R_DATABASE_TYPE` VALUES ('12', 'LucidDB', 'LucidDB');
INSERT INTO `R_DATABASE_TYPE` VALUES ('13', 'TERADATA', 'Teradata');
INSERT INTO `R_DATABASE_TYPE` VALUES ('14', 'UNIVERSE', 'UniVerse database');
INSERT INTO `R_DATABASE_TYPE` VALUES ('15', 'HIVE', 'Hadoop Hive');
INSERT INTO `R_DATABASE_TYPE` VALUES ('16', 'MONETDB', 'MonetDB');
INSERT INTO `R_DATABASE_TYPE` VALUES ('17', 'SPARKSIMBA', 'SparkSQL');
INSERT INTO `R_DATABASE_TYPE` VALUES ('18', 'CACHE', 'Intersystems Cache');
INSERT INTO `R_DATABASE_TYPE` VALUES ('19', 'MSSQL', 'MS SQL Server');
INSERT INTO `R_DATABASE_TYPE` VALUES ('20', 'GREENPLUM', 'Greenplum');
INSERT INTO `R_DATABASE_TYPE` VALUES ('21', 'GENERIC', 'Generic database');
INSERT INTO `R_DATABASE_TYPE` VALUES ('22', 'SQLITE', 'SQLite');
INSERT INTO `R_DATABASE_TYPE` VALUES ('23', 'REMEDY-AR-SYSTEM', 'Remedy Action Request System');
INSERT INTO `R_DATABASE_TYPE` VALUES ('24', 'MONDRIAN', 'Native Mondrian');
INSERT INTO `R_DATABASE_TYPE` VALUES ('25', 'NETEZZA', 'Netezza');
INSERT INTO `R_DATABASE_TYPE` VALUES ('26', 'VERTICA5', 'Vertica 5+');
INSERT INTO `R_DATABASE_TYPE` VALUES ('27', 'POSTGRESQL', 'PostgreSQL');
INSERT INTO `R_DATABASE_TYPE` VALUES ('28', 'EXASOL4', 'Exasol 4');
INSERT INTO `R_DATABASE_TYPE` VALUES ('29', 'HYPERSONIC', 'Hypersonic');
INSERT INTO `R_DATABASE_TYPE` VALUES ('30', 'AS/400', 'AS/400');
INSERT INTO `R_DATABASE_TYPE` VALUES ('31', 'ORACLERDB', 'Oracle RDB');
INSERT INTO `R_DATABASE_TYPE` VALUES ('32', 'DBASE', 'dBase III, IV or 5');
INSERT INTO `R_DATABASE_TYPE` VALUES ('33', 'KINGBASEES', 'KingbaseES');
INSERT INTO `R_DATABASE_TYPE` VALUES ('34', 'KettleThin', 'Pentaho Data Services');
INSERT INTO `R_DATABASE_TYPE` VALUES ('35', 'SAPR3', 'SAP ERP System');
INSERT INTO `R_DATABASE_TYPE` VALUES ('36', 'SQLBASE', 'Gupta SQL Base');
INSERT INTO `R_DATABASE_TYPE` VALUES ('37', 'DERBY', 'Apache Derby');
INSERT INTO `R_DATABASE_TYPE` VALUES ('38', 'VERTICA', 'Vertica');
INSERT INTO `R_DATABASE_TYPE` VALUES ('39', 'INFINIDB', 'Calpont InfiniDB');
INSERT INTO `R_DATABASE_TYPE` VALUES ('40', 'MYSQL', 'MySQL');
INSERT INTO `R_DATABASE_TYPE` VALUES ('41', 'IMPALASIMBA', 'Cloudera Impala');
INSERT INTO `R_DATABASE_TYPE` VALUES ('42', 'MSSQLNATIVE', 'MS SQL Server (Native)');
INSERT INTO `R_DATABASE_TYPE` VALUES ('43', 'H2', 'H2');
INSERT INTO `R_DATABASE_TYPE` VALUES ('44', 'SAPDB', 'MaxDB (SAP DB)');
INSERT INTO `R_DATABASE_TYPE` VALUES ('45', 'VECTORWISE', 'Ingres VectorWise');
INSERT INTO `R_DATABASE_TYPE` VALUES ('46', 'DB2', 'IBM DB2');
INSERT INTO `R_DATABASE_TYPE` VALUES ('47', 'NEOVIEW', 'Neoview');
INSERT INTO `R_DATABASE_TYPE` VALUES ('48', 'SYBASEIQ', 'SybaseIQ');
INSERT INTO `R_DATABASE_TYPE` VALUES ('49', 'REDSHIFT', 'Redshift');
INSERT INTO `R_DATABASE_TYPE` VALUES ('50', 'FIREBIRD', 'Firebird SQL');
INSERT INTO `R_DATABASE_TYPE` VALUES ('51', 'OpenERPDatabaseMeta', 'OpenERP Server');

-- ----------------------------
-- Table structure for R_DEPENDENCY
-- ----------------------------
DROP TABLE IF EXISTS `R_DEPENDENCY`;
CREATE TABLE `R_DEPENDENCY` (
  `ID_DEPENDENCY` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_DATABASE` int(11) DEFAULT NULL,
  `TABLE_NAME` varchar(255) DEFAULT NULL,
  `FIELD_NAME` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_DEPENDENCY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_DEPENDENCY
-- ----------------------------

-- ----------------------------
-- Table structure for R_DIRECTORY
-- ----------------------------
DROP TABLE IF EXISTS `R_DIRECTORY`;
CREATE TABLE `R_DIRECTORY` (
  `ID_DIRECTORY` bigint(20) NOT NULL,
  `ID_DIRECTORY_PARENT` int(11) DEFAULT NULL,
  `DIRECTORY_NAME` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_DIRECTORY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_DIRECTORY
-- ----------------------------

-- ----------------------------
-- Table structure for R_ELEMENT
-- ----------------------------
DROP TABLE IF EXISTS `R_ELEMENT`;
CREATE TABLE `R_ELEMENT` (
  `ID_ELEMENT` bigint(20) NOT NULL,
  `ID_ELEMENT_TYPE` int(11) DEFAULT NULL,
  `NAME` mediumtext,
  PRIMARY KEY (`ID_ELEMENT`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_ELEMENT
-- ----------------------------

-- ----------------------------
-- Table structure for R_ELEMENT_ATTRIBUTE
-- ----------------------------
DROP TABLE IF EXISTS `R_ELEMENT_ATTRIBUTE`;
CREATE TABLE `R_ELEMENT_ATTRIBUTE` (
  `ID_ELEMENT_ATTRIBUTE` bigint(20) NOT NULL,
  `ID_ELEMENT` int(11) DEFAULT NULL,
  `ID_ELEMENT_ATTRIBUTE_PARENT` int(11) DEFAULT NULL,
  `ATTR_KEY` varchar(255) DEFAULT NULL,
  `ATTR_VALUE` mediumtext,
  PRIMARY KEY (`ID_ELEMENT_ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_ELEMENT_ATTRIBUTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_ELEMENT_TYPE
-- ----------------------------
DROP TABLE IF EXISTS `R_ELEMENT_TYPE`;
CREATE TABLE `R_ELEMENT_TYPE` (
  `ID_ELEMENT_TYPE` bigint(20) NOT NULL,
  `ID_NAMESPACE` int(11) DEFAULT NULL,
  `NAME` mediumtext,
  `DESCRIPTION` mediumtext,
  PRIMARY KEY (`ID_ELEMENT_TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_ELEMENT_TYPE
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOB
-- ----------------------------
DROP TABLE IF EXISTS `R_JOB`;
CREATE TABLE `R_JOB` (
  `ID_JOB` bigint(20) NOT NULL,
  `ID_DIRECTORY` int(11) DEFAULT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `DESCRIPTION` mediumtext,
  `EXTENDED_DESCRIPTION` mediumtext,
  `JOB_VERSION` varchar(255) DEFAULT NULL,
  `JOB_STATUS` int(11) DEFAULT NULL,
  `ID_DATABASE_LOG` int(11) DEFAULT NULL,
  `TABLE_NAME_LOG` varchar(255) DEFAULT NULL,
  `CREATED_USER` varchar(255) DEFAULT NULL,
  `CREATED_DATE` datetime DEFAULT NULL,
  `MODIFIED_USER` varchar(255) DEFAULT NULL,
  `MODIFIED_DATE` datetime DEFAULT NULL,
  `USE_BATCH_ID` tinyint(1) DEFAULT NULL,
  `PASS_BATCH_ID` tinyint(1) DEFAULT NULL,
  `USE_LOGFIELD` tinyint(1) DEFAULT NULL,
  `SHARED_FILE` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_JOB`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOB
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOB_ATTRIBUTE
-- ----------------------------
DROP TABLE IF EXISTS `R_JOB_ATTRIBUTE`;
CREATE TABLE `R_JOB_ATTRIBUTE` (
  `ID_JOB_ATTRIBUTE` bigint(20) NOT NULL,
  `ID_JOB` int(11) DEFAULT NULL,
  `NR` int(11) DEFAULT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `VALUE_NUM` bigint(20) DEFAULT NULL,
  `VALUE_STR` mediumtext,
  PRIMARY KEY (`ID_JOB_ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOB_ATTRIBUTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOB_HOP
-- ----------------------------
DROP TABLE IF EXISTS `R_JOB_HOP`;
CREATE TABLE `R_JOB_HOP` (
  `ID_JOB_HOP` bigint(20) NOT NULL,
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_JOBENTRY_COPY_FROM` int(11) DEFAULT NULL,
  `ID_JOBENTRY_COPY_TO` int(11) DEFAULT NULL,
  `ENABLED` tinyint(1) DEFAULT NULL,
  `EVALUATION` tinyint(1) DEFAULT NULL,
  `UNCONDITIONAL` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_JOB_HOP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOB_HOP
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOB_LOCK
-- ----------------------------
DROP TABLE IF EXISTS `R_JOB_LOCK`;
CREATE TABLE `R_JOB_LOCK` (
  `ID_JOB_LOCK` bigint(20) NOT NULL,
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_USER` int(11) DEFAULT NULL,
  `LOCK_MESSAGE` mediumtext,
  `LOCK_DATE` datetime DEFAULT NULL,
  PRIMARY KEY (`ID_JOB_LOCK`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOB_LOCK
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOB_NOTE
-- ----------------------------
DROP TABLE IF EXISTS `R_JOB_NOTE`;
CREATE TABLE `R_JOB_NOTE` (
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_NOTE` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOB_NOTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOBENTRY
-- ----------------------------
DROP TABLE IF EXISTS `R_JOBENTRY`;
CREATE TABLE `R_JOBENTRY` (
  `ID_JOBENTRY` bigint(20) NOT NULL,
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_JOBENTRY_TYPE` int(11) DEFAULT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `DESCRIPTION` mediumtext,
  PRIMARY KEY (`ID_JOBENTRY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOBENTRY
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOBENTRY_ATTRIBUTE
-- ----------------------------
DROP TABLE IF EXISTS `R_JOBENTRY_ATTRIBUTE`;
CREATE TABLE `R_JOBENTRY_ATTRIBUTE` (
  `ID_JOBENTRY_ATTRIBUTE` bigint(20) NOT NULL,
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_JOBENTRY` int(11) DEFAULT NULL,
  `NR` int(11) DEFAULT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `VALUE_NUM` double DEFAULT NULL,
  `VALUE_STR` mediumtext,
  PRIMARY KEY (`ID_JOBENTRY_ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOBENTRY_ATTRIBUTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOBENTRY_COPY
-- ----------------------------
DROP TABLE IF EXISTS `R_JOBENTRY_COPY`;
CREATE TABLE `R_JOBENTRY_COPY` (
  `ID_JOBENTRY_COPY` bigint(20) NOT NULL,
  `ID_JOBENTRY` int(11) DEFAULT NULL,
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_JOBENTRY_TYPE` int(11) DEFAULT NULL,
  `NR` int(11) DEFAULT NULL,
  `GUI_LOCATION_X` int(11) DEFAULT NULL,
  `GUI_LOCATION_Y` int(11) DEFAULT NULL,
  `GUI_DRAW` tinyint(1) DEFAULT NULL,
  `PARALLEL` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_JOBENTRY_COPY`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOBENTRY_COPY
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOBENTRY_DATABASE
-- ----------------------------
DROP TABLE IF EXISTS `R_JOBENTRY_DATABASE`;
CREATE TABLE `R_JOBENTRY_DATABASE` (
  `ID_JOB` int(11) DEFAULT NULL,
  `ID_JOBENTRY` int(11) DEFAULT NULL,
  `ID_DATABASE` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOBENTRY_DATABASE
-- ----------------------------

-- ----------------------------
-- Table structure for R_JOBENTRY_TYPE
-- ----------------------------
DROP TABLE IF EXISTS `R_JOBENTRY_TYPE`;
CREATE TABLE `R_JOBENTRY_TYPE` (
  `ID_JOBENTRY_TYPE` bigint(20) NOT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_JOBENTRY_TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_JOBENTRY_TYPE
-- ----------------------------
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('1', 'WEBSERVICE_AVAILABLE', '检查web服务是否可用');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('2', 'MYSQL_BULK_FILE', '从 Mysql 批量导出到文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('3', 'COPY_MOVE_RESULT_FILENAMES', '复制/移动结果文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('4', 'XSD_VALIDATOR', 'XSD Validator');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('5', 'HadoopCopyFilesPlugin', 'Hadoop Copy Files');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('6', 'SPECIAL', '特殊作业项');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('7', 'FILE_COMPARE', '比较文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('8', 'CREATE_FOLDER', '创建一个目录');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('9', 'MAIL_VALIDATOR', '邮件验证');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('10', 'PALO_CUBE_DELETE', 'Palo Cube Delete');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('11', 'HadoopTransJobExecutorPlugin', 'Pentaho MapReduce');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('12', 'MAIL', '发送邮件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('13', 'TRUNCATE_TABLES', '清空表');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('14', 'MSGBOX_INFO', '显示消息对话框');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('15', 'WAIT_FOR_SQL', '等待SQL');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('16', 'FTPS_GET', 'FTPS 下载');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('17', 'FTP_DELETE', 'FTP 删除');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('18', 'COLUMNS_EXIST', '检查列是否存在');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('19', 'CHECK_FILES_LOCKED', '检查文件是否被锁');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('20', 'UNZIP', '解压缩文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('21', 'JOB', '作业');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('22', 'DELETE_FILE', '删除一个文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('23', 'DataRefineryBuildModel', 'Build Model');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('24', 'SHELL', 'Shell');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('25', 'ABORT', '中止作业');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('26', 'HiveJobExecutorPlugin', 'Amazon Hive Job Executor');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('27', 'SqoopImport', 'Sqoop Import');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('28', 'XML_WELL_FORMED', 'Check if XML file is well formed');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('29', 'SFTP', 'SFTP 下载');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('30', 'HTTP', 'HTTP');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('31', 'FTP_PUT', 'FTP 上传');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('32', 'SQL', 'SQL');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('33', 'WRITE_TO_FILE', '写入文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('34', 'PGP_VERIFY_FILES', '用PGP验证文件签名');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('35', 'DOS_UNIX_CONVERTER', 'DOS和UNIX之间的文本转换');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('36', 'PGP_DECRYPT_FILES', '用PGP解密文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('37', 'HadoopJobExecutorPlugin', 'Hadoop Job Executor ');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('38', 'TALEND_JOB_EXEC', 'Talend 作业执行');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('39', 'EVAL', '使用 JavaScript 脚本验证');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('40', 'DELAY', '等待');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('41', 'HL7MLLPAcknowledge', 'HL7 MLLP Acknowledge');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('42', 'PALO_CUBE_CREATE', 'Palo Cube Create');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('43', 'FTP', 'FTP 下载');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('44', 'FOLDERS_COMPARE', '比较目录');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('45', 'ZIP_FILE', 'Zip 压缩文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('46', 'GET_POP', 'POP 收信');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('47', 'TRANS', '转换');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('48', 'SEND_NAGIOS_PASSIVE_CHECK', '发送Nagios 被动检查');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('49', 'SET_VARIABLES', '设置变量');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('50', 'MS_ACCESS_BULK_LOAD', 'MS Access Bulk Load');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('51', 'HadoopPigScriptExecutorPlugin', 'Pig Script Executor');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('52', 'DummyJob', 'Example Job');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('53', 'COPY_FILES', '复制文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('54', 'EVAL_FILES_METRICS', '计算文件大小或个数');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('55', 'PING', 'Ping 一台主机');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('56', 'ADD_RESULT_FILENAMES', '添加文件到结果文件中');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('57', 'DELETE_FOLDERS', '删除目录');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('58', 'CHECK_DB_CONNECTIONS', '检查数据库连接');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('59', 'FILE_EXISTS', '检查一个文件是否存在');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('60', 'EVAL_TABLE_CONTENT', '计算表中的记录数');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('61', 'FILES_EXIST', '检查多个文件是否存在');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('62', 'SFTPPUT', 'SFTP 上传');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('63', 'FTPS_PUT', 'FTPS 上传');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('64', 'DELETE_RESULT_FILENAMES', '从结果文件中删除文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('65', 'DELETE_FILES', '删除多个文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('66', 'PGP_ENCRYPT_FILES', '用PGP加密文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('67', 'OozieJobExecutor', 'Oozie Job Executor');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('68', 'WRITE_TO_LOG', '写日志');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('69', 'SUCCESS', '成功');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('70', 'WAIT_FOR_FILE', '等待文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('71', 'MSSQL_BULK_LOAD', 'SQLServer 批量加载');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('72', 'TELNET', '远程登录一台主机');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('73', 'MOVE_FILES', '移动文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('74', 'XSLT', 'XSL Transformation');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('75', 'EMRJobExecutorPlugin', 'Amazon EMR Job Executor');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('76', 'DTD_VALIDATOR', 'DTD Validator');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('77', 'DATASOURCE_PUBLISH', 'Publish Model');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('78', 'SqoopExport', 'Sqoop Export');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('79', 'HL7MLLPInput', 'HL7 MLLP Input');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('80', 'FOLDER_IS_EMPTY', '检查目录是否为空');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('81', 'SIMPLE_EVAL', '检验字段的值');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('82', 'SparkSubmit', 'Spark Submit');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('83', 'EXPORT_REPOSITORY', '导出资源库到XML文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('84', 'TABLE_EXISTS', '检查表是否存在');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('85', 'SYSLOG', '用 Syslog 发送信息');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('86', 'MYSQL_BULK_LOAD', 'Mysql 批量加载');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('87', 'CREATE_FILE', '创建文件');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('88', 'SNMP_TRAP', '发送 SNMP 自陷');
INSERT INTO `R_JOBENTRY_TYPE` VALUES ('89', 'CONNECTED_TO_REPOSITORY', '检查是否连接到资源库');

-- ----------------------------
-- Table structure for R_LOG
-- ----------------------------
DROP TABLE IF EXISTS `R_LOG`;
CREATE TABLE `R_LOG` (
  `ID_LOG` bigint(20) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `ID_LOGLEVEL` int(11) DEFAULT NULL,
  `LOGTYPE` varchar(255) DEFAULT NULL,
  `FILENAME` varchar(255) DEFAULT NULL,
  `FILEEXTENTION` varchar(255) DEFAULT NULL,
  `ADD_DATE` tinyint(1) DEFAULT NULL,
  `ADD_TIME` tinyint(1) DEFAULT NULL,
  `ID_DATABASE_LOG` int(11) DEFAULT NULL,
  `TABLE_NAME_LOG` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_LOG`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_LOG
-- ----------------------------

-- ----------------------------
-- Table structure for R_LOGLEVEL
-- ----------------------------
DROP TABLE IF EXISTS `R_LOGLEVEL`;
CREATE TABLE `R_LOGLEVEL` (
  `ID_LOGLEVEL` bigint(20) NOT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_LOGLEVEL`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_LOGLEVEL
-- ----------------------------
INSERT INTO `R_LOGLEVEL` VALUES ('1', 'Error', '错误日志');
INSERT INTO `R_LOGLEVEL` VALUES ('2', 'Minimal', '最小日志');
INSERT INTO `R_LOGLEVEL` VALUES ('3', 'Basic', '基本日志');
INSERT INTO `R_LOGLEVEL` VALUES ('4', 'Detailed', '详细日志');
INSERT INTO `R_LOGLEVEL` VALUES ('5', 'Debug', '调试');
INSERT INTO `R_LOGLEVEL` VALUES ('6', 'Rowlevel', '行级日志(非常详细)');

-- ----------------------------
-- Table structure for R_NAMESPACE
-- ----------------------------
DROP TABLE IF EXISTS `R_NAMESPACE`;
CREATE TABLE `R_NAMESPACE` (
  `ID_NAMESPACE` bigint(20) NOT NULL,
  `NAME` mediumtext,
  PRIMARY KEY (`ID_NAMESPACE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_NAMESPACE
-- ----------------------------

-- ----------------------------
-- Table structure for R_NOTE
-- ----------------------------
DROP TABLE IF EXISTS `R_NOTE`;
CREATE TABLE `R_NOTE` (
  `ID_NOTE` bigint(20) NOT NULL,
  `VALUE_STR` mediumtext,
  `GUI_LOCATION_X` int(11) DEFAULT NULL,
  `GUI_LOCATION_Y` int(11) DEFAULT NULL,
  `GUI_LOCATION_WIDTH` int(11) DEFAULT NULL,
  `GUI_LOCATION_HEIGHT` int(11) DEFAULT NULL,
  `FONT_NAME` mediumtext,
  `FONT_SIZE` int(11) DEFAULT NULL,
  `FONT_BOLD` tinyint(1) DEFAULT NULL,
  `FONT_ITALIC` tinyint(1) DEFAULT NULL,
  `FONT_COLOR_RED` int(11) DEFAULT NULL,
  `FONT_COLOR_GREEN` int(11) DEFAULT NULL,
  `FONT_COLOR_BLUE` int(11) DEFAULT NULL,
  `FONT_BACK_GROUND_COLOR_RED` int(11) DEFAULT NULL,
  `FONT_BACK_GROUND_COLOR_GREEN` int(11) DEFAULT NULL,
  `FONT_BACK_GROUND_COLOR_BLUE` int(11) DEFAULT NULL,
  `FONT_BORDER_COLOR_RED` int(11) DEFAULT NULL,
  `FONT_BORDER_COLOR_GREEN` int(11) DEFAULT NULL,
  `FONT_BORDER_COLOR_BLUE` int(11) DEFAULT NULL,
  `DRAW_SHADOW` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_NOTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_NOTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_PARTITION
-- ----------------------------
DROP TABLE IF EXISTS `R_PARTITION`;
CREATE TABLE `R_PARTITION` (
  `ID_PARTITION` bigint(20) NOT NULL,
  `ID_PARTITION_SCHEMA` int(11) DEFAULT NULL,
  `PARTITION_ID` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_PARTITION`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_PARTITION
-- ----------------------------

-- ----------------------------
-- Table structure for R_PARTITION_SCHEMA
-- ----------------------------
DROP TABLE IF EXISTS `R_PARTITION_SCHEMA`;
CREATE TABLE `R_PARTITION_SCHEMA` (
  `ID_PARTITION_SCHEMA` bigint(20) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `DYNAMIC_DEFINITION` tinyint(1) DEFAULT NULL,
  `PARTITIONS_PER_SLAVE` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_PARTITION_SCHEMA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_PARTITION_SCHEMA
-- ----------------------------

-- ----------------------------
-- Table structure for R_REPOSITORY_LOG
-- ----------------------------
DROP TABLE IF EXISTS `R_REPOSITORY_LOG`;
CREATE TABLE `R_REPOSITORY_LOG` (
  `ID_REPOSITORY_LOG` bigint(20) NOT NULL,
  `REP_VERSION` varchar(255) DEFAULT NULL,
  `LOG_DATE` datetime DEFAULT NULL,
  `LOG_USER` varchar(255) DEFAULT NULL,
  `OPERATION_DESC` mediumtext,
  PRIMARY KEY (`ID_REPOSITORY_LOG`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_REPOSITORY_LOG
-- ----------------------------
INSERT INTO `R_REPOSITORY_LOG` VALUES ('1', '5.0', '2017-10-17 10:51:19', 'admin', 'Creation of the Kettle repository');
INSERT INTO `R_REPOSITORY_LOG` VALUES ('2', '5.0', '2017-10-17 10:52:41', 'admin', 'Creating slave server [master1]');
INSERT INTO `R_REPOSITORY_LOG` VALUES ('3', '5.0', '2017-10-17 10:52:41', 'admin', 'Creation of initial version');
INSERT INTO `R_REPOSITORY_LOG` VALUES ('4', '5.0', '2017-10-17 10:53:00', 'admin', 'Creating slave server [master2]');
INSERT INTO `R_REPOSITORY_LOG` VALUES ('5', '5.0', '2017-10-17 10:53:00', 'admin', 'Creation of initial version');

-- ----------------------------
-- Table structure for R_SLAVE
-- ----------------------------
DROP TABLE IF EXISTS `R_SLAVE`;
CREATE TABLE `R_SLAVE` (
  `ID_SLAVE` bigint(20) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `HOST_NAME` varchar(255) DEFAULT NULL,
  `PORT` varchar(255) DEFAULT NULL,
  `WEB_APP_NAME` varchar(255) DEFAULT NULL,
  `USERNAME` varchar(255) DEFAULT NULL,
  `PASSWORD` varchar(255) DEFAULT NULL,
  `PROXY_HOST_NAME` varchar(255) DEFAULT NULL,
  `PROXY_PORT` varchar(255) DEFAULT NULL,
  `NON_PROXY_HOSTS` varchar(255) DEFAULT NULL,
  `MASTER` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_SLAVE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_SLAVE
-- ----------------------------
INSERT INTO `R_SLAVE` VALUES ('1', 'master1', '10.23.13.183', '8080', null, 'cluster', 'Encrypted 2be98afc86aa7f2e4cb1aa265cd86aac8', null, null, null, '1');
INSERT INTO `R_SLAVE` VALUES ('2', 'master2', '10.23.13.184', '8080', null, 'cluster', 'Encrypted 2be98afc86aa7f2e4cb1aa265cd86aac8', null, null, null, '1');

-- ----------------------------
-- Table structure for R_STEP
-- ----------------------------
DROP TABLE IF EXISTS `R_STEP`;
CREATE TABLE `R_STEP` (
  `ID_STEP` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `DESCRIPTION` mediumtext,
  `ID_STEP_TYPE` int(11) DEFAULT NULL,
  `DISTRIBUTE` tinyint(1) DEFAULT NULL,
  `COPIES` int(11) DEFAULT NULL,
  `GUI_LOCATION_X` int(11) DEFAULT NULL,
  `GUI_LOCATION_Y` int(11) DEFAULT NULL,
  `GUI_DRAW` tinyint(1) DEFAULT NULL,
  `COPIES_STRING` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_STEP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_STEP
-- ----------------------------

-- ----------------------------
-- Table structure for R_STEP_ATTRIBUTE
-- ----------------------------
DROP TABLE IF EXISTS `R_STEP_ATTRIBUTE`;
CREATE TABLE `R_STEP_ATTRIBUTE` (
  `ID_STEP_ATTRIBUTE` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_STEP` int(11) DEFAULT NULL,
  `NR` int(11) DEFAULT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `VALUE_NUM` bigint(20) DEFAULT NULL,
  `VALUE_STR` mediumtext,
  PRIMARY KEY (`ID_STEP_ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_STEP_ATTRIBUTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_STEP_DATABASE
-- ----------------------------
DROP TABLE IF EXISTS `R_STEP_DATABASE`;
CREATE TABLE `R_STEP_DATABASE` (
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_STEP` int(11) DEFAULT NULL,
  `ID_DATABASE` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_STEP_DATABASE
-- ----------------------------

-- ----------------------------
-- Table structure for R_STEP_TYPE
-- ----------------------------
DROP TABLE IF EXISTS `R_STEP_TYPE`;
CREATE TABLE `R_STEP_TYPE` (
  `ID_STEP_TYPE` bigint(20) NOT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  `HELPTEXT` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`ID_STEP_TYPE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_STEP_TYPE
-- ----------------------------
INSERT INTO `R_STEP_TYPE` VALUES ('1', 'Delete', '删除', '基于关键字删除记录');
INSERT INTO `R_STEP_TYPE` VALUES ('2', 'WriteToLog', '写日志', 'Write data to log');
INSERT INTO `R_STEP_TYPE` VALUES ('3', 'SyslogMessage', '发送信息至Syslog', 'Send message to Syslog server');
INSERT INTO `R_STEP_TYPE` VALUES ('4', 'TextFileOutput', '文本文件输出', '写记录到一个文本文件.');
INSERT INTO `R_STEP_TYPE` VALUES ('5', 'MultiwayMergeJoin', 'Multiway Merge Join', 'Multiway Merge Join');
INSERT INTO `R_STEP_TYPE` VALUES ('6', 'LDAPOutput', 'LDAP 输出', 'Perform Insert, upsert, update, add or delete operations on records based on their DN (Distinguished  Name).');
INSERT INTO `R_STEP_TYPE` VALUES ('7', 'SAPINPUT', 'SAP Input', 'Read data from SAP ERP, optionally with parameters');
INSERT INTO `R_STEP_TYPE` VALUES ('8', 'AccessInput', 'Access 输入', 'Read data from a Microsoft Access file');
INSERT INTO `R_STEP_TYPE` VALUES ('9', 'JoinRows', '记录关联 (笛卡尔输出)', '这个步骤的输出是输入流的笛卡尔的结果.{0} 输出结果的记录数是输入流记录之间的乘积.');
INSERT INTO `R_STEP_TYPE` VALUES ('10', 'JsonInput', 'JSON Input', 'Extract relevant portions out of JSON structures (file or incoming field) and output rows');
INSERT INTO `R_STEP_TYPE` VALUES ('11', 'TableOutput', '表输出', '写信息到一个数据库表');
INSERT INTO `R_STEP_TYPE` VALUES ('12', 'MySQLBulkLoader', 'MySQL 批量加载', 'MySQL bulk loader step, loading data over a named pipe (not available on MS Windows)');
INSERT INTO `R_STEP_TYPE` VALUES ('13', 'SingleThreader', 'Single Threader', 'Executes a transformation snippet in a single thread.  You need a standard mapping or a transformation with an Injector step where data from the parent transformation will arive in blocks.');
INSERT INTO `R_STEP_TYPE` VALUES ('14', 'HL7Input', 'HL7 Input', 'Reads and parses HL7 messages and outputs a series of values from the messages');
INSERT INTO `R_STEP_TYPE` VALUES ('15', 'RandomCCNumberGenerator', '生成随机的信用卡号', 'Generate random valide (luhn check) credit card numbers');
INSERT INTO `R_STEP_TYPE` VALUES ('16', 'RegexEval', '正则表达式', 'Regular expression Evaluation\nThis step uses a regular expression to evaluate a field. It can also extract new fields out of an existing field with capturing groups.');
INSERT INTO `R_STEP_TYPE` VALUES ('17', 'PaloDimOutput', 'Palo Dim Output', 'Writes data to defined Palo Dimension');
INSERT INTO `R_STEP_TYPE` VALUES ('18', 'S3FileOutputPlugin', 'S3 File Output', 'Create files in an S3 location');
INSERT INTO `R_STEP_TYPE` VALUES ('19', 'IfNull', '替换NULL值', 'Sets a field value to a constant if it is null.');
INSERT INTO `R_STEP_TYPE` VALUES ('20', 'ColumnExists', '检查表里的列是否存在', 'Check if a column exists in a table on a specified connection.');
INSERT INTO `R_STEP_TYPE` VALUES ('21', 'FileLocked', '检查文件是否已被锁定', 'Check if a file is locked by another process');
INSERT INTO `R_STEP_TYPE` VALUES ('22', 'SocketWriter', 'Socket 写', 'Socket writer.  A socket server that can send rows of data to a socket reader.');
INSERT INTO `R_STEP_TYPE` VALUES ('23', 'ExcelInput', 'Excel输入', '从一个微软的Excel文件里读取数据. 兼容Excel 95, 97 and 2000.');
INSERT INTO `R_STEP_TYPE` VALUES ('24', 'TeraFast', 'Teradata Fastload 批量加载', 'The Teradata Fastload Bulk loader');
INSERT INTO `R_STEP_TYPE` VALUES ('25', 'DataGrid', '自定义常量数据', 'Enter rows of static data in a grid, usually for testing, reference or demo purpose');
INSERT INTO `R_STEP_TYPE` VALUES ('26', 'DetectLastRow', '识别流的最后一行', 'Last row will be marked');
INSERT INTO `R_STEP_TYPE` VALUES ('27', 'TypeExitEdi2XmlStep', 'Edi to XML', 'Converts Edi text to generic XML');
INSERT INTO `R_STEP_TYPE` VALUES ('28', 'SortRows', '排序记录', '基于字段值把记录排序(升序或降序)');
INSERT INTO `R_STEP_TYPE` VALUES ('29', 'FieldsChangeSequence', '根据字段值来改变序列', 'Add sequence depending of fields value change.\nEach time value of at least one field change, PDI will reset sequence. ');
INSERT INTO `R_STEP_TYPE` VALUES ('30', 'RowsFromResult', '从结果获取记录', '这个允许你从同一个任务的前一个条目里读取记录.');
INSERT INTO `R_STEP_TYPE` VALUES ('31', 'JobExecutor', '执行作业', 'This step executes a Pentaho Data Integration job, sets parameters and passes rows.');
INSERT INTO `R_STEP_TYPE` VALUES ('32', 'BlockUntilStepsFinish', '阻塞数据直到步骤都完成', 'Block this step until selected steps finish.');
INSERT INTO `R_STEP_TYPE` VALUES ('33', 'AutoDoc', '自动文档输出', 'This step automatically generates documentation based on input in the form of a list of transformations and jobs');
INSERT INTO `R_STEP_TYPE` VALUES ('34', 'SFTPPut', 'SFTP Put', 'Upload a file or a stream file to remote host via SFTP');
INSERT INTO `R_STEP_TYPE` VALUES ('35', 'ProcessFiles', '处理文件', 'Process one file per row (copy or move or delete).\nThis step only accept filename in input.');
INSERT INTO `R_STEP_TYPE` VALUES ('36', 'RuleAccumulator', 'Rules Accumulator', 'Rules Accumulator Step');
INSERT INTO `R_STEP_TYPE` VALUES ('37', 'DimensionLookup', '维度查询/更新', '在一个数据仓库里更新一个渐变维 {0} 或者在这个维里查询信息.');
INSERT INTO `R_STEP_TYPE` VALUES ('38', 'TypeExitGoogleAnalyticsInputStep', 'Google Analytics', 'Fetches data from google analytics account');
INSERT INTO `R_STEP_TYPE` VALUES ('39', 'PaloCellInput', 'Palo Cell Input', 'Reads data from a defined Palo Cube ');
INSERT INTO `R_STEP_TYPE` VALUES ('40', 'Append', '追加流', 'Append 2 streams in an ordered way');
INSERT INTO `R_STEP_TYPE` VALUES ('41', 'ZipFile', 'Zip 文件', 'Zip a file.\nFilename will be extracted from incoming stream.');
INSERT INTO `R_STEP_TYPE` VALUES ('42', 'StringCut', '剪切字符串', 'Strings cut (substring).');
INSERT INTO `R_STEP_TYPE` VALUES ('43', 'MergeRows', '合并记录', '合并两个数据流, 并根据某个关键字排序.  这两个数据流被比较，以标识相等的、变更的、删除的和新建的记录.');
INSERT INTO `R_STEP_TYPE` VALUES ('44', 'Validator', '数据检验', 'Validates passing data based on a set of rules');
INSERT INTO `R_STEP_TYPE` VALUES ('45', 'SSTableOutput', 'SSTable Output', 'Writes to a filesystem directory as a Cassandra SSTable');
INSERT INTO `R_STEP_TYPE` VALUES ('46', 'MemoryGroupBy', '在内存中分组', 'Builds aggregates in a group by fashion.\nThis step doesn\'t require sorted input.');
INSERT INTO `R_STEP_TYPE` VALUES ('47', 'SystemInfo', '获取系统信息', '获取系统信息，例如时间、日期.');
INSERT INTO `R_STEP_TYPE` VALUES ('48', 'Denormaliser', '列转行', 'Denormalises rows by looking up key-value pairs and by assigning them to new fields in the输出 rows.{0}This method aggregates and needs the输入 rows to be sorted on the grouping fields');
INSERT INTO `R_STEP_TYPE` VALUES ('49', 'ExcelOutput', 'Excel输出', 'Stores records into an Excel (XLS) document with formatting information.');
INSERT INTO `R_STEP_TYPE` VALUES ('50', 'OlapInput', 'OLAP 输入', 'Execute and retrieve data using an MDX query against any XML/A OLAP datasource using olap4j');
INSERT INTO `R_STEP_TYPE` VALUES ('51', 'Delay', '延迟行', 'Output each input row after a delay');
INSERT INTO `R_STEP_TYPE` VALUES ('52', 'NullIf', '设置值为NULL', '如果一个字段值等于某个固定值，那么把这个字段值设置成null');
INSERT INTO `R_STEP_TYPE` VALUES ('53', 'SalesforceInput', 'Salesforce Input', 'Extract data from Salesforce');
INSERT INTO `R_STEP_TYPE` VALUES ('54', 'CallEndpointStep', 'Call endpoint', 'Call an endpoint of the Pentaho Server.');
INSERT INTO `R_STEP_TYPE` VALUES ('55', 'SocketReader', 'Socket 读', 'Socket reader.  A socket client that connects to a server (Socket Writer step).');
INSERT INTO `R_STEP_TYPE` VALUES ('56', 'CombinationLookup', '联合查询/更新', '更新数据仓库里的一个junk维 {0} 可选的, 科研查询维里的信息.{1}junk维的主键是所有的字段.');
INSERT INTO `R_STEP_TYPE` VALUES ('57', 'FilesToResult', '复制文件到结果', 'This step allows you to set filenames in the result of this transformation.\nSubsequent job entries can then use this information.');
INSERT INTO `R_STEP_TYPE` VALUES ('58', 'GPBulkLoader', 'Greenplum Bulk Loader', 'Greenplum Bulk Loader');
INSERT INTO `R_STEP_TYPE` VALUES ('59', 'VerticaBulkLoader', 'Vertica Bulk Loader', 'Bulk load data into a Vertica database table');
INSERT INTO `R_STEP_TYPE` VALUES ('60', 'SalesforceDelete', 'Salesforce Delete', 'Delete records in Salesforce module.');
INSERT INTO `R_STEP_TYPE` VALUES ('61', 'HTTP', 'HTTP client', 'Call a web service over HTTP by supplying a base URL by allowing parameters to be set dynamically');
INSERT INTO `R_STEP_TYPE` VALUES ('62', 'XSDValidator', 'XSD Validator', 'Validate XML source (files or streams) against XML Schema Definition.');
INSERT INTO `R_STEP_TYPE` VALUES ('63', 'SetValueConstant', '将字段值设置为常量', 'Set value of a field to a constant');
INSERT INTO `R_STEP_TYPE` VALUES ('64', 'ScriptValueMod', 'JavaScript代码', 'This is a modified plugin for the Scripting Values with improved interface and performance.\nWritten & donated to open source by Martin Lange, Proconis : http://www.proconis.de');
INSERT INTO `R_STEP_TYPE` VALUES ('65', 'CassandraInput', 'Cassandra Input', 'Reads data from a Cassandra table');
INSERT INTO `R_STEP_TYPE` VALUES ('66', 'SwitchCase', 'Switch / Case', 'Switch a row to a certain target step based on the case value in a field.');
INSERT INTO `R_STEP_TYPE` VALUES ('67', 'DBLookup', '数据库查询', '使用字段值在数据库里查询值');
INSERT INTO `R_STEP_TYPE` VALUES ('68', 'FuzzyMatch', '模糊匹配', 'Finding approximate matches to a string using matching algorithms.\nRead a field from a main stream and output approximative value from lookup stream.');
INSERT INTO `R_STEP_TYPE` VALUES ('69', 'TeraDataBulkLoader', 'Teradata TPT Bulk Loader', 'Teradata TPT bulkloader, using tbuild command');
INSERT INTO `R_STEP_TYPE` VALUES ('70', 'TextFileInput', '文本文件输入', '从一个文本文件（几种格式）里读取数据{0}这些数据可以被传递到下一个步骤里...');
INSERT INTO `R_STEP_TYPE` VALUES ('71', 'SalesforceInsert', 'Salesforce Insert', 'Insert records in Salesforce module.');
INSERT INTO `R_STEP_TYPE` VALUES ('72', 'CouchDbInput', 'CouchDb Input', 'Reads from a Couch DB view');
INSERT INTO `R_STEP_TYPE` VALUES ('73', 'ShapeFileReader', 'ESRI Shapefile Reader', 'Reads shape file data from an ESRI shape file and linked DBF file');
INSERT INTO `R_STEP_TYPE` VALUES ('74', 'RssInput', 'RSS 输入', 'Read RSS feeds');
INSERT INTO `R_STEP_TYPE` VALUES ('75', 'Unique', '去除重复记录', '去除重复的记录行，保持记录唯一{0}这个仅仅基于一个已经排好序的输入.{1}如果输入没有排序, 仅仅两个连续的记录行被正确处理.');
INSERT INTO `R_STEP_TYPE` VALUES ('76', 'InfobrightOutput', 'Infobright 批量加载', 'Load data to an Infobright database table');
INSERT INTO `R_STEP_TYPE` VALUES ('77', 'UserDefinedJavaClass', 'Java 代码', 'This step allows you to program a step using Java code');
INSERT INTO `R_STEP_TYPE` VALUES ('78', 'HBaseRowDecoder', 'HBase Row Decoder', 'Decodes an incoming key and HBase result object according to a mapping ');
INSERT INTO `R_STEP_TYPE` VALUES ('79', 'OraBulkLoader', 'Oracle 批量加载', 'Use Oracle Bulk Loader to load data');
INSERT INTO `R_STEP_TYPE` VALUES ('80', 'StepsMetrics', '转换步骤信息统计', 'Return metrics for one or several steps');
INSERT INTO `R_STEP_TYPE` VALUES ('81', 'HadoopEnterPlugin', 'MapReduce Input', 'Enter a Hadoop Mapper or Reducer transformation');
INSERT INTO `R_STEP_TYPE` VALUES ('82', 'AvroInput', 'Avro Input', 'Reads data from an Avro file');
INSERT INTO `R_STEP_TYPE` VALUES ('83', 'GetTableNames', '获取表名', 'Get table names from database connection and send them to the next step');
INSERT INTO `R_STEP_TYPE` VALUES ('84', 'SetSessionVariableStep', 'Set session variables', 'Set session variables in the current user session.');
INSERT INTO `R_STEP_TYPE` VALUES ('85', 'MappingInput', '映射输入规范', '指定一个映射的字段输入');
INSERT INTO `R_STEP_TYPE` VALUES ('86', 'Abort', '中止', 'Abort a transformation');
INSERT INTO `R_STEP_TYPE` VALUES ('87', 'CreateSharedDimensions', 'Shared Dimension', 'Create shared dimensions for use with Streamlined Data Refinery.');
INSERT INTO `R_STEP_TYPE` VALUES ('88', 'HBaseInput', 'HBase Input', 'Reads data from a HBase table according to a mapping ');
INSERT INTO `R_STEP_TYPE` VALUES ('89', 'PropertyOutput', '配置文件输出', 'Write data to properties file');
INSERT INTO `R_STEP_TYPE` VALUES ('90', 'JsonOutput', 'JSON Output', 'Create JSON block and output it in a field or a file.');
INSERT INTO `R_STEP_TYPE` VALUES ('91', 'TableExists', '检查表是否存在', 'Check if a table exists on a specified connection');
INSERT INTO `R_STEP_TYPE` VALUES ('92', 'DetectEmptyStream', '检测空流', 'This step will output one empty row if input stream is empty\n(ie when input stream does not contain any row)');
INSERT INTO `R_STEP_TYPE` VALUES ('93', 'GPLoad', 'Greenplum Load', 'Greenplum Load');
INSERT INTO `R_STEP_TYPE` VALUES ('94', 'PaloDimInput', 'Palo Dim Input', 'Reads data from a defined Palo Dimension');
INSERT INTO `R_STEP_TYPE` VALUES ('95', 'Normaliser', '行转列', 'De-normalised information can be normalised using this step type.');
INSERT INTO `R_STEP_TYPE` VALUES ('96', 'SelectValues', '字段选择', '选择或移除记录里的字。{0}此外，可以设置字段的元数据: 类型, 长度和精度.');
INSERT INTO `R_STEP_TYPE` VALUES ('97', 'XMLInputStream', 'XML Input Stream (StAX)', 'This step is capable of processing very large and complex XML files very fast.');
INSERT INTO `R_STEP_TYPE` VALUES ('98', 'Flattener', '行扁平化', 'Flattens consequetive rows based on the order in which they appear in the输入 stream');
INSERT INTO `R_STEP_TYPE` VALUES ('99', 'InsertUpdate', '插入 / 更新', '基于关键字更新或插入记录到数据库.');
INSERT INTO `R_STEP_TYPE` VALUES ('100', 'MongoDbOutput', 'MongoDB Output', 'Writes to a Mongo DB collection');
INSERT INTO `R_STEP_TYPE` VALUES ('101', 'MondrianInput', 'Mondrian 输入', 'Execute and retrieve data using an MDX query against a Pentaho Analyses OLAP server (Mondrian)');
INSERT INTO `R_STEP_TYPE` VALUES ('102', 'SQLFileOutput', 'SQL 文件输出', 'Output SQL INSERT statements to file');
INSERT INTO `R_STEP_TYPE` VALUES ('103', 'ReplaceString', '字符串替换', 'Replace all occurences a word in a string with another word.');
INSERT INTO `R_STEP_TYPE` VALUES ('104', 'CubeInput', 'Cube 文件输入', '从一个cube读取记录.');
INSERT INTO `R_STEP_TYPE` VALUES ('105', 'WebServiceLookup', 'Web 服务查询', '使用 Web 服务查询信息');
INSERT INTO `R_STEP_TYPE` VALUES ('106', 'TableCompare', '比较表', 'Compares 2 tables and gives back a list of differences');
INSERT INTO `R_STEP_TYPE` VALUES ('107', 'MailValidator', '检验邮件地址', 'Check if an email address is valid.');
INSERT INTO `R_STEP_TYPE` VALUES ('108', 'OpenERPObjectOutputImport', 'OpenERP Object Output', 'Writes data into OpenERP objects using the object import procedure');
INSERT INTO `R_STEP_TYPE` VALUES ('109', 'RowGenerator', '生成记录', '产生一些空记录或相等的行.');
INSERT INTO `R_STEP_TYPE` VALUES ('110', 'DBJoin', '数据库连接', '使用数据流里的值作为参数执行一个数据库查询');
INSERT INTO `R_STEP_TYPE` VALUES ('111', 'RuleExecutor', 'Rules Executor', 'Rules Executor Step');
INSERT INTO `R_STEP_TYPE` VALUES ('112', 'MergeJoin', '记录集连接', 'Joins two streams on a given key and outputs a joined set. The input streams must be sorted on the join key');
INSERT INTO `R_STEP_TYPE` VALUES ('113', 'OpenERPObjectInput', 'OpenERP Object Input', 'Reads data from OpenERP objects');
INSERT INTO `R_STEP_TYPE` VALUES ('114', 'SalesforceUpdate', 'Salesforce Update', 'Update records in Salesforce module.');
INSERT INTO `R_STEP_TYPE` VALUES ('115', 'XMLJoin', 'XML Join', 'Joins a stream of XML-Tags into a target XML string');
INSERT INTO `R_STEP_TYPE` VALUES ('116', 'GetVariable', '获取变量', 'Determine the values of certain (environment or Kettle) variables and put them in field values.');
INSERT INTO `R_STEP_TYPE` VALUES ('117', 'DBProc', '调用DB存储过程', '通过调用数据库存储过程获得返回值.');
INSERT INTO `R_STEP_TYPE` VALUES ('118', 'RssOutput', 'RSS 输出', 'Read RSS stream.');
INSERT INTO `R_STEP_TYPE` VALUES ('119', 'S3CSVINPUT', 'S3 CSV Input', 'Is capable of reading CSV data stored on Amazon S3 in parallel');
INSERT INTO `R_STEP_TYPE` VALUES ('120', 'SimpleMapping', 'Simple Mapping (sub-transformation)', 'Run a mapping (sub-transformation), use MappingInput and MappingOutput to specify the fields interface.  This is the simplified version only allowing one input and one output data set.');
INSERT INTO `R_STEP_TYPE` VALUES ('121', 'OpenERPObjectDelete', 'OpenERP Object Delete', 'Deletes OpenERP objects');
INSERT INTO `R_STEP_TYPE` VALUES ('122', 'LDAPInput', 'LDAP 输入', 'Read data from LDAP host');
INSERT INTO `R_STEP_TYPE` VALUES ('123', 'XBaseInput', 'XBase输入', '从一个XBase类型的文件(DBF)读取记录');
INSERT INTO `R_STEP_TYPE` VALUES ('124', 'CheckSum', '增加校验列', 'Add a checksum column for each input row');
INSERT INTO `R_STEP_TYPE` VALUES ('125', 'ParallelGzipCsvInput', 'GZIP CSV Input', 'Parallel GZIP CSV file input reader');
INSERT INTO `R_STEP_TYPE` VALUES ('126', 'SortedMerge', '排序合并', 'Sorted Merge');
INSERT INTO `R_STEP_TYPE` VALUES ('127', 'ChangeFileEncoding', '改变文件编码', 'Change file encoding and create a new file');
INSERT INTO `R_STEP_TYPE` VALUES ('128', 'LoadFileInput', '文件内容加载至内存', 'Load file content in memory');
INSERT INTO `R_STEP_TYPE` VALUES ('129', 'Janino', '利用Janino计算Java表达式', 'Calculate the result of a Java Expression using Janino');
INSERT INTO `R_STEP_TYPE` VALUES ('130', 'getXMLData', 'Get data from XML', 'Get data from XML file by using XPath.\n This step also allows you to parse XML defined in a previous field.');
INSERT INTO `R_STEP_TYPE` VALUES ('131', 'LDIFInput', 'LDIF 输入', 'Read data from LDIF files');
INSERT INTO `R_STEP_TYPE` VALUES ('132', 'PGBulkLoader', 'PostgreSQL 批量加载', 'PostgreSQL Bulk Loader');
INSERT INTO `R_STEP_TYPE` VALUES ('133', 'CubeOutput', 'Cube输出', '把数据写入一个cube');
INSERT INTO `R_STEP_TYPE` VALUES ('134', 'SymmetricCryptoTrans', '对称加密', 'Encrypt or decrypt a string using symmetric encryption.\nAvailable algorithms are DES, AES, TripleDES.');
INSERT INTO `R_STEP_TYPE` VALUES ('135', 'AddXML', 'Add XML', 'Encode several fields into an XML fragment');
INSERT INTO `R_STEP_TYPE` VALUES ('136', 'HBaseOutput', 'HBase Output', 'Writes data to an HBase table according to a mapping');
INSERT INTO `R_STEP_TYPE` VALUES ('137', 'TableInput', '表输入', '从数据库表里读取信息.');
INSERT INTO `R_STEP_TYPE` VALUES ('138', 'SASInput', 'SAS 输入', 'This step reads files in sas7bdat (SAS) native format');
INSERT INTO `R_STEP_TYPE` VALUES ('139', 'AccessOutput', 'Access 输出', 'Stores records into an MS-Access database table.');
INSERT INTO `R_STEP_TYPE` VALUES ('140', 'ElasticSearchBulk', 'ElasticSearch Bulk Insert', 'Performs bulk inserts into ElasticSearch');
INSERT INTO `R_STEP_TYPE` VALUES ('141', 'HadoopExitPlugin', 'MapReduce Output', 'Exit a Hadoop Mapper or Reducer transformation ');
INSERT INTO `R_STEP_TYPE` VALUES ('142', 'GetFileNames', '获取文件名', 'Get file names from the operating system and send them to the next step.');
INSERT INTO `R_STEP_TYPE` VALUES ('143', 'StringOperations', '字符串操作', 'Apply certain operations like trimming, padding and others to string value.');
INSERT INTO `R_STEP_TYPE` VALUES ('144', 'SetVariable', '设置变量', 'Set environment variables based on a single input row.');
INSERT INTO `R_STEP_TYPE` VALUES ('145', 'RandomValue', '生成随机数', 'Generate random value');
INSERT INTO `R_STEP_TYPE` VALUES ('146', 'GetSessionVariableStep', 'Get session variables', 'Get session variables from the current user session.');
INSERT INTO `R_STEP_TYPE` VALUES ('147', 'FieldSplitter', '拆分字段', '当你想把一个字段拆分成多个时，使用这个类型.');
INSERT INTO `R_STEP_TYPE` VALUES ('148', 'BlockingStep', '阻塞数据', 'This step blocks until all incoming rows have been processed.  Subsequent steps only recieve the last input row to this step.');
INSERT INTO `R_STEP_TYPE` VALUES ('149', 'CsvInput', 'CSV文件输入', 'Simple CSV file input');
INSERT INTO `R_STEP_TYPE` VALUES ('150', 'PropertyInput', '配置文件输入', 'Read data (key, value) from properties files.');
INSERT INTO `R_STEP_TYPE` VALUES ('151', 'SynchronizeAfterMerge', '数据同步', 'This step perform insert/update/delete in one go based on the value of a field. ');
INSERT INTO `R_STEP_TYPE` VALUES ('152', 'StreamLookup', '流查询', '从转换中的其它流里查询值.');
INSERT INTO `R_STEP_TYPE` VALUES ('153', 'Dummy', '空操作 (什么也不做)', '这个步骤类型什么都不作.{0} 当你想测试或拆分数据流的时候有用.');
INSERT INTO `R_STEP_TYPE` VALUES ('154', 'PGPEncryptStream', 'PGP Encrypt stream', 'Encrypt data stream with PGP');
INSERT INTO `R_STEP_TYPE` VALUES ('155', 'GetFilesRowsCount', '获取文件行数', 'Returns rows count for text files.');
INSERT INTO `R_STEP_TYPE` VALUES ('156', 'SetValueField', '设置字段值', 'Set value of a field with another value field');
INSERT INTO `R_STEP_TYPE` VALUES ('157', 'PGPDecryptStream', 'PGP Decrypt stream', 'Decrypt data stream with PGP');
INSERT INTO `R_STEP_TYPE` VALUES ('158', 'MongoDbInput', 'MongoDB Input', 'Reads from a Mongo DB collection');
INSERT INTO `R_STEP_TYPE` VALUES ('159', 'MetaInject', 'ETL Metadata Injection', 'Does stuff');
INSERT INTO `R_STEP_TYPE` VALUES ('160', 'Mapping', '映射 (子转换)', '运行一个映射 (子转换), 使用MappingInput和MappingOutput来指定接口的字段');
INSERT INTO `R_STEP_TYPE` VALUES ('161', 'DynamicSQLRow', '执行Dynamic SQL', 'Execute dynamic SQL statement build in a previous field');
INSERT INTO `R_STEP_TYPE` VALUES ('162', 'Update', '更新', '基于关键字更新记录到数据库');
INSERT INTO `R_STEP_TYPE` VALUES ('163', 'UniqueRowsByHashSet', '唯一行 (哈希值)', 'Remove double rows and leave only unique occurrences by using a HashSet.');
INSERT INTO `R_STEP_TYPE` VALUES ('164', 'Formula', '公式', '使用 Pentaho 的公式库来计算公式');
INSERT INTO `R_STEP_TYPE` VALUES ('165', 'WebServiceAvailable', '检查web服务是否可用', 'Check if a webservice is available');
INSERT INTO `R_STEP_TYPE` VALUES ('166', 'ExecProcess', '启动一个进程', 'Execute a process and return the result');
INSERT INTO `R_STEP_TYPE` VALUES ('167', 'Injector', '记录注射', 'Injector step to allow to inject rows into the transformation through the java API');
INSERT INTO `R_STEP_TYPE` VALUES ('168', 'SampleRows', '样本行', 'Filter rows based on the line number.');
INSERT INTO `R_STEP_TYPE` VALUES ('169', 'DummyStep', 'Example Step', 'This is a plugin example step');
INSERT INTO `R_STEP_TYPE` VALUES ('170', 'GetSlaveSequence', 'Get ID from slave server', 'Retrieves unique IDs in blocks from a slave server.  The referenced sequence needs to be configured on the slave server in the XML configuration file.');
INSERT INTO `R_STEP_TYPE` VALUES ('171', 'MappingOutput', '映射输出规范', '指定一个映射的字段输出');
INSERT INTO `R_STEP_TYPE` VALUES ('172', 'FileExists', '检查文件是否存在', 'Check if a file exists');
INSERT INTO `R_STEP_TYPE` VALUES ('173', 'Script', 'Script', 'Calculate values by scripting in Ruby, Python, Groovy, JavaScript, ... (JSR-223)');
INSERT INTO `R_STEP_TYPE` VALUES ('174', 'GroupBy', '分组', '以分组的形式创建聚合.{0}这个仅仅在一个已经排好序的输入有效.{1}如果输入没有排序, 仅仅两个连续的记录行被正确处理.');
INSERT INTO `R_STEP_TYPE` VALUES ('175', 'GetRepositoryNames', '获取资源库配置', 'Lists detailed information about transformations and/or jobs in a repository');
INSERT INTO `R_STEP_TYPE` VALUES ('176', 'PaloCellOutput', 'Palo Cell Output', 'Writes data to a defined Palo Cube');
INSERT INTO `R_STEP_TYPE` VALUES ('177', 'ClosureGenerator', 'Closure Generator', 'This step allows you to generates a closure table using parent-child relationships.');
INSERT INTO `R_STEP_TYPE` VALUES ('178', 'Sequence', '增加序列', '从序列获取下一个值');
INSERT INTO `R_STEP_TYPE` VALUES ('179', 'FilterRows', '过滤记录', '使用简单的相等来过滤记录');
INSERT INTO `R_STEP_TYPE` VALUES ('180', 'VectorWiseBulkLoader', 'Ingres VectorWise 批量加载', 'This step interfaces with the Ingres VectorWise Bulk Loader \"COPY TABLE\" command.');
INSERT INTO `R_STEP_TYPE` VALUES ('181', 'ConcatFields', 'Concat Fields', 'Concat fields together into a new field (similar to the Text File Output step)');
INSERT INTO `R_STEP_TYPE` VALUES ('182', 'TypeExitExcelWriterStep', 'Microsoft Excel 输出', 'Writes or appends data to an Excel file');
INSERT INTO `R_STEP_TYPE` VALUES ('183', 'OldTextFileInput', 'Old Text file input', '从一个文本文件（几种格式）里读取数据{0}这些数据可以被传递到下一个步骤里...');
INSERT INTO `R_STEP_TYPE` VALUES ('184', 'AnalyticQuery', '分析查询', 'Execute analytic queries over a sorted dataset (LEAD/LAG/FIRST/LAST)');
INSERT INTO `R_STEP_TYPE` VALUES ('185', 'FixedInput', '固定宽度文件输入', 'Fixed file input');
INSERT INTO `R_STEP_TYPE` VALUES ('186', 'PrioritizeStreams', '数据流优先级排序', 'Prioritize streams in an order way.');
INSERT INTO `R_STEP_TYPE` VALUES ('187', 'CassandraOutput', 'Cassandra Output', 'Writes to a Cassandra table');
INSERT INTO `R_STEP_TYPE` VALUES ('188', 'HadoopFileInputPlugin', 'Hadoop File Input', 'Process files from an HDFS location');
INSERT INTO `R_STEP_TYPE` VALUES ('189', 'ValueMapper', '值映射', 'Maps values of a certain field from one value to another');
INSERT INTO `R_STEP_TYPE` VALUES ('190', 'Constant', '增加常量', '给记录增加一到多个常量');
INSERT INTO `R_STEP_TYPE` VALUES ('191', 'ExecSQLRow', '执行SQL脚本(字段流替换)', 'Execute SQL script extracted from a field\ncreated in a previous step.');
INSERT INTO `R_STEP_TYPE` VALUES ('192', 'JavaFilter', '根据Java代码过滤记录', 'Filter rows using java code');
INSERT INTO `R_STEP_TYPE` VALUES ('193', 'XMLOutput', 'XML Output', 'Write data to an XML file');
INSERT INTO `R_STEP_TYPE` VALUES ('194', 'ReservoirSampling', '数据采样', '[Transform] Samples a fixed number of rows from the incoming stream');
INSERT INTO `R_STEP_TYPE` VALUES ('195', 'SalesforceUpsert', 'Salesforce Upsert', 'Insert or update records in Salesforce module.');
INSERT INTO `R_STEP_TYPE` VALUES ('196', 'RowsToResult', '复制记录到结果', '使用这个步骤把记录写到正在执行的任务.{0}信息将会被传递给同一个任务里的下一个条目.');
INSERT INTO `R_STEP_TYPE` VALUES ('197', 'MonetDBAgileMart', 'MonetDB Agile Mart', 'Load data into MonetDB for Agile BI use cases');
INSERT INTO `R_STEP_TYPE` VALUES ('198', 'ExecSQL', '执行SQL脚本', '执行一个SQL脚本, 另外，可以使用输入的记录作为参数');
INSERT INTO `R_STEP_TYPE` VALUES ('199', 'FieldMetadataAnnotation', 'Annotate Stream', 'Add more details to describe data for published models used by the Streamlined Data Refinery.');
INSERT INTO `R_STEP_TYPE` VALUES ('200', 'Rest', 'REST Client', 'Consume RESTfull services.\nREpresentational State Transfer (REST) is a key design idiom that embraces a stateless client-server\narchitecture in which the web services are viewed as resources and can be identified by their URLs');
INSERT INTO `R_STEP_TYPE` VALUES ('201', 'LucidDBStreamingLoader', 'LucidDB Streaming Loader', 'Load data into LucidDB by using Remote Rows UDX.');
INSERT INTO `R_STEP_TYPE` VALUES ('202', 'TransExecutor', 'Transformation Executor', 'This step executes a Pentaho Data Integration transformation, sets parameters and passes rows.');
INSERT INTO `R_STEP_TYPE` VALUES ('203', 'StepMetastructure', '流的元数据', 'This is a step to read the metadata of the incoming stream.');
INSERT INTO `R_STEP_TYPE` VALUES ('204', 'Calculator', '计算器', '通过执行简单的计算创建一个新字段');
INSERT INTO `R_STEP_TYPE` VALUES ('205', 'SplitFieldToRows3', '列拆分为多行', 'Splits a single string field by delimiter and creates a new row for each split term');
INSERT INTO `R_STEP_TYPE` VALUES ('206', 'YamlInput', 'Yaml 输入', 'Read YAML source (file or stream) parse them and convert them to rows and writes these to one or more output. ');
INSERT INTO `R_STEP_TYPE` VALUES ('207', 'FilesFromResult', '从结果获取文件', 'This step allows you to read filenames used or generated in a previous entry in a job.');
INSERT INTO `R_STEP_TYPE` VALUES ('208', 'UnivariateStats', '单变量统计', 'This step computes some simple stats based on a single input field');
INSERT INTO `R_STEP_TYPE` VALUES ('209', 'SSH', '运行SSH命令', 'Run SSH commands and returns result.');
INSERT INTO `R_STEP_TYPE` VALUES ('210', 'XSLT', 'XSL Transformation', 'Make an XSL Transformation');
INSERT INTO `R_STEP_TYPE` VALUES ('211', 'MailInput', '邮件信息输入', 'Read POP3/IMAP server and retrieve messages');
INSERT INTO `R_STEP_TYPE` VALUES ('212', 'CloneRow', '克隆行', 'Clone a row as many times as needed');
INSERT INTO `R_STEP_TYPE` VALUES ('213', 'HTTPPOST', 'HTTP Post', 'Call a web service request over HTTP by supplying a base URL by allowing parameters to be set dynamically');
INSERT INTO `R_STEP_TYPE` VALUES ('214', 'TableAgileMart', 'Table Agile Mart', 'Load data into a table for Agile BI use cases');
INSERT INTO `R_STEP_TYPE` VALUES ('215', 'HadoopFileOutputPlugin', 'Hadoop File Output', 'Create files in an HDFS location ');
INSERT INTO `R_STEP_TYPE` VALUES ('216', 'GetSubFolders', '获取子目录名', 'Read a parent folder and return all subfolders');
INSERT INTO `R_STEP_TYPE` VALUES ('217', 'NumberRange', '数值范围', 'Create ranges based on numeric field');
INSERT INTO `R_STEP_TYPE` VALUES ('218', 'CreditCardValidator', '检验信用卡号码是否有效', 'The Credit card validator step will help you tell:\n(1) if a credit card number is valid (uses LUHN10 (MOD-10) algorithm)\n(2) which credit card vendor handles that number\n(VISA, MasterCard, Diners Club, EnRoute, American Express (AMEX),...)');
INSERT INTO `R_STEP_TYPE` VALUES ('219', 'MonetDBBulkLoader', 'MonetDB 批量加载', 'Load data into MonetDB by using their bulk load command in streaming mode.');
INSERT INTO `R_STEP_TYPE` VALUES ('220', 'Mail', '发送邮件', 'Send eMail.');
INSERT INTO `R_STEP_TYPE` VALUES ('221', 'SecretKeyGenerator', '生成密钥', 'Generate secret key for algorithms such as DES, AES, TripleDES.');
INSERT INTO `R_STEP_TYPE` VALUES ('222', 'PentahoReportingOutput', 'Pentaho 报表输出', 'Executes an existing report (PRPT)');

-- ----------------------------
-- Table structure for R_TRANS_ATTRIBUTE
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_ATTRIBUTE`;
CREATE TABLE `R_TRANS_ATTRIBUTE` (
  `ID_TRANS_ATTRIBUTE` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `NR` int(11) DEFAULT NULL,
  `CODE` varchar(255) DEFAULT NULL,
  `VALUE_NUM` bigint(20) DEFAULT NULL,
  `VALUE_STR` mediumtext,
  PRIMARY KEY (`ID_TRANS_ATTRIBUTE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_ATTRIBUTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_CLUSTER
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_CLUSTER`;
CREATE TABLE `R_TRANS_CLUSTER` (
  `ID_TRANS_CLUSTER` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_CLUSTER` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID_TRANS_CLUSTER`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_CLUSTER
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_HOP
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_HOP`;
CREATE TABLE `R_TRANS_HOP` (
  `ID_TRANS_HOP` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_STEP_FROM` int(11) DEFAULT NULL,
  `ID_STEP_TO` int(11) DEFAULT NULL,
  `ENABLED` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_TRANS_HOP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_HOP
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_LOCK
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_LOCK`;
CREATE TABLE `R_TRANS_LOCK` (
  `ID_TRANS_LOCK` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_USER` int(11) DEFAULT NULL,
  `LOCK_MESSAGE` mediumtext,
  `LOCK_DATE` datetime DEFAULT NULL,
  PRIMARY KEY (`ID_TRANS_LOCK`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_LOCK
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_NOTE
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_NOTE`;
CREATE TABLE `R_TRANS_NOTE` (
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_NOTE` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_NOTE
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_PARTITION_SCHEMA
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_PARTITION_SCHEMA`;
CREATE TABLE `R_TRANS_PARTITION_SCHEMA` (
  `ID_TRANS_PARTITION_SCHEMA` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_PARTITION_SCHEMA` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID_TRANS_PARTITION_SCHEMA`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_PARTITION_SCHEMA
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_RECORD
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_RECORD`;

-- ----------------------------
-- Records of R_TRANS_RECORD
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_SLAVE
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_SLAVE`;
CREATE TABLE `R_TRANS_SLAVE` (
  `ID_TRANS_SLAVE` bigint(20) NOT NULL,
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_SLAVE` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID_TRANS_SLAVE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_SLAVE
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANS_STEP_CONDITION
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANS_STEP_CONDITION`;
CREATE TABLE `R_TRANS_STEP_CONDITION` (
  `ID_TRANSFORMATION` int(11) DEFAULT NULL,
  `ID_STEP` int(11) DEFAULT NULL,
  `ID_CONDITION` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANS_STEP_CONDITION
-- ----------------------------

-- ----------------------------
-- Table structure for R_TRANSFORMATION
-- ----------------------------
DROP TABLE IF EXISTS `R_TRANSFORMATION`;
CREATE TABLE `R_TRANSFORMATION` (
  `ID_TRANSFORMATION` bigint(20) NOT NULL,
  `ID_DIRECTORY` int(11) DEFAULT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `DESCRIPTION` mediumtext,
  `EXTENDED_DESCRIPTION` mediumtext,
  `TRANS_VERSION` varchar(255) DEFAULT NULL,
  `TRANS_STATUS` int(11) DEFAULT NULL,
  `ID_STEP_READ` int(11) DEFAULT NULL,
  `ID_STEP_WRITE` int(11) DEFAULT NULL,
  `ID_STEP_INPUT` int(11) DEFAULT NULL,
  `ID_STEP_OUTPUT` int(11) DEFAULT NULL,
  `ID_STEP_UPDATE` int(11) DEFAULT NULL,
  `ID_DATABASE_LOG` int(11) DEFAULT NULL,
  `TABLE_NAME_LOG` varchar(255) DEFAULT NULL,
  `USE_BATCHID` tinyint(1) DEFAULT NULL,
  `USE_LOGFIELD` tinyint(1) DEFAULT NULL,
  `ID_DATABASE_MAXDATE` int(11) DEFAULT NULL,
  `TABLE_NAME_MAXDATE` varchar(255) DEFAULT NULL,
  `FIELD_NAME_MAXDATE` varchar(255) DEFAULT NULL,
  `OFFSET_MAXDATE` double DEFAULT NULL,
  `DIFF_MAXDATE` double DEFAULT NULL,
  `CREATED_USER` varchar(255) DEFAULT NULL,
  `CREATED_DATE` datetime DEFAULT NULL,
  `MODIFIED_USER` varchar(255) DEFAULT NULL,
  `MODIFIED_DATE` datetime DEFAULT NULL,
  `SIZE_ROWSET` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID_TRANSFORMATION`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_TRANSFORMATION
-- ----------------------------

-- ----------------------------
-- Table structure for R_USER
-- ----------------------------
DROP TABLE IF EXISTS `R_USER`;
CREATE TABLE `R_USER` (
  `ID_USER` bigint(20) NOT NULL,
  `LOGIN` varchar(255) DEFAULT NULL,
  `PASSWORD` varchar(255) DEFAULT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `DESCRIPTION` varchar(255) DEFAULT NULL,
  `ENABLED` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_USER`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_USER
-- ----------------------------
INSERT INTO `R_USER` VALUES ('1', 'admin', '2be98afc86aa7f2e4cb79ce71da9fa6d4', 'Administrator', 'User manager', '1');
INSERT INTO `R_USER` VALUES ('2', 'guest', '2be98afc86aa7f2e4cb79ce77cb97bcce', 'Guest account', 'Read-only guest account', '1');

-- ----------------------------
-- Table structure for R_VALUE
-- ----------------------------
DROP TABLE IF EXISTS `R_VALUE`;
CREATE TABLE `R_VALUE` (
  `ID_VALUE` bigint(20) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `VALUE_TYPE` varchar(255) DEFAULT NULL,
  `VALUE_STR` varchar(255) DEFAULT NULL,
  `IS_NULL` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_VALUE`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_VALUE
-- ----------------------------

-- ----------------------------
-- Table structure for R_VERSION
-- ----------------------------
DROP TABLE IF EXISTS `R_VERSION`;
CREATE TABLE `R_VERSION` (
  `ID_VERSION` bigint(20) NOT NULL,
  `MAJOR_VERSION` int(11) DEFAULT NULL,
  `MINOR_VERSION` int(11) DEFAULT NULL,
  `UPGRADE_DATE` datetime DEFAULT NULL,
  `IS_UPGRADE` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`ID_VERSION`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of R_VERSION
-- ----------------------------
INSERT INTO `R_VERSION` VALUES ('1', '5', '0', '2017-10-17 10:51:20', '0');

-- ----------------------------
-- Table structure for R_RECORD_HISTORY
-- ----------------------------
DROP TABLE IF EXISTS `R_RECORD_HISTORY`;
CREATE TABLE `R_RECORD_HISTORY` (
`ID`  bigint(20) NOT NULL ,
`NAME`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`RECORD_TYPE`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`ID_RUN`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`STATUS`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`HOSTNAME`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`ERROR_MSG`  varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`CREATE_TIME`  timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE CURRENT_TIMESTAMP 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for R_RECORD_JOB
-- ----------------------------
DROP TABLE IF EXISTS `R_RECORD_JOB`;
CREATE TABLE `R_RECORD_JOB` (
`ID_JOB`  bigint(20) NOT NULL ,
`NAME_JOB`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`ID_RUN`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`STATUS`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`HOSTNAME`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`ERROR_MSG`  varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`CREATE_TIME`  timestamp NOT NULL DEFAULT current_timestamp() ,
`UPDATE_TIME`  timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`ID_JOB`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for R_RECORD_TRANS
-- ----------------------------
DROP TABLE IF EXISTS `R_RECORD_TRANS`;
CREATE TABLE `R_RECORD_TRANS` (
`ID_TRANSFORMATION`  bigint(20) NOT NULL ,
`NAME_TRANSFORMATION`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL ,
`ID_RUN`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`STATUS`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`HOSTNAME`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`ERROR_MSG`  varchar(512) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
`CREATE_TIME`  timestamp NOT NULL DEFAULT current_timestamp() ,
`UPDATE_TIME`  timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE CURRENT_TIMESTAMP ,
PRIMARY KEY (`ID_TRANSFORMATION`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for R_RECORD_STRATEGY
-- ----------------------------
DROP TABLE IF EXISTS `R_RECORD_SCHEDULER`;
CREATE TABLE `R_RECORD_STRATEGY` (
`ID`  bigint(20) NOT NULL ,
`NAME`  varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`RECORD_TYPE`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`SCHEDULER_TYPE`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' ,
`CRON_EXPRESSION`  varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' ,
UNIQUE INDEX `UNIQUE` (`ID`, `RECORD_TYPE`) USING BTREE 
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8 COLLATE=utf8_general_ci
ROW_FORMAT=DYNAMIC
;