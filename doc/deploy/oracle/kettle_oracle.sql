-------------------------------------------
-- Export file for user KETTLE           --
-- Created by css on 2017/11/9, 15:27:49 --
-------------------------------------------

set define off
spool kettle_oracle.log

prompt
prompt Creating table R_CLUSTER
prompt ========================
prompt
create table KETTLE.R_CLUSTER
(
  id_cluster             INTEGER not null,
  name                   VARCHAR2(255),
  base_port              VARCHAR2(255),
  sockets_buffer_size    VARCHAR2(255),
  sockets_flush_interval VARCHAR2(255),
  sockets_compressed     CHAR(1),
  dynamic_cluster        CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_CLUSTER
  add primary key (ID_CLUSTER)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_CLUSTER_SLAVE
prompt ==============================
prompt
create table KETTLE.R_CLUSTER_SLAVE
(
  id_cluster_slave INTEGER not null,
  id_cluster       INTEGER,
  id_slave         INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_CLUSTER_SLAVE
  add primary key (ID_CLUSTER_SLAVE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_CONDITION
prompt ==========================
prompt
create table KETTLE.R_CONDITION
(
  id_condition        INTEGER not null,
  id_condition_parent INTEGER,
  negated             CHAR(1),
  operator            VARCHAR2(255),
  left_name           VARCHAR2(255),
  condition_function  VARCHAR2(255),
  right_name          VARCHAR2(255),
  id_value_right      INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_CONDITION
  add primary key (ID_CONDITION)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_DATABASE
prompt =========================
prompt
create table KETTLE.R_DATABASE
(
  id_database         INTEGER not null,
  name                VARCHAR2(255),
  id_database_type    INTEGER,
  id_database_contype INTEGER,
  host_name           VARCHAR2(255),
  database_name       CLOB,
  port                INTEGER,
  username            VARCHAR2(255),
  password            VARCHAR2(255),
  servername          VARCHAR2(255),
  data_tbs            VARCHAR2(255),
  index_tbs           VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_DATABASE
  add primary key (ID_DATABASE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_DATABASE_ATTRIBUTE
prompt ===================================
prompt
create table KETTLE.R_DATABASE_ATTRIBUTE
(
  id_database_attribute INTEGER not null,
  id_database           INTEGER,
  code                  VARCHAR2(255),
  value_str             CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create unique index KETTLE.IDX_RDAT on KETTLE.R_DATABASE_ATTRIBUTE (ID_DATABASE, CODE)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
alter table KETTLE.R_DATABASE_ATTRIBUTE
  add primary key (ID_DATABASE_ATTRIBUTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_DATABASE_CONTYPE
prompt =================================
prompt
create table KETTLE.R_DATABASE_CONTYPE
(
  id_database_contype INTEGER not null,
  code                VARCHAR2(255),
  description         VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_DATABASE_CONTYPE
  add primary key (ID_DATABASE_CONTYPE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_DATABASE_TYPE
prompt ==============================
prompt
create table KETTLE.R_DATABASE_TYPE
(
  id_database_type INTEGER not null,
  code             VARCHAR2(255),
  description      VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_DATABASE_TYPE
  add primary key (ID_DATABASE_TYPE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_DEPENDENCY
prompt ===========================
prompt
create table KETTLE.R_DEPENDENCY
(
  id_dependency     INTEGER not null,
  id_transformation INTEGER,
  id_database       INTEGER,
  table_name        VARCHAR2(255),
  field_name        VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_DEPENDENCY
  add primary key (ID_DEPENDENCY)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_DIRECTORY
prompt ==========================
prompt
create table KETTLE.R_DIRECTORY
(
  id_directory        INTEGER not null,
  id_directory_parent INTEGER,
  directory_name      VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create unique index KETTLE.IDX_RDIR on KETTLE.R_DIRECTORY (ID_DIRECTORY_PARENT, DIRECTORY_NAME)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
alter table KETTLE.R_DIRECTORY
  add primary key (ID_DIRECTORY)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_ELEMENT
prompt ========================
prompt
create table KETTLE.R_ELEMENT
(
  id_element      INTEGER not null,
  id_element_type INTEGER,
  name            VARCHAR2(1999)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_ELEMENT
  add primary key (ID_ELEMENT)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_ELEMENT_ATTRIBUTE
prompt ==================================
prompt
create table KETTLE.R_ELEMENT_ATTRIBUTE
(
  id_element_attribute        INTEGER not null,
  id_element                  INTEGER,
  id_element_attribute_parent INTEGER,
  attr_key                    VARCHAR2(255),
  attr_value                  CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_ELEMENT_ATTRIBUTE
  add primary key (ID_ELEMENT_ATTRIBUTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_ELEMENT_TYPE
prompt =============================
prompt
create table KETTLE.R_ELEMENT_TYPE
(
  id_element_type INTEGER not null,
  id_namespace    INTEGER,
  name            VARCHAR2(1999),
  description     CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_ELEMENT_TYPE
  add primary key (ID_ELEMENT_TYPE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOB
prompt ====================
prompt
create table KETTLE.R_JOB
(
  id_job               INTEGER not null,
  id_directory         INTEGER,
  name                 VARCHAR2(255),
  description          CLOB,
  extended_description CLOB,
  job_version          VARCHAR2(255),
  job_status           INTEGER,
  id_database_log      INTEGER,
  table_name_log       VARCHAR2(255),
  created_user         VARCHAR2(255),
  created_date         DATE,
  modified_user        VARCHAR2(255),
  modified_date        DATE,
  use_batch_id         CHAR(1),
  pass_batch_id        CHAR(1),
  use_logfield         CHAR(1),
  shared_file          VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_JOB
  add primary key (ID_JOB)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOBENTRY
prompt =========================
prompt
create table KETTLE.R_JOBENTRY
(
  id_jobentry      INTEGER not null,
  id_job           INTEGER,
  id_jobentry_type INTEGER,
  name             VARCHAR2(255),
  description      CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_JOBENTRY
  add primary key (ID_JOBENTRY)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOBENTRY_ATTRIBUTE
prompt ===================================
prompt
create table KETTLE.R_JOBENTRY_ATTRIBUTE
(
  id_jobentry_attribute INTEGER not null,
  id_job                INTEGER,
  id_jobentry           INTEGER,
  nr                    INTEGER,
  code                  VARCHAR2(255),
  value_num             NUMBER(13,2),
  value_str             CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create unique index KETTLE.IDX_RJEA on KETTLE.R_JOBENTRY_ATTRIBUTE (ID_JOBENTRY_ATTRIBUTE, CODE, NR)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
alter table KETTLE.R_JOBENTRY_ATTRIBUTE
  add primary key (ID_JOBENTRY_ATTRIBUTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOBENTRY_COPY
prompt ==============================
prompt
create table KETTLE.R_JOBENTRY_COPY
(
  id_jobentry_copy INTEGER not null,
  id_jobentry      INTEGER,
  id_job           INTEGER,
  id_jobentry_type INTEGER,
  nr               INTEGER,
  gui_location_x   INTEGER,
  gui_location_y   INTEGER,
  gui_draw         CHAR(1),
  parallel         CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_JOBENTRY_COPY
  add primary key (ID_JOBENTRY_COPY)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOBENTRY_DATABASE
prompt ==================================
prompt
create table KETTLE.R_JOBENTRY_DATABASE
(
  id_job      INTEGER,
  id_jobentry INTEGER,
  id_database INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create index KETTLE.IDX_RJD1 on KETTLE.R_JOBENTRY_DATABASE (ID_JOB)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
create index KETTLE.IDX_RJD2 on KETTLE.R_JOBENTRY_DATABASE (ID_DATABASE)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOBENTRY_TYPE
prompt ==============================
prompt
create table KETTLE.R_JOBENTRY_TYPE
(
  id_jobentry_type INTEGER not null,
  code             VARCHAR2(255),
  description      VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_JOBENTRY_TYPE
  add primary key (ID_JOBENTRY_TYPE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_JOB_ATTRIBUTE
prompt ==============================
prompt
create table KETTLE.R_JOB_ATTRIBUTE
(
  id_job_attribute INTEGER not null,
  id_job           INTEGER,
  nr               INTEGER,
  code             VARCHAR2(255),
  value_num        INTEGER,
  value_str        CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create unique index KETTLE.IDX_JATT on KETTLE.R_JOB_ATTRIBUTE (ID_JOB, CODE, NR)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
alter table KETTLE.R_JOB_ATTRIBUTE
  add primary key (ID_JOB_ATTRIBUTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOB_HOP
prompt ========================
prompt
create table KETTLE.R_JOB_HOP
(
  id_job_hop            INTEGER not null,
  id_job                INTEGER,
  id_jobentry_copy_from INTEGER,
  id_jobentry_copy_to   INTEGER,
  enabled               CHAR(1),
  evaluation            CHAR(1),
  unconditional         CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_JOB_HOP
  add primary key (ID_JOB_HOP)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOB_LOCK
prompt =========================
prompt
create table KETTLE.R_JOB_LOCK
(
  id_job_lock  INTEGER not null,
  id_job       INTEGER,
  id_user      INTEGER,
  lock_message CLOB,
  lock_date    DATE
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_JOB_LOCK
  add primary key (ID_JOB_LOCK)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_JOB_NOTE
prompt =========================
prompt
create table KETTLE.R_JOB_NOTE
(
  id_job  INTEGER,
  id_note INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;

prompt
prompt Creating table R_LOG
prompt ====================
prompt
create table KETTLE.R_LOG
(
  id_log          INTEGER not null,
  name            VARCHAR2(255),
  id_loglevel     INTEGER,
  logtype         VARCHAR2(255),
  filename        VARCHAR2(255),
  fileextention   VARCHAR2(255),
  add_date        CHAR(1),
  add_time        CHAR(1),
  id_database_log INTEGER,
  table_name_log  VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_LOG
  add primary key (ID_LOG)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_LOGLEVEL
prompt =========================
prompt
create table KETTLE.R_LOGLEVEL
(
  id_loglevel INTEGER not null,
  code        VARCHAR2(255),
  description VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_LOGLEVEL
  add primary key (ID_LOGLEVEL)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_NAMESPACE
prompt ==========================
prompt
create table KETTLE.R_NAMESPACE
(
  id_namespace INTEGER not null,
  name         VARCHAR2(1999)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_NAMESPACE
  add primary key (ID_NAMESPACE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_NOTE
prompt =====================
prompt
create table KETTLE.R_NOTE
(
  id_note                      INTEGER not null,
  value_str                    CLOB,
  gui_location_x               INTEGER,
  gui_location_y               INTEGER,
  gui_location_width           INTEGER,
  gui_location_height          INTEGER,
  font_name                    CLOB,
  font_size                    INTEGER,
  font_bold                    CHAR(1),
  font_italic                  CHAR(1),
  font_color_red               INTEGER,
  font_color_green             INTEGER,
  font_color_blue              INTEGER,
  font_back_ground_color_red   INTEGER,
  font_back_ground_color_green INTEGER,
  font_back_ground_color_blue  INTEGER,
  font_border_color_red        INTEGER,
  font_border_color_green      INTEGER,
  font_border_color_blue       INTEGER,
  draw_shadow                  CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_NOTE
  add primary key (ID_NOTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_PARTITION
prompt ==========================
prompt
create table KETTLE.R_PARTITION
(
  id_partition        INTEGER not null,
  id_partition_schema INTEGER,
  partition_id        VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_PARTITION
  add primary key (ID_PARTITION)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_PARTITION_SCHEMA
prompt =================================
prompt
create table KETTLE.R_PARTITION_SCHEMA
(
  id_partition_schema  INTEGER not null,
  name                 VARCHAR2(255),
  dynamic_definition   CHAR(1),
  partitions_per_slave VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_PARTITION_SCHEMA
  add primary key (ID_PARTITION_SCHEMA)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_REPOSITORY_LOG
prompt ===============================
prompt
create table KETTLE.R_REPOSITORY_LOG
(
  id_repository_log INTEGER not null,
  rep_version       VARCHAR2(255),
  log_date          DATE,
  log_user          VARCHAR2(255),
  operation_desc    CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_REPOSITORY_LOG
  add primary key (ID_REPOSITORY_LOG)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_SLAVE
prompt ======================
prompt
create table KETTLE.R_SLAVE
(
  id_slave        INTEGER not null,
  name            VARCHAR2(255),
  host_name       VARCHAR2(255),
  port            VARCHAR2(255),
  web_app_name    VARCHAR2(255),
  username        VARCHAR2(255),
  password        VARCHAR2(255),
  proxy_host_name VARCHAR2(255),
  proxy_port      VARCHAR2(255),
  non_proxy_hosts VARCHAR2(255),
  master          CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_SLAVE
  add primary key (ID_SLAVE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_STEP
prompt =====================
prompt
create table KETTLE.R_STEP
(
  id_step           INTEGER not null,
  id_transformation INTEGER,
  name              VARCHAR2(255),
  description       CLOB,
  id_step_type      INTEGER,
  distribute        CHAR(1),
  copies            INTEGER,
  gui_location_x    INTEGER,
  gui_location_y    INTEGER,
  gui_draw          CHAR(1),
  copies_string     VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_STEP
  add primary key (ID_STEP)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_STEP_ATTRIBUTE
prompt ===============================
prompt
create table KETTLE.R_STEP_ATTRIBUTE
(
  id_step_attribute INTEGER not null,
  id_transformation INTEGER,
  id_step           INTEGER,
  nr                INTEGER,
  code              VARCHAR2(255),
  value_num         INTEGER,
  value_str         CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create unique index KETTLE.IDX_RSAT on KETTLE.R_STEP_ATTRIBUTE (ID_STEP, CODE, NR)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
alter table KETTLE.R_STEP_ATTRIBUTE
  add primary key (ID_STEP_ATTRIBUTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_STEP_DATABASE
prompt ==============================
prompt
create table KETTLE.R_STEP_DATABASE
(
  id_transformation INTEGER,
  id_step           INTEGER,
  id_database       INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create index KETTLE.IDX_RSD1 on KETTLE.R_STEP_DATABASE (ID_TRANSFORMATION)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
create index KETTLE.IDX_RSD2 on KETTLE.R_STEP_DATABASE (ID_DATABASE)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_STEP_TYPE
prompt ==========================
prompt
create table KETTLE.R_STEP_TYPE
(
  id_step_type INTEGER not null,
  code         VARCHAR2(255),
  description  VARCHAR2(255),
  helptext     VARCHAR2(255)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_STEP_TYPE
  add primary key (ID_STEP_TYPE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_TRANSFORMATION
prompt ===============================
prompt
create table KETTLE.R_TRANSFORMATION
(
  id_transformation    INTEGER not null,
  id_directory         INTEGER,
  name                 VARCHAR2(255),
  description          CLOB,
  extended_description CLOB,
  trans_version        VARCHAR2(255),
  trans_status         INTEGER,
  id_step_read         INTEGER,
  id_step_write        INTEGER,
  id_step_input        INTEGER,
  id_step_output       INTEGER,
  id_step_update       INTEGER,
  id_database_log      INTEGER,
  table_name_log       VARCHAR2(255),
  use_batchid          CHAR(1),
  use_logfield         CHAR(1),
  id_database_maxdate  INTEGER,
  table_name_maxdate   VARCHAR2(255),
  field_name_maxdate   VARCHAR2(255),
  offset_maxdate       NUMBER(12,2),
  diff_maxdate         NUMBER(12,2),
  created_user         VARCHAR2(255),
  created_date         DATE,
  modified_user        VARCHAR2(255),
  modified_date        DATE,
  size_rowset          INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_TRANSFORMATION
  add primary key (ID_TRANSFORMATION)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_ATTRIBUTE
prompt ================================
prompt
create table KETTLE.R_TRANS_ATTRIBUTE
(
  id_trans_attribute INTEGER not null,
  id_transformation  INTEGER,
  nr                 INTEGER,
  code               VARCHAR2(255),
  value_num          INTEGER,
  value_str          CLOB
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
create unique index KETTLE.IDX_TATT on KETTLE.R_TRANS_ATTRIBUTE (ID_TRANSFORMATION, CODE, NR)
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;
alter table KETTLE.R_TRANS_ATTRIBUTE
  add primary key (ID_TRANS_ATTRIBUTE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_CLUSTER
prompt ==============================
prompt
create table KETTLE.R_TRANS_CLUSTER
(
  id_trans_cluster  INTEGER not null,
  id_transformation INTEGER,
  id_cluster        INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_TRANS_CLUSTER
  add primary key (ID_TRANS_CLUSTER)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_HOP
prompt ==========================
prompt
create table KETTLE.R_TRANS_HOP
(
  id_trans_hop      INTEGER not null,
  id_transformation INTEGER,
  id_step_from      INTEGER,
  id_step_to        INTEGER,
  enabled           CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_TRANS_HOP
  add primary key (ID_TRANS_HOP)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_LOCK
prompt ===========================
prompt
create table KETTLE.R_TRANS_LOCK
(
  id_trans_lock     INTEGER not null,
  id_transformation INTEGER,
  id_user           INTEGER,
  lock_message      CLOB,
  lock_date         DATE
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_TRANS_LOCK
  add primary key (ID_TRANS_LOCK)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_NOTE
prompt ===========================
prompt
create table KETTLE.R_TRANS_NOTE
(
  id_transformation INTEGER,
  id_note           INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;

prompt
prompt Creating table R_TRANS_PARTITION_SCHEMA
prompt =======================================
prompt
create table KETTLE.R_TRANS_PARTITION_SCHEMA
(
  id_trans_partition_schema INTEGER not null,
  id_transformation         INTEGER,
  id_partition_schema       INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_TRANS_PARTITION_SCHEMA
  add primary key (ID_TRANS_PARTITION_SCHEMA)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_SLAVE
prompt ============================
prompt
create table KETTLE.R_TRANS_SLAVE
(
  id_trans_slave    INTEGER not null,
  id_transformation INTEGER,
  id_slave          INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_TRANS_SLAVE
  add primary key (ID_TRANS_SLAVE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_TRANS_STEP_CONDITION
prompt =====================================
prompt
create table KETTLE.R_TRANS_STEP_CONDITION
(
  id_transformation INTEGER,
  id_step           INTEGER,
  id_condition      INTEGER
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;

prompt
prompt Creating table R_USER
prompt =====================
prompt
create table KETTLE.R_USER
(
  id_user     INTEGER not null,
  login       VARCHAR2(255),
  password    VARCHAR2(255),
  name        VARCHAR2(255),
  description VARCHAR2(255),
  enabled     CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_USER
  add primary key (ID_USER)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );

prompt
prompt Creating table R_VALUE
prompt ======================
prompt
create table KETTLE.R_VALUE
(
  id_value   INTEGER not null,
  name       VARCHAR2(255),
  value_type VARCHAR2(255),
  value_str  VARCHAR2(255),
  is_null    CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255;
alter table KETTLE.R_VALUE
  add primary key (ID_VALUE)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255;

prompt
prompt Creating table R_VERSION
prompt ========================
prompt
create table KETTLE.R_VERSION
(
  id_version    INTEGER not null,
  major_version INTEGER,
  minor_version INTEGER,
  upgrade_date  DATE,
  is_upgrade    CHAR(1)
)
tablespace USERS
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
alter table KETTLE.R_VERSION
  add primary key (ID_VERSION)
  using index 
  tablespace USERS
  pctfree 10
  initrans 2
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );



-- ----------------------------
-- Table structure for R_RECORD_HISTORY
-- ----------------------------
-- Create table
create table R_RECORD_HISTORY
(
  UUID		  VARCHAR2(64) not null,
  id          VARCHAR2(225) not null,
  name        VARCHAR2(64) not null,
  id_run      VARCHAR2(64),
  status      VARCHAR2(64) default '' not null,
  hostname    VARCHAR2(64),
  error_msg   VARCHAR2(512),
  create_time DATE not null
)

-- ----------------------------
-- Table structure for R_RECORD_JOB
-- ----------------------------

CREATE TABLE R_RECORD_JOB (
UUID  varchar2(64)  NOT NULL ,
ID_JOB  VARCHAR2(225) NOT NULL ,
NAME_JOB  varchar2(64)  NOT NULL ,
ID_RUN  varchar2(64)  NULL ,
STATUS  varchar2(64)  NOT NULL ,
HOSTNAME  varchar2(64)  NULL ,
CRON_EXPRESSION  varchar2(16)  NULL  ,
ERROR_MSG  varchar2(512)  NULL ,
CREATE_TIME  date NOT NULL,
UPDATE_TIME  date NOT NULL,
PRIMARY KEY (ID_JOB,UUID)
);

-- ----------------------------
-- Table structure for R_RECORD_HISTORY
-- ----------------------------

CREATE TABLE R_RECORD_DEPENDENT (
MASTER_UUID_ID  varchar2(64) NOT NULL ,
META_ID  VARCHAR2(225) NOT NULL ,
META_TYPE  varchar2(16)  NOT NULL ,
CREATE_TIME  date NOT NULL
);

spool off
