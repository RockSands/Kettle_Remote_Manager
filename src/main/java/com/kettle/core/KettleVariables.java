package com.kettle.core;

public class KettleVariables {
	/**
	 * 转换记录的表名
	 */
	public static final String R_TRANS_RECORD = "R_RECORD_TRANS";

	/**
	 * 工作记录的表名
	 */
	public static final String R_JOB_RECORD = "R_RECORD_JOB";

	/**
	 * 历史记录的表名
	 */
	public static final String R_HISTORY_RECORD = "R_RECORD_HISTORY";

	/**
	 * 历史记录的ID
	 */
	public static final String R_HISTORY_RECORD_ID = "ID";

	/**
	 * 历史记录的NAME
	 */
	public static final String R_HISTORY_RECORD_NAME = "NAME";

	/**
	 * 记录的RECORD_TYPE
	 */
	public static final String R_RECORD_RECORD_TYPE = "RECORD_TYPE";

	/**
	 * 记录的UUID
	 */
	public static final String R_RECORD_UUID = "UUID";

	/**
	 * 记录的CRON表达式
	 */
	public static final String R_RECORD_CRON_EXPRESSION = "CRON_EXPRESSION";

	/**
	 * 历史记录的TYPE:TRANS
	 */
	public static final String R_HISTORY_RECORD_TYPE_TRANS = "TRANS";

	/**
	 * 历史记录的TYPE:JOB
	 */
	public static final String R_HISTORY_RECORD_TYPE_JOB = "JOB";

	/**
	 * 转换记录的元数据ID:唯一
	 */
	public static final String R_TRANS_RECORD_ID_TRANS = "ID_TRANSFORMATION";

	/**
	 * 工作记录的元数据ID:唯一
	 */
	public static final String R_JOB_RECORD_ID_JOB = "ID_JOB";

	/**
	 * 转换记录的元数据名称
	 */
	public static final String R_TRANS_RECORD_NAME_TRANS = "NAME_TRANSFORMATION";

	/**
	 * 工作记录的元数据名称
	 */
	public static final String R_JOB_RECORD_NAME_JOB = "NAME_JOB";

	/**
	 * 工作或转换记录的运行ID:唯一
	 */
	public static final String R_RECORD_ID_RUN = "ID_RUN";

	/**
	 * 工作或转换记录的主机名
	 */
	public static final String R_RECORD_HOSTNAME = "HOSTNAME";

	/**
	 * 工作或转换记录的创建时间
	 */
	public static final String R_RECORD_CREATETIME = "CREATE_TIME";

	/**
	 * 工作或转换记录的更新时间
	 */
	public static final String R_RECORD_UPDATETIME = "UPDATE_TIME";

	/**
	 * 工作或转换记录的异常信息
	 */
	public static final String R_RECORD_ERRORMSG = "ERROR_MSG";

	/**
	 * 工作或转换记录的状态
	 */
	public static final String R_RECORD_STATUS = "STATUS";

	/**
	 * 工作记录的类型
	 */
	public static final String R_JOB_RECORD_TYPE = "TYPE";

	/**
	 * 记录的运行状态:运行中
	 */
	public static final String RECORD_STATUS_RUNNING = "RUNNING";

	/**
	 * 记录的运行状态:受理
	 */
	public static final String RECORD_STATUS_APPLY = "APPLY";

	/**
	 * 记录的运行状态:异常
	 */
	public static final String RECORD_STATUS_ERROR = "ERROR";

	/**
	 * 记录的运行状态:异常
	 */
	public static final String RECORD_STATUS_FINISHED = "FINISHED";

	/**
	 * 远端的运行状态:异常
	 */
	public static final String REMOTE_STATUS_ERROR = "ERROR";

	/**
	 * 远端的运行状态:正常
	 */
	public static final String REMOTE_STATUS_RUNNING = "Online";
}
