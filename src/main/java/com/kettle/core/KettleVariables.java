package com.kettle.core;

public class KettleVariables {
	/**
	 * 转换记录的表名
	 */
	public static final String R_TRANS_RECORD = "R_TRANS_RECORD";

	/**
	 * 工作记录的表名
	 */
	public static final String R_JOB_RECORD = "R_JOB_RECORD";

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
	 * 工作记录的类型:重复
	 */
	public static final String JOB_RECORD_TYPE_REPEAT = "REPEAT";

	/**
	 * 工作记录的类型:执行一次
	 */
	public static final String JOB_RECORD_TYPE_ONCE = "ONCE";

	/**
	 * 远端的运行状态:异常
	 */
	public static final String REMOTE_STATUS_ERROR = "ERROR";

	/**
	 * 远端的运行状态:正常
	 */
	public static final String REMOTE_STATUS_RUNNING = "RUNNING";
}
