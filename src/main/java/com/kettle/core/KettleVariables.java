package com.kettle.core;

public class KettleVariables {
	/**
	 * 记录的表名
	 */
	public static final String R_TRANS_RECORD = "R_TRANS_RECORD";

	/**
	 * 集群记录的表名
	 */
	public static final String R_TRANS_RECORD_SPLIT = "R_TRANS_RECORD_SPLIT";

	/**
	 * 记录的转换元数据ID:唯一
	 */
	public static final String R_RECORD_ID_TRANS = "ID_TRANSFORMATION";

	/**
	 * 记录的转换元数据名称
	 */
	public static final String R_RECORD_NAME_TRANS = "NAME_TRANSFORMATION";

	/**
	 * 记录的运行ID:唯一
	 */
	public static final String R_RECORD_ID_RUN = "ID_RUN";

	/**
	 * 记录的主机名
	 */
	public static final String R_RECORD_HOSTNAME = "HOSTNAME";

	/**
	 * 记录的集群名称
	 */
	public static final String R_RECORD_CLUSTERNAME = "CLUSTERNAME";

	/**
	 * 表的主机名
	 */
	public static final String R_RECORD_STATUS = "STATUS";

	/**
	 * ID
	 */
	public static final String R_TRANS_RECORD_SPLIT_ID = "ID_SPLIT";

	/**
	 * 记录的运行状态:运行中
	 */
	public static final String RECORD_STATUS_RUNNING = "RUNNING";

	/**
	 * 记录的运行状态:异常
	 */
	public static final String RECORD_STATUS_ERROR = "ERROR";

	/**
	 * 记录的运行状态:异常
	 */
	public static final String RECORD_STATUS_FINISHED = "FINISHED";

	/**
	 * 记录的运行状态:其他
	 */
	public static final String RECORD_STATUS_OTRHER = "OTRHER";

	/**
	 * 远端的运行状态:异常
	 */
	public static final String REMOTE_STATUS_ERROR = "ERROR";

	/**
	 * 远端的运行状态:正常
	 */
	public static final String REMOTE_STATUS_RUNNING = "RUNNING";
}
