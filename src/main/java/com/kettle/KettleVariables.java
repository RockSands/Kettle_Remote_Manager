package com.kettle;

public class KettleVariables {
	/**
	 * 记录的表名
	 */
	public static final String R_TRANS_RECORD = "R_TRANS_RECORD";

	/**
	 * 表的转换元数据ID:唯一
	 */
	public static final String R_RECORD_ID_TRANS = "ID_TRANSFORMATION";

	/**
	 * 表的转换元数据名称
	 */
	public static final String R_RECORD_NAME_TRANS = "NAME_TRANSFORMATION";

	/**
	 * 表的运行ID:唯一
	 */
	public static final String R_RECORD_ID_RUN = "ID_RUN";

	/**
	 * 表的主机名
	 */
	public static final String R_RECORD_HOSTNAME = "HOSTNAME";

	/**
	 * 表的主机名
	 */
	public static final String R_RECORD_STATUS = "STATUS";

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
}
