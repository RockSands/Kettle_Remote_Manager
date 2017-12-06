package com.kettle.core;

/**
 * Kettle环境的默认值
 * 
 * @author Administrator
 *
 */
public class KettleEnvDefault {
	/**
	 * 任务数据库是否使用连接池
	 */
	public static final String KETTLE_RECORD_DB_POOL = "Y";

	/**
	 * 任务数据库连接池初始连接数
	 */
	public static final int KETTLE_RECORD_DB_POOL_INIT = 5;

	/**
	 * 任务数据库连接池最大连接数
	 */
	public static final int KETTLE_RECORD_DB_POOL_MAX = 10;
	/**
	 * 远端并行任务数量
	 */
	public static final int KETTLE_RECORD_MAX_PER_REMOTE = 6;

	/**
	 * 任务池最大数量,null或小于1标识不判断
	 */
	public static final Integer KETTLE_RECORD_POOL_MAX = null;

	/**
	 * 任务完成后保存的最长时间.
	 */
	public static final Integer KETTLE_RECORD_PERSIST_MAX_HOUR = null;

	/**
	 * 任务远端运行的超时时间
	 */
	public static final Integer KETTLE_RECORD_RUNNING_TIMEOUT = null;

}
