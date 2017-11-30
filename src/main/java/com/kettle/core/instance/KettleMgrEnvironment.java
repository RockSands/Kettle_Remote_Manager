package com.kettle.core.instance;

import org.pentaho.di.core.util.EnvUtil;

import com.kettle.core.KettleEnvDefault;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.pool.KettleRecordPool;
import com.kettle.remote.KettleRemotePool;

/**
 * Kettle管理环境
 * 
 * @author Administrator
 *
 */
public class KettleMgrEnvironment {

	/**
	 * 远端池
	 */
	private KettleRemotePool remotePool;

	/**
	 * 任务池
	 */
	private KettleRecordPool recordPool;

	/**
	 * Kettle资源库
	 */
	private KettleRepositoryClient repositoryClient;

	/**
	 * 数据库
	 */
	private KettleDBClient dbClient;

	/**
	 * Record任务最大保持数量
	 */
	public static Integer KETTLE_RECORD_POOL_MAX = NVLInt("KETTLE_RECORD_POOL_MAX",
			KettleEnvDefault.KETTLE_RECORD_POOL_MAX);

	/**
	 * Record任务保留最长时间
	 */
	public static Integer KETTLE_RECORD_PERSIST_MAX_HOUR = NVLInt("KETTLE_RECORD_PERSIST_MAX_HOUR",
			KettleEnvDefault.KETTLE_RECORD_PERSIST_MAX_HOUR);

	/**
	 * Record任务保留最长时间
	 */
	public static int KETTLE_RECORD_MAX_PER_REMOTE = NVLInt("KETTLE_RECORD_MAX_PER_REMOTE",
			KettleEnvDefault.KETTLE_RECORD_MAX_PER_REMOTE);

	/**
	 * Record任务保留最长时间
	 */
	public static Integer KETTLE_RECORD_RUNNING_TIMEOUT = NVLInt("KETTLE_RECORD_RUNNING_TIMEOUT",
			KettleEnvDefault.KETTLE_RECORD_RUNNING_TIMEOUT);

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	public static String NVLStr(String key, String defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : EnvUtil.getSystemProperty(key);
	}

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	public static Integer NVLInt(String key, Integer defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : Integer.valueOf(EnvUtil.getSystemProperty(key));
	}

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	public static Long NVLLong(String key, Long defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : Long.valueOf(EnvUtil.getSystemProperty(key));
	}

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	public static Double NVLDouble(String key, Double defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : Double.valueOf(EnvUtil.getSystemProperty(key));
	}

	/**
	 * @return
	 */
	public KettleRemotePool getRemotePool() {
		return remotePool;
	}

	/**
	 * @return
	 */
	public KettleRecordPool getRecordPool() {
		return recordPool;
	}

	/**
	 * @return
	 */
	public KettleRepositoryClient getRepositoryClient() {
		return repositoryClient;
	}

	/**
	 * @return
	 */
	public KettleDBClient getDbClient() {
		return dbClient;
	}

	void setRemotePool(KettleRemotePool remotePool) {
		this.remotePool = remotePool;
	}

	void setRecordPool(KettleRecordPool recordPool) {
		this.recordPool = recordPool;
	}

	void setRepositoryClient(KettleRepositoryClient repositoryClient) {
		this.repositoryClient = repositoryClient;
	}

	void setDbClient(KettleDBClient dbClient) {
		this.dbClient = dbClient;
	}
}
