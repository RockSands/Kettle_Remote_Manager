package com.kettle.core.instance;

import org.pentaho.di.core.util.EnvUtil;

import com.kettle.core.KettleEnvDefault;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemotePool;

public class KettleMgrEnvironment {

	/**
	 * 远端池
	 */
	static KettleRemotePool remotePool;

	/**
	 * 任务池
	 */
	static KettleRecordPool recordPool;

	/**
	 * Kettle资源库
	 */
	static KettleRepositoryClient repositoryClient;

	/**
	 * 数据库
	 */
	static KettleDBClient dbClient;

	/**
	 * Record任务最大保持数量
	 */
	public static int KETTLE_RECORD_POOL_MAX = NVLInt("KETTLE_RECORD_POOL_MAX",
			KettleEnvDefault.KETTLE_RECORD_MAX_PER_REMOTE);

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	static String NVLStr(String key, String defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : EnvUtil.getSystemProperty(key);
	}

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	static int NVLInt(String key, Integer defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : Integer.valueOf(EnvUtil.getSystemProperty(key));
	}

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	static long NVLLong(String key, long defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : Long.valueOf(EnvUtil.getSystemProperty(key));
	}

	/**
	 * @param key
	 * @param defaultVal
	 * @return
	 */
	static double NVLDouble(String key, double defaultVal) {
		return EnvUtil.getSystemProperty(key) == null ? defaultVal : Double.valueOf(EnvUtil.getSystemProperty(key));
	}

	/**
	 * @return
	 */
	public static KettleRemotePool getRemotePool() {
		return remotePool;
	}

	/**
	 * @return
	 */
	public static KettleRecordPool getRecordPool() {
		return recordPool;
	}

	/**
	 * @return
	 */
	public static KettleRepositoryClient getRepositoryClient() {
		return repositoryClient;
	}

	/**
	 * @return
	 */
	public static KettleDBClient getDbClient() {
		return dbClient;
	}
}
