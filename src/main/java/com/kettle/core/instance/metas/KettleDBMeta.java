package com.kettle.core.instance.metas;

import com.kettle.core.instance.KettleMgrEnvironment;

/**
 * 数据库元数据
 * 
 * @author Administrator
 *
 */
public class KettleDBMeta {

	/**
	 * 数据库类别
	 */
	private String type;

	/**
	 * 数据库IP
	 */
	private String host;

	/**
	 * 端口
	 */
	private String port;

	/**
	 * DataBase
	 */
	private String database;

	/**
	 * 数据库登入用户
	 */
	private String user;

	/**
	 * 数据库登入密码
	 */
	private String passwd;

	/**
	 * 是否使用连接池
	 */
	private boolean usePool;

	/**
	 * 连接池初始大小
	 */
	private Integer poolInit;

	/**
	 * 连接池最大值
	 */
	private Integer poolMax;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	public boolean isUsePool() {
		return usePool;
	}

	public void setUsePool(boolean usePool) {
		if (usePool) {//初始化的时候将默认值设置
			setPoolMax(poolMax == null ? KettleMgrEnvironment.KETTLE_RECORD_DB_POOL_MAX : poolMax);
			setPoolInit(poolInit == null ? KettleMgrEnvironment.KETTLE_RECORD_DB_POOL_INIT : poolInit);
		}
		this.usePool = usePool;
	}

	public Integer getPoolInit() {
		return poolInit;
	}

	public void setPoolInit(Integer poolInit) {
		this.poolInit = poolInit;
	}

	public Integer getPoolMax() {
		return poolMax;
	}

	public void setPoolMax(Integer poolMax) {
		this.poolMax = poolMax;
	}
}
