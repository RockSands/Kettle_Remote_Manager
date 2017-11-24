package com.kettle.core.instance.metas.builder;

import java.util.List;

import org.apache.commons.beanutils.BeanUtils;

import com.kettle.core.instance.metas.KettleSQLSMeta;

/**
 * KettleSQL元数据构建器
 * @author Administrator
 *
 */
public class KettleSQLSMetaBuilder {
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
	 * 执行的SQL
	 */
	private List<String> sqls;

	public String getType() {
		return type;
	}

	public KettleSQLSMetaBuilder dbtype(String type) {
		this.type = type;
		return this;
	}

	public String getHost() {
		return host;
	}

	public KettleSQLSMetaBuilder dbHost(String host) {
		this.host = host;
		return this;
	}

	public String getPort() {
		return port;
	}

	public KettleSQLSMetaBuilder dbPort(String port) {
		this.port = port;
		return this;
	}

	public String getDatabase() {
		return database;
	}

	public KettleSQLSMetaBuilder dbDatabase(String database) {
		this.database = database;
		return this;
	}

	public String getUser() {
		return user;
	}

	public KettleSQLSMetaBuilder dbUser(String user) {
		this.user = user;
		return this;
	}

	public String getPasswd() {
		return passwd;
	}

	public KettleSQLSMetaBuilder dbPasswd(String passwd) {
		this.passwd = passwd;
		return this;
	}

	public List<String> getSqls() {
		return sqls;
	}

	public KettleSQLSMetaBuilder excuteSqls(List<String> sqls) {
		this.sqls = sqls;
		return this;
	}

	public KettleSQLSMeta build() {
		KettleSQLSMeta sqlsMeta = new KettleSQLSMeta();
		try {
			BeanUtils.copyProperties(sqlsMeta, this);
		} catch (Exception e) {
			throw new RuntimeException("KettleTableMetaBuilder使用BeanUtils失败!", e);
		}
		return sqlsMeta;
	}
}
