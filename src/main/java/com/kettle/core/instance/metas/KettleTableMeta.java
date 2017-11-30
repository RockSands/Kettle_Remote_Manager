package com.kettle.core.instance.metas;

import java.util.List;

/**
 * 
 * Kettle的Table描述
 * @author Administrator
 *
 */
public class KettleTableMeta {
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
	private String sql;

	/**
	 * SQL执行出的所有列
	 */
	private List<String> columns;

	/**
	 * 唯一标识数据行的列组
	 */
	private List<String> pkcolumns;

	/**
	 * 表名
	 */
	private String tableName;

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

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public List<String> getPkcolumns() {
		return pkcolumns;
	}

	public void setPkcolumns(List<String> pkcolumns) {
		this.pkcolumns = pkcolumns;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
