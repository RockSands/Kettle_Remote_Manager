package com.kettle.core.instance.metas.builder;

import java.util.List;
import org.apache.commons.beanutils.BeanUtils;
import com.kettle.core.instance.metas.KettleTableMeta;

/**
 * 构建
 * 
 * @author Administrator
 *
 */
public class KettleTableMetaBuilder {
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

	public KettleTableMetaBuilder dbType(String type) {
		this.type = type;
		return this;
	}

	public KettleTableMetaBuilder dbHost(String host) {
		this.host = host;
		return this;
	}

	public KettleTableMetaBuilder dbPort(String port) {
		this.port = port;
		return this;
	}

	public KettleTableMetaBuilder dbBase(String database) {
		this.database = database;
		return this;
	}

	public KettleTableMetaBuilder dbUser(String user) {
		this.user = user;
		return this;
	}

	public KettleTableMetaBuilder dbPasswd(String passwd) {
		this.passwd = passwd;
		return this;
	}

	public KettleTableMetaBuilder selectSql(String sql) {
		this.sql = sql;
		return this;
	}

	public KettleTableMetaBuilder pkcolumns(List<String> pkcolumns) {
		this.pkcolumns = pkcolumns;
		return this;
	}

	public KettleTableMetaBuilder tableName(String tableName) {
		this.tableName = tableName;
		return this;
	}

	public String getType() {
		return type;
	}

	public String getHost() {
		return host;
	}

	public String getPort() {
		return port;
	}

	public String getDatabase() {
		return database;
	}

	public String getUser() {
		return user;
	}

	public String getPasswd() {
		return passwd;
	}

	public String getSql() {
		return sql;
	}

	public List<String> getColumns() {
		return columns;
	}

	public List<String> getPkcolumns() {
		return pkcolumns;
	}

	public String getTableName() {
		return tableName;
	}

	public KettleTableMeta build() {
		KettleTableMeta tableMeta = new KettleTableMeta();
		try {
			BeanUtils.copyProperties(tableMeta, this);
		} catch (Exception e) {
			throw new RuntimeException("KettleTableMetaBuilder使用BeanUtils失败!", e);
		}
		return tableMeta;
	}
}
