package com.kettle.core.metas;

import java.util.List;

/**
 * 
 * Kettle的Table描述
 * 
 * @author Administrator
 *
 */
public class KettleTableMeta extends KettleDBMeta {

	/**
	 * 执行的SQL
	 */
	private String sql;

	/**
	 * 表名
	 */
	private String tableName;

	/**
	 * 使用的列
	 */
	private List<String> columns;

	/**
	 * 唯一标识数据行的列组
	 */
	private List<String> pkcolumns;

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
