package com.kettle.core.metas;

import java.util.List;

/**
 * Kettle的SQL元数据定义
 * 
 * @author Administrator
 *
 */
public class KettleSQLSMeta extends KettleDBMeta {
	/**
	 * 执行的SQL
	 */
	private List<String> sqls;

	public List<String> getSqls() {
		return sqls;
	}

	public void setSqls(List<String> sqls) {
		this.sqls = sqls;
	}
}
