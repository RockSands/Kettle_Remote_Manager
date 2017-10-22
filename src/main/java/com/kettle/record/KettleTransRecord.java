package com.kettle.record;

import org.pentaho.di.trans.TransMeta;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleTransRecord extends KettleRecord {

	/**
	 * Job元数据
	 */
	private TransMeta KettleMeta;

	public KettleTransRecord() {
	}

	public KettleTransRecord(TransMeta KettleMeta) {
		this.KettleMeta = KettleMeta;
	}

	public TransMeta getKettleMeta() {
		return KettleMeta;
	}

	public void setKettleMeta(TransMeta kettleMeta) {
		KettleMeta = kettleMeta;
	}

}
