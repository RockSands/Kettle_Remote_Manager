package com.kettle.record.bean;

import org.pentaho.di.trans.TransMeta;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleRecord;

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

	@Override
	public String getRecordType() {
		return KettleVariables.R_HISTORY_RECORD_TYPE_TRANS;
	}

}
