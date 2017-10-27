package com.kettle.record;

import org.pentaho.di.trans.TransMeta;

import com.kettle.core.KettleVariables;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleTransRecord extends KettleRecord<TransMeta> {

	public KettleTransRecord() {
	}

	public KettleTransRecord(TransMeta transMeta) {
		super.setKettleMeta(transMeta);
	}

	@Override
	public String getRecordType() {
		return KettleVariables.R_HISTORY_RECORD_TYPE_TRANS;
	}

}
