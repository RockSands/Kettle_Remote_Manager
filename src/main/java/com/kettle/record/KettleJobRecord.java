package com.kettle.record;

import org.pentaho.di.job.JobMeta;

import com.kettle.core.KettleVariables;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleJobRecord extends KettleRecord<JobMeta> {

	public KettleJobRecord() {
	}

	public KettleJobRecord(JobMeta kettleMeta) {
		super.setKettleMeta(kettleMeta);
	}

	@Override
	public String getRecordType() {
		return KettleVariables.R_HISTORY_RECORD_TYPE_JOB;
	}
}
