package com.kettle.record;

import org.pentaho.di.job.JobMeta;

import com.kettle.core.KettleVariables;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleJobRecord extends KettleRecord {
	/**
	 * Job元数据
	 */
	private JobMeta KettleMeta;

	public KettleJobRecord() {
	}

	public KettleJobRecord(JobMeta KettleMeta) {
		this.KettleMeta = KettleMeta;
	}

	public JobMeta getKettleMeta() {
		return KettleMeta;
	}

	public void setKettleMeta(JobMeta kettleMeta) {
		KettleMeta = kettleMeta;
	}

	@Override
	public String getRecordType() {
		return KettleVariables.R_HISTORY_RECORD_TYPE_JOB;
	}
}
