package com.kettle.record;

import org.pentaho.di.job.JobMeta;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleJobRecord extends KettleRecord {
	/**
	 * 类型
	 */
	private String type;

	/**
	 * Job元数据
	 */
	private JobMeta KettleMeta;

	public KettleJobRecord() {
	}

	public KettleJobRecord(JobMeta KettleMeta) {
		this.KettleMeta = KettleMeta;
	}

	@Override
	public long getId() {
		return Long.valueOf(KettleMeta.getObjectId().getId());
	}

	@Override
	public String getName() {
		return KettleMeta.getName();
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public JobMeta getKettleMeta() {
		return KettleMeta;
	}

	public void setKettleMeta(JobMeta kettleMeta) {
		KettleMeta = kettleMeta;
	}
}
