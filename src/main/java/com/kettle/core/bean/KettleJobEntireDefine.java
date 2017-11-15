package com.kettle.core.bean;

import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.job.JobMeta;
import org.pentaho.di.trans.TransMeta;

/**
 * KettleJob的完整定义
 * @author Administrator
 *
 */
public class KettleJobEntireDefine {
	/**
	 * 
	 */
	private String uuid;
	/**
	 * 
	 */
	private List<TransMeta> dependentTrans;
	/**
	 * 
	 */
	private List<JobMeta> dependentJobs;
	/**
	 * 
	 */
	private JobMeta mainJob;
	
	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public JobMeta getMainJob() {
		return mainJob;
	}

	public void setMainJob(JobMeta mainJob) {
		this.mainJob = mainJob;
	}

	public List<TransMeta> getDependentTrans() {
		if (dependentTrans == null) {
			dependentTrans = new LinkedList<TransMeta>();
		}
		return dependentTrans;
	}

	public List<JobMeta> getDependentJobs() {
		if (dependentJobs == null) {
			dependentJobs = new LinkedList<JobMeta>();
		}
		return dependentJobs;
	}
}
