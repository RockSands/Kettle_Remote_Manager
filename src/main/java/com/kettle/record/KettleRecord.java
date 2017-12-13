package com.kettle.record;

import java.util.Date;

import org.pentaho.di.job.JobMeta;

import com.kettle.core.KettleVariables;

/**
 * Kettle记录
 * 
 * @author Administrator
 *
 */
public class KettleRecord {

	/**
	 * ID
	 */
	private String uuid;

	/**
	 * ID
	 */
	private String jobid;

	/**
	 * 名称
	 */
	private String name;

	/**
	 * 运行ID-远程的执行ObjectID
	 */
	private String runID;
	/**
	 * 状态
	 */
	private String status;

	/**
	 * 主机信息
	 */
	private String hostname;

	/**
	 * CRON表达式
	 */
	private String cronExpression;

	/**
	 * 创建时间
	 */
	private Date createTime;

	/**
	 * 更新时间
	 */
	private Date updateTime;

	/**
	 * 异常信息
	 */
	private String errMsg;

	/**
	 * Job元数据
	 */
	private JobMeta KettleMeta;

	public KettleRecord() {
	}

	public JobMeta getKettleMeta() {
		return KettleMeta;
	}

	public void setKettleMeta(JobMeta kettleMeta) {
		KettleMeta = kettleMeta;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRunID() {
		return runID;
	}

	public void setRunID(String runID) {
		this.runID = runID;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		if (errMsg != null) {
			if (errMsg.length() > 500) {
				this.errMsg = errMsg.trim().substring(0, 500);
			} else {
				this.errMsg = errMsg;
			}
		}
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public Date getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}

	/**
	 * 是否运行状态
	 * 
	 * @return
	 */
	public boolean isRunning() {
		return KettleVariables.RECORD_STATUS_RUNNING.equals(this.getStatus());
	}

	/**
	 * 是否受理状态
	 * 
	 * @return
	 */
	public boolean isApply() {
		return KettleVariables.RECORD_STATUS_APPLY.equals(this.getStatus());
	}

	/**
	 * 是否异常状态
	 * 
	 * @return
	 */
	public boolean isError() {
		return KettleVariables.RECORD_STATUS_ERROR.equals(this.getStatus());
	}

	/**
	 * 是否完成中
	 * 
	 * @return
	 */
	public boolean isFinished() {
		return KettleVariables.RECORD_STATUS_FINISHED.equals(this.getStatus());
	}

	/**
	 * 是否注册
	 * 
	 * @return
	 */
	public boolean isRegiste() {
		return KettleVariables.RECORD_STATUS_REGISTE.equals(this.getStatus());
	}
}
