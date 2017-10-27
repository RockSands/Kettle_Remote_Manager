package com.kettle.record;

import java.sql.Timestamp;

import org.pentaho.di.repository.RepositoryElementInterface;

import com.kettle.core.KettleVariables;

public abstract class KettleRecord<E extends RepositoryElementInterface> {

	/**
	 * ID
	 */
	private long id;

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
	private Timestamp createTime;

	/**
	 * 更新时间
	 */
	private Timestamp updateTime;

	/**
	 * 异常信息
	 */
	private String errMsg;

	/**
	 * Kettle元数据
	 */
	private E kettleMeta;

	/**
	 * 获取类型
	 * 
	 * @return
	 */
	public abstract String getRecordType();

	/**
	 * 获取Kettle元数据
	 * 
	 * @return
	 */
	public E getKettleMeta() {
		return kettleMeta;
	}

	/**
	 * 设置Kettle元数据
	 */
	public void setKettleMeta(E kettleMeta) {
		this.kettleMeta = kettleMeta;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
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

	public Timestamp getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Timestamp createTime) {
		this.createTime = createTime;
	}

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		if (errMsg.length() > 500) {
			this.errMsg = errMsg.substring(0, 500);
		}
		this.errMsg = errMsg;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public Timestamp getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(Timestamp updateTime) {
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
}
