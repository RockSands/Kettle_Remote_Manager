package com.kettle.record;

import java.sql.Timestamp;

import com.kettle.core.KettleVariables;

public abstract class KettleRecord {

	private long id;

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
	 * 创建时间
	 */
	private Timestamp createTime;

	/**
	 * 异常信息
	 */
	private String errMsg;

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
		this.errMsg = errMsg;
	}

	/**
	 * 是否运行中
	 * 
	 * @return
	 */
	public boolean isRunning() {
		return KettleVariables.RECORD_STATUS_RUNNING.equals(this.getStatus());
	}
}
