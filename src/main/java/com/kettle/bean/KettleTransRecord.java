package com.kettle.bean;

import java.sql.Timestamp;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleTransRecord {
	/**
	 * 对应Tran的Id,全局唯一
	 */
	private long transId;
	/**
	 * 对应Tran的Name
	 */
	private String transName;

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
	 * 集群名称
	 */
	private Timestamp createTime;

	/**
	 * 异常信息
	 */
	private String errMsg;

	public String getTransName() {
		return transName;
	}

	public void setTransName(String transName) {
		this.transName = transName;
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

	public long getTransId() {
		return transId;
	}

	public void setTransId(long transId) {
		this.transId = transId;
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
}
