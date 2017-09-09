package com.kettle;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleTransBean {
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
	 * 异常信息
	 */
	private String errMsg;

	/**
	 * 主机信息
	 */
	private String hostname;

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

	public String getErrMsg() {
		return errMsg;
	}

	public void setErrMsg(String errMsg) {
		this.errMsg = errMsg;
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
}
