package com.kettle;

public class KettleTransResult {
	/**
	 * TransID
	 */
	private long transID;
	/**
	 * 状态
	 */
	private String status;

	/**
	 * 异常信息
	 */
	private String errMsg;

	public long getTransID() {
		return transID;
	}

	public void setTransID(long transID) {
		this.transID = transID;
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
}
