package com.kettle.core.bean;

public class KettleResult {
	/**
	 * id
	 */
	private long id;
	/**
	 * 状态
	 */
	private String status;

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
