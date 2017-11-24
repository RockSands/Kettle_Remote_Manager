package com.kettle.core.bean;

/**
 * Kettle的结果,北向
 * @author Administrator
 *
 */
public class KettleResult {
	/**
	 * id
	 */
	private String uuid;
	/**
	 * 状态
	 */
	private String status;

	/**
	 * 异常信息
	 */
	private String errMsg;

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
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
