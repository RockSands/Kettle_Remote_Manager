package com.kettle;

public class KettleTransResult {
	/**
	 * ID
	 */
	private String transID;
	
	/**
	 * 远程ID
	 */
	private String remoteID;
	/**
	 * 状态
	 */
	private String status;

	public String getTransID() {
		return transID;
	}

	public void setTransID(String transID) {
		this.transID = transID;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getRemoteID() {
		return remoteID;
	}

	protected void setRemoteID(String remoteID) {
		this.remoteID = remoteID;
	}
}
