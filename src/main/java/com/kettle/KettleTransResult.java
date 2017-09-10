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
}
