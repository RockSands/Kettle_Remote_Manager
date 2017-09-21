package com.kettle;

/**
 * 数据库实体对象
 * 
 * @author Administrator
 *
 */
public class KettleTransSplitBean {

	private Long splitId;
	/**
	 * 
	 */
	private String transName;
	/**
	 * 状态
	 */
	private String status;
	/**
	 * 主机信息
	 */
	private String hostname;

	public Long getSplitId() {
		return splitId;
	}

	public void setSplitId(Long splitId) {
		this.splitId = splitId;
	}

	public String getTransName() {
		return transName;
	}

	public void setTransName(String transName) {
		this.transName = transName;
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
}
