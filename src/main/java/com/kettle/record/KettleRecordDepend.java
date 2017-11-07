package com.kettle.record;

import java.util.Date;

/**
 * 依赖
 * @author Administrator
 *
 */
public class KettleRecordDepend {
	/**
	 * 主JobID
	 */
	private long masterID;

	/**
	 * 本对象ID
	 */
	private long id;

	/**
	 * 类型
	 */
	private String type;

	/**
	 * 创建时间
	 */
	private Date createTime;

	public long getMasterID() {
		return masterID;
	}

	public void setMasterID(long masterID) {
		this.masterID = masterID;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
}
