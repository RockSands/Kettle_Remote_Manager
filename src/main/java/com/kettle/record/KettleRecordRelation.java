package com.kettle.record;

import java.util.Date;

/**
 * Kettle记录依赖
 * @author Administrator
 *
 */
public class KettleRecordRelation {
	/**
	 * 主JobUUID
	 */
	private String masterUUID;

	/**
	 * Kettle对象ID
	 */
	private String metaid;

	/**
	 * 类型
	 */
	private String type;

	/**
	 * 创建时间
	 */
	private Date createTime;

	public String getMasterUUID() {
		return masterUUID;
	}

	public void setMasterUUID(String masterUUID) {
		this.masterUUID = masterUUID;
	}

	public String getMetaid() {
		return metaid;
	}

	public void setMetaid(String metaid) {
		this.metaid = metaid;
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
