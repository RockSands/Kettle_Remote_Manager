package com.kettle;

import java.util.ArrayList;
import java.util.List;

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
	 * 主机信息
	 */
	private String hostname;

	/**
	 * 集群名称
	 */
	private String clusterName;

	/**
	 * 集群的记录
	 */
	private List<KettleTransSplitBean> clusterSplits;

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

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public List<KettleTransSplitBean> getClusterSplits() {
		if (clusterSplits == null) {
			clusterSplits = new ArrayList<KettleTransSplitBean>();
		}
		return clusterSplits;
	}
}
