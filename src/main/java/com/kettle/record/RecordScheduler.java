package com.kettle.record;

public class RecordScheduler {
	/**
	 * id
	 */
	private long id;

	/**
	 * name
	 */
	private String name;

	/**
	 * Record类别
	 */
	private String recordType;

	/**
	 * 调度类型
	 */
	private String schedulerType;

	/**
	 * CRON表达式
	 */
	private String cronExpression;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getRecordType() {
		return recordType;
	}

	public void setRecordType(String recordType) {
		this.recordType = recordType;
	}

	public String getSchedulerType() {
		return schedulerType;
	}

	public void setSchedulerType(String schedulerType) {
		this.schedulerType = schedulerType;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}
}
