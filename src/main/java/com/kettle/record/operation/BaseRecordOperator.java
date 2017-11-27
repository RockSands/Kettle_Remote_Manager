package com.kettle.record.operation;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.core.bean.KettleRecord;

public abstract class BaseRecordOperator implements IRecordOperator {

	/**
	 * 任务
	 */
	protected KettleRecord record;

	/**
	 * 获取任务
	 * 
	 * @return
	 */
	protected KettleRecord getRecord() {
		return record;
	}

	/**
	 * 设置任务
	 * 
	 * @param record
	 */
	public void setRecord(KettleRecord record) {
		this.record = record;
	}

	/**
	 * 处理任务
	 * 
	 * @param record
	 * @throws KettleException
	 */
	public void dealRecord() throws KettleException {
		if (record.isApply()) {
			dealApply();
		} else if (record.isRegiste()) {
			dealRegiste();
		} else if (record.isRepeat()) {
			dealRepeat();
		} else if (record.isRunning()) {
			dealRunning();
		} else if (record.isFinished()) {
			dealFinished();
		} else {
			dealError();
		}
	}
}
