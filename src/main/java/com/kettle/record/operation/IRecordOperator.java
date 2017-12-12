package com.kettle.record.operation;

import org.pentaho.di.core.exception.KettleException;

import com.kettle.record.KettleRecord;

/**
 * Record的操作定义
 * 
 * @author Administrator
 *
 */
public interface IRecordOperator {
	/**
	 * 加载Record
	 * 
	 * @param record
	 * @return
	 */
	public boolean attachRecord(KettleRecord record);

	/**
	 * 卸载Record
	 * 
	 * @param record
	 * @return
	 */
	public KettleRecord detachRecord();

	/**
	 * 卸载Record
	 * 
	 * @param record
	 * @return
	 */
	public KettleRecord getRecord();

	/**
	 * 是否空闲
	 * 
	 * @return
	 */
	public boolean isAttached();

	/**
	 * 申请
	 */
	public void dealApply() throws KettleException;

	/**
	 * 注册
	 */
	public void dealRegiste() throws KettleException;

	/**
	 * 异常
	 * 
	 * @throws KettleException
	 */
	public void dealError() throws KettleException;

	/**
	 * 完成
	 */
	public void dealFinished() throws KettleException;

	/**
	 * 运行中
	 */
	public void dealRunning() throws KettleException;

	/**
	 * 删除中
	 */
	public void dealRemoving();
}
