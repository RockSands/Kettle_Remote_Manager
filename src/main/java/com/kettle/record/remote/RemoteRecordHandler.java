package com.kettle.record.remote;

import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.record.KettleRecord;
import com.kettle.remote.KettleRemoteClient;

public class RemoteRecordHandler {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteRecordHandler.class);
	/**
	 * 远端
	 */
	private final KettleRemoteClient client;

	/**
	 * 处理的任务
	 */
	private final KettleRecord[] recordArr;

	/**
	 * @param client
	 */
	public RemoteRecordHandler(KettleRemoteClient client) {
		this.client = client;
		this.recordArr = new KettleRecord[client.maxRecord];
	}

	/**
	 * 可以接受多少的任务
	 * 
	 * @return
	 */
	public synchronized int readyCount() {
		int count = 0;
		for (KettleRecord record : recordArr) {
			if (record == null) {
				count++;
			}
		}
		return count;
	}

	/**
	 * 启动Record
	 * 
	 * @param record
	 * @throws Exception
	 */
	public synchronized boolean addRecord(KettleRecord record) {
		if (readyCount() > 0 && client.isRunning()) {
			return false;
		}
		int index = 0;
		for (; index < recordArr.length; index++) {
			if (recordArr[index] == null) {
				recordArr[index] = record;
				break;
			}
		}
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		try {
			client.remoteSendJob(record);
			record.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
			return true;
		} catch (Exception ex) {
			record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			record.setErrMsg(ex.getMessage());
			recordArr[index] = null;
			return false;
		}
	}

	/**
	 * 同步远端状态
	 * 
	 * @throws Exception
	 */
	public void syncRemoteStatus() throws Exception {
		client.remoteJobStatus(applyRecord);
	}

	/**
	 * @return
	 */
	public KettleRecord getRecord() {
		return applyRecord;
	}

	/**
	 * 释放applyRecord
	 * 
	 * @param record
	 * @return
	 */
	public KettleRecord releaseRecord(boolean remoteClean) {
		KettleRecord record = applyRecord;
		if (remoteClean) {
			try {
				client.remoteRemoveJob(record);
			} catch (Exception e) {
				logger.error("Remote[" + client.getHostName() + "]释放Record[" + record.getUuid() + "]发生异常!", e);
			}
		}
		applyRecord = null;
		return record;
	}
}
