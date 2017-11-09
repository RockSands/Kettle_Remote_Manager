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
	private KettleRecord applyRecord;

	public RemoteRecordHandler(KettleRemoteClient client) {
		this.client = client;
	}

	/**
	 * 是否准备接受Record
	 * 
	 * @return
	 */
	public synchronized boolean isReady() {
		return applyRecord == null;
	}

	/**
	 * 启动Record
	 * 
	 * @param record
	 * @throws Exception
	 */
	public synchronized void startRecord(KettleRecord record) throws Exception {
		if (!isReady()) {
			throw new KettleException("RemoteRecordProcess运行中,无法执行新的Record[" + record.getUuid() + "]");
		}
		checkStatus(record);
		applyRecord = record;
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		try {
			client.remoteSendJob(record);
		} catch (Exception ex) {
			record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
			record.setErrMsg(ex.getMessage());
			applyRecord = null;
			throw new KettleException("Remote[" + client.getHostName() + "]无法远端执行Record[" + record.getUuid() + "]!",
					ex);
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

	/**
	 * 检查Record
	 * 
	 * @param record
	 * @throws KettleException
	 */
	private void checkStatus(KettleRecord record) throws KettleException {
		if (record.getKettleMeta() == null) {
			throw new KettleException(
					"record[" + record.getUuid() + "]未定义Meta，无法被Remote[" + client.getHostName() + "]处理");
		}
		if (!client.isRunning()) {
			throw new KettleException("Remote[" + client.getHostName() + "]异常状态,无法处理新的Record!");
		}
	}
}
