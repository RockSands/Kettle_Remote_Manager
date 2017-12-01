package com.kettle.remote.record;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;
import com.kettle.record.pool.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;

/**
 * Kettle远程串行行服务,模型比较保守
 * 
 * @author Administrator
 *
 */
public class RemoteSerialRecordHandler implements Runnable {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RemoteSerialRecordHandler.class);

	/**
	 * 远端
	 */
	private final KettleRemoteClient remoteClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool recordPool;

	/**
	 * 处理的任务
	 */
	private final RemoteRecordOperator remoteRecordOperator;

	/**
	 * 保存遗留的任务
	 */
	private List<KettleRecord> kettleRecords = new LinkedList<KettleRecord>();

	/**
	 * @param client
	 */
	public RemoteSerialRecordHandler(KettleRemoteClient remoteClient, List<KettleRecord> oldRecords) {
		this.remoteClient = remoteClient;
		this.remoteRecordOperator = new RemoteRecordOperator(remoteClient);
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		if (oldRecords != null && !oldRecords.isEmpty()) {
			this.kettleRecords.addAll(oldRecords);
		}
	}

	/**
	 * 获取记录
	 * 
	 * @return
	 */
	private synchronized void fetchRecord() {
		KettleRecord recordTMP = null;
		if (!remoteClient.isRunning()) {
			dealRemoteErrorRecords();
		} else {
			while (kettleRecords.size() < KettleMgrEnvironment.KETTLE_RECORD_MAX_PER_REMOTE) {
				recordTMP = recordPool.nextRecord();
				if (recordTMP == null) {
					break;
				} else {
					kettleRecords.add(recordTMP);
				}
			}
		}
	}

	/**
	 * 远程错误是对Record的处理
	 */
	private void dealRemoteErrorRecords() {
		KettleRecord recordTMP = null;
		if (kettleRecords.isEmpty()) {
			return;
		}
		for (Iterator<KettleRecord> it = kettleRecords.iterator(); it.hasNext();) {
			recordTMP = it.next();
			if (recordTMP != null && recordTMP.isApply()) {
				recordPool.addPrioritizeRecord(recordTMP);
				it.remove();
			}
			if (recordTMP.isRunning()
					&& (System.currentTimeMillis() - recordTMP.getUpdateTime().getTime()) / 1000 > 5) {
				try {
					recordTMP.setStatus(KettleVariables.RECORD_STATUS_ERROR);
					recordTMP.setErrMsg("Remote[" + remoteClient.getHostName() + "]长时间无法连接!");
					remoteRecordOperator.attachRecordForce(recordTMP);
					remoteRecordOperator.dealRecord();
					remoteRecordOperator.detachRecord();
				} catch (Exception ex) {
					logger.error("Remote[" + remoteClient.getHostName() + "]无法连接,对运行的的Record[" + recordTMP.getUuid()
							+ "]处理发生异常!", ex);
				}
				it.remove();
			}
		}
	}

	/**
	 * 获取记录
	 * 
	 * @return
	 */
	private synchronized void dealRecords() {
		KettleRecord indexRecord;
		for (Iterator<KettleRecord> it = kettleRecords.iterator(); it.hasNext();) {
			indexRecord = it.next();
			if (indexRecord == null) {
				continue;
			}
			try {
				remoteRecordOperator.attachRecord(indexRecord);
				remoteRecordOperator.dealRecord();
			} catch (Exception ex) {
				logger.error("Kettle远端[" + remoteClient.getHostName() + "]处理record[" + indexRecord.getUuid() + "]发生异常!",
						ex);
			} finally {
				remoteRecordOperator.detachRecord();
			}
			if (indexRecord.isError() || indexRecord.isFinished()) {
				it.remove();
			}
		}
	}

	@Override
	public void run() {
		logger.debug("Kettle远端进程[" + remoteClient.getHostName() + "]守护进程唤醒!");
		try {
			fetchRecord();
			dealRecords();
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteClient.getHostName() + "]守护进程结束!", ex);
		}
	}

}
