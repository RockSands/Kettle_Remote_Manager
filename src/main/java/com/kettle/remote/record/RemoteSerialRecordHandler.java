package com.kettle.remote.record;

import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private final RemoteRecordOperator[] remoteRecordOperators;

	/**
	 * @param client
	 */
	public RemoteSerialRecordHandler(KettleRemoteClient remoteClient) {
		this.remoteClient = remoteClient;
		this.remoteRecordOperators = new RemoteRecordOperator[remoteClient.maxRecord];
		for (int i = 0; i < remoteClient.maxRecord; i++) {
			remoteRecordOperators[i] = new RemoteRecordOperator(remoteClient);
		}
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
	}

	/**
	 * 排序,将Null放到最后
	 */
	private void sortRemoteRecordOperators() {
		Arrays.sort(remoteRecordOperators, new Comparator<RemoteRecordOperator>() {
			public int compare(RemoteRecordOperator o1, RemoteRecordOperator o2) {
				if (o1.getRecord() == null) {
					return 1;
				}
				if (o2.getRecord() == null) {
					return -1;
				}
				return 0;
			}
		});
	}

	@Override
	public void run() {
		try {
			sortRemoteRecordOperators();
			KettleRecord recordTMP = null;
			for (RemoteRecordOperator remoteRecordOperator : remoteRecordOperators) {
				// 如果已经加载,直接进行任务处理
				if (remoteRecordOperator.isAttached()) {
					// 进行处理
					remoteRecordOperator.dealRecord();
					if (remoteRecordOperator.isFinished()) {
						remoteRecordOperator.detachRecord();
					}
				} else {
					recordTMP = recordPool.nextRecord();
					if (recordTMP == null) {
						break;
					}
				}
				// 尝试自动加载任务
				if (!remoteRecordOperator.isAttached()) {
					// 自动加载任务
					if (remoteRecordOperator.attachRecord(recordTMP)) {
						remoteRecordOperator.dealRecord();
					} else {
						recordPool.addPrioritizeRecord(recordTMP);
					}
				}
			}
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteClient.getHostName() + "]定时任务发生异常成!", ex);
		}
	}

}
