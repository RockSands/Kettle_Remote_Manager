package com.kettle.remote.record;

import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.bean.KettleRecord;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecordPool;
import com.kettle.remote.KettleRemoteClient;

/**
 * Kettle远程任务处理,串行单元
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
		logger.debug("Kettle远端[" + remoteClient.getHostName() + "]定时任务轮询启动!");
		try {
			sortRemoteRecordOperators();
			KettleRecord recordTMP = null;
			for (RemoteRecordOperator recordOperator : remoteRecordOperators) {
				if (!recordOperator.isAttached()) { // 加载下一个
					recordTMP = recordPool.nextRecord();
					if (recordTMP != null && recordOperator.attachRecord(recordTMP)) {
						recordPool.addPrioritizeRecord(recordTMP);
					}
				}
				if (!recordOperator.isAttached()) {// 如果扔未加载成功,表明没有任务,直接完成
					continue;
				} else {
					// 进行处理
					recordOperator.dealRecord();
					if (recordOperator.isError() || recordOperator.isFinished()) {
						KettleRecord record = recordOperator.detachRecord();
						recordPool.deleteRecord(record.getUuid());
						logger.debug(
								"Kettle远端[" + remoteClient.getHostName() + "]已经处理完成Record[" + record.getUuid() + "]!");
					} else {
						continue;
					}
				}
			}
		} catch (Exception ex) {
			logger.error("Kettle远端[" + remoteClient.getHostName() + "]定时任务发生异常成!", ex);
		}
		logger.debug("Kettle远端[" + remoteClient.getHostName() + "]定时任务轮询完成!");
	}

}
