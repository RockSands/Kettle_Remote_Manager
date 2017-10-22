package com.kettle.record;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KettleRecordPool {
	/**
	 * Tran/Job 保存记录名称,队列
	 */
	private final BlockingQueue<KettleRecord> recordQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * 添加的转换任务
	 * 
	 * @param transMeta
	 * @param record
	 */
	public void addRecords(KettleRecord record) {
		recordQueue.offer(record);
	}

	/**
	 * 获取下一个
	 * 
	 * @return
	 */
	public synchronized KettleRecord take() {
		if (recordQueue.isEmpty()) {
			return null;
		}
		try {
			return recordQueue.take();
		} catch (InterruptedException e) {
			return null;
		}
	}
}
