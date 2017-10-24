package com.kettle.record;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class KettleRecordPool {
	/**
	 * Tran/Job 保存记录名称,队列
	 */
	private final Queue<KettleRecord> recordQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * Tran/Job 保存记录名称,优先队列
	 */
	private final Queue<KettleRecord> recordPrioritizeQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * 添加的转换任务
	 * 
	 * @param transMeta
	 * @param record
	 */
	public void addRecord(KettleRecord record) {
		if (record != null) {
			recordQueue.offer(record);
		}
	}

	/**
	 * 添加的转换任务-优先
	 * 
	 * @param transMeta
	 * @param record
	 */
	public void addPrioritizeRecord(KettleRecord record) {
		if (record != null) {
			recordPrioritizeQueue.offer(record);
		}
	}

	/**
	 * 获取下一个,并在Pool中删除
	 * 
	 * @return
	 */
	public synchronized KettleRecord nextRecord() {
		KettleRecord record = null;
		if (!recordPrioritizeQueue.isEmpty()) {
			record = recordPrioritizeQueue.poll();
			return record;
		}
		if (record == null && !recordQueue.isEmpty()) {
			record = recordQueue.poll();
			return record;
		}
		return null;
	}
}
