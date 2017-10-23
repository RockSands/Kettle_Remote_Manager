package com.kettle.record;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class KettleRecordPool {
	/**
	 * Tran/Job 保存记录名称,队列
	 */
	private final Queue<KettleRecord> recordQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * 添加的转换任务
	 * 
	 * @param transMeta
	 * @param record
	 */
	public void addRecords(KettleRecord record) {
		if (record != null) {
			recordQueue.add(record);
		}
	}

	/**
	 * 获取下一个,并在Pool中删除
	 * 
	 * @return
	 */
	public synchronized KettleRecord nextRecord() {
		return recordQueue.poll();
	}
}
