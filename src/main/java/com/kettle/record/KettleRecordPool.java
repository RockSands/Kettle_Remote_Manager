package com.kettle.record;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.StdSchedulerFactory;

public class KettleRecordPool {

	/**
	 * 任务调度工厂
	 */
	SchedulerFactory schedulerFactory = new StdSchedulerFactory();
	/**
	 * Record队列的标识集合
	 */
	private Set<String> recordQueueSet = new HashSet<String>();
	/**
	 * Tran/Job 保存记录名称,队列
	 */
	private final Queue<KettleRecord> recordQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * Tran/Job 保存记录名称,优先队列
	 */
	private final Queue<KettleRecord> recordPrioritizeQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * @param record
	 * @return
	 */
	public boolean isAcceptedRecord(KettleRecord record) {
		if (record == null) {
			return true;
		}
		return recordQueueSet.contains(record.getRecordType() + "_" + record.getId());
	}

	/**
	 * @param record
	 * @return
	 */
	public void removeRecordFlag(KettleRecord record) {
		recordQueueSet.remove(record.getRecordType() + "_" + record.getId());
	}

	/**
	 * 添加的转换任务
	 * 
	 * @param record
	 * @return 是否添加成功
	 */
	public synchronized boolean addRecord(KettleRecord record) {
		if (record != null) {
			if (!isAcceptedRecord(record)) {
				return recordQueue.offer(record);
			}
		}
		return false;
	}

	/**
	 * 添加的转换任务-优先
	 * 
	 * @param record
	 * @return 是否添加成功
	 */
	public synchronized boolean addPrioritizeRecord(KettleRecord record) {
		if (record != null) {
			if (!isAcceptedRecord(record)) {
				return recordPrioritizeQueue.offer(record);
			}
		}
		return false;
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
		}
		if (record == null && !recordQueue.isEmpty()) {
			record = recordQueue.poll();
		}
		if (record != null) {
			removeRecordFlag(record);
		}
		return record;
	}

	/**
	 * 任务记录
	 * 
	 * @param record
	 */
	public void addRecordSchedule(RecordScheduler scheduler) {
		JobDetail jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).build();
		SimpleTrigger simpleTrigger = new SimpleTrigger("simpleTrigger", "triggerGroup-s1");
		CronScheduleBuilder.cronSchedule(scheduler.getCronExpression());
	}

	/**
	 * 
	 * @author Administrator
	 *
	 */
	private class RecordSchedulerJob implements Job {

		@Override
		public void execute(JobExecutionContext arg0) throws JobExecutionException {

		}

	}
}
