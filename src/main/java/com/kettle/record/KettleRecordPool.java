package com.kettle.record;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
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
	 * 
	 * @throws Exception
	 */
	public KettleRecordPool() throws Exception {
		schedulerFactory.getScheduler().start();
	}

	/**
	 * 任务数量
	 * 
	 * @return
	 */
	public int size() {
		return recordQueue.size() + recordPrioritizeQueue.size();

	}

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
	 * 添加的转换任务,该任务仅执行一次
	 * 
	 * @param record
	 * @return 是否添加成功
	 * @throws Exception
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
	 * 添加定时任务
	 * 
	 * @param record
	 * @throws SchedulerException
	 */
	public void addSchedulerRecord(KettleRecord record) throws Exception {
		if (record == null || record.getCronExpression() == null) {
			throw new Exception("添加SchedulerRecord[" + record.getName() + "]失败,未找到CRON表达式!");
		}
		Trigger trigger = TriggerBuilder.newTrigger().startNow()
				.withSchedule(CronScheduleBuilder.cronSchedule(record.getCronExpression())).build();
		JobDataMap newJobDataMap = new JobDataMap();
		newJobDataMap.put("RECORD", record);
		JobDetail jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).setJobData(newJobDataMap).build();
		schedulerFactory.getScheduler().scheduleJob(jobDetail, trigger);
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
	 * 定时调度,将record放入优先队列
	 * 
	 * @author Administrator
	 */
	public class RecordSchedulerJob implements Job {
		@Override
		public void execute(JobExecutionContext arg0) throws JobExecutionException {
			KettleRecord record = (KettleRecord) arg0.getJobDetail().getJobDataMap().get("RECORD");
			addPrioritizeRecord(record);
		}
	}
}
