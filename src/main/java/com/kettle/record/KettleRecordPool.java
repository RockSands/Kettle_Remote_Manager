package com.kettle.record;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;

public class KettleRecordPool {
	/**
	 * 日志
	 */
	Logger logger = LoggerFactory.getLogger(KettleRecordPool.class);
	/**
	 * 任务调度工厂
	 */
	Scheduler scheduler = null;
	/**
	 * Record队列的标识集合
	 */
	private Set<Long> recordQueueSet = new HashSet<Long>();
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
		SchedulerFactory schedulerfactory = new StdSchedulerFactory();
		scheduler = schedulerfactory.getScheduler();
		scheduler.start();
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
		return recordQueueSet.contains(record.getId());
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
				record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
				recordQueueSet.add(record.getId());
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
		// 循环任务默认为完成,等待下次执行
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(String.valueOf(record.getId())).startNow()
				.withSchedule(CronScheduleBuilder.cronSchedule(record.getCronExpression())).build();
		JobDataMap newJobDataMap = new JobDataMap();
		newJobDataMap.put("RECORD", record);
		newJobDataMap.put("RECORDPOOL", this);
		JobDetail jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).withIdentity(String.valueOf(record.getId()))
				.setJobData(newJobDataMap).build();
		scheduler.scheduleJob(jobDetail, trigger);
	}

	/**
	 * 更新
	 * 
	 * @param jobID
	 * @param newCron
	 * @throws Exception
	 */
	public void modifySchedulerRecord(long jobID, String newCron) throws Exception {
		if (newCron == null || "".equals(newCron.trim())) {
			throw new Exception("修改SchedulerRecord[" + jobID + "]是CRON表达式不能为空!");
		}
		TriggerKey triggerKey = new TriggerKey(String.valueOf(jobID));
		if (!scheduler.checkExists(triggerKey)) {
			throw new Exception("修改SchedulerRecord[" + jobID + "]失败,记录未找到!");
		}
		Trigger newTrigger = TriggerBuilder.newTrigger().withIdentity(String.valueOf(jobID)).startNow()
				.withSchedule(CronScheduleBuilder.cronSchedule(newCron)).build();
		scheduler.rescheduleJob(triggerKey, newTrigger);
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
				recordQueueSet.add(record.getId());
				return recordPrioritizeQueue.offer(record);
			}
		}
		return false;
	}

	public synchronized void deleteRecord(long jobID) {
		recordQueueSet.remove(jobID);
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
			// 如果Set存在,表示还在受理
			if (recordQueueSet.contains(record.getId())) {
				recordQueueSet.remove(record.getId());
			} else {
				// 如果Set不存在,则直接下一个,该Record已经删除
				record = nextRecord();
			}
		}
		return record;
	}
}
