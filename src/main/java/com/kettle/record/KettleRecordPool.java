package com.kettle.record;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
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

import com.kettle.core.KettleVariables;

public class KettleRecordPool {
	/**
	 * 任务调度工厂
	 */
	private static Scheduler scheduler = null;

	/**
	 * 最大任务数
	 */
	private int recordMax = 1000;

	/**
	 * 存储Record的Map
	 */
	private Map<Long, KettleRecord> recordCache = new HashMap<Long, KettleRecord>();

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
		if (EnvUtil.getSystemProperty("KETTLE_RECORD_POOL_MAX") != null) {
			recordMax = Integer.valueOf(EnvUtil.getSystemProperty("KETTLE_RECORD_POOL_MAX"));
		}
		SchedulerFactory schedulerfactory = new StdSchedulerFactory();
		scheduler = schedulerfactory.getScheduler();
		scheduler.start();
	}

	/**
	 * 任务数量验证
	 * 
	 * @return
	 * @throws KettleException
	 */
	private void check() throws KettleException {
		if (size() > recordMax) {
			throw new KettleException("KettleRecordPool的任务数量已满,无法接受任务!");
		}
	}

	/**
	 * @param record
	 * @return
	 */
	private boolean isAcceptedRecord(KettleRecord record) {
		if (record == null) {
			return true;
		}
		return recordCache.containsKey(record.getId());
	}

	/**
	 * 添加的转换任务,该任务仅执行一次
	 * 
	 * @param record
	 * @return 是否添加成功
	 * @throws KettleException
	 */
	public synchronized boolean addRecord(KettleRecord record) throws KettleException {
		if (record != null) {
			if (!isAcceptedRecord(record)) {
				check();
				record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
				recordCache.put(record.getId(), record);
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
				recordCache.put(record.getId(), record);
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
			// 如果Cache不存在,则直接下一个,该Record已经删除或处理完成
			if (!recordCache.containsKey(record.getId())) {
				record = nextRecord();
			}
		}
		return record;
	}

	/**
	 * 删除
	 * 
	 * @param jobID
	 */
	public void deleteRecord(long jobID) {
		recordCache.remove(jobID);
	}

	/**
	 * 获取Record
	 * 
	 * @param id
	 * @return
	 */
	public KettleRecord getRecord(long id) {
		return recordCache.get(id);
	}

	/**
	 * 任务数量
	 * 
	 * @return
	 */
	public int size() {
		return recordQueue.size() + recordPrioritizeQueue.size();
	}
}
