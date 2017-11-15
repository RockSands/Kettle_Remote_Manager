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
import com.kettle.core.bean.KettleRecord;

/**
 * Kettle任务池
 * 
 * @author chenkw
 *
 */
public class KettleRecordPool {
	/**
	 * Repeat任务调度工厂
	 */
	private static Scheduler scheduler = null;

	/**
	 * 最大任务数
	 */
	private int recordMax = 1000;

	/**
	 * 存储Record的Map
	 */
	private Map<String, KettleRecord> recordCache = new HashMap<String, KettleRecord>();

	/**
	 * 记录队列
	 */
	private final Queue<KettleRecord> recordQueue = new LinkedBlockingQueue<KettleRecord>();

	/**
	 * 优先记录队列
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
				recordCache.put(record.getUuid(), record);
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
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(record.getUuid()).startNow()
				.withSchedule(CronScheduleBuilder.cronSchedule(record.getCronExpression())).build();
		JobDataMap newJobDataMap = new JobDataMap();
		newJobDataMap.put("RECORD", record);
		newJobDataMap.put("RECORDPOOL", this);
		JobDetail jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).withIdentity(record.getUuid())
				.setJobData(newJobDataMap).build();
		scheduler.scheduleJob(jobDetail, trigger);
	}

	/**
	 * 更新重复任务的策略
	 * 
	 * @param jobID
	 * @param newCron
	 * @throws Exception
	 */
	public void modifySchedulerRecord(String uuid, String newCron) throws Exception {
		if (newCron == null || "".equals(newCron.trim())) {
			throw new Exception("修改SchedulerRecord[" + uuid + "]是CRON表达式不能为空!");
		}
		TriggerKey triggerKey = new TriggerKey(String.valueOf(uuid));
		if (!scheduler.checkExists(triggerKey)) {
			throw new Exception("修改SchedulerRecord[" + uuid + "]失败,记录未找到!");
		}
		Trigger newTrigger = TriggerBuilder.newTrigger().withIdentity(String.valueOf(uuid)).startNow()
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
				recordCache.put(record.getUuid(), record);
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
			if (!recordCache.containsKey(record.getUuid())) {
				record = nextRecord();
				return record;
			} else {
				deleteRecord(record.getUuid());
				return record;
			}
		}
		return record;
	}

	/**
	 * 删除
	 * 
	 * @param jobID
	 */
	public void deleteRecord(String uuid) {
		recordCache.remove(uuid);
	}

	/**
	 * 获取Record
	 * 
	 * @param id
	 * @return
	 */
	public KettleRecord getRecord(String uuid) {
		return recordCache.get(uuid);
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
		return recordCache.containsKey(record.getUuid());
	}
}
