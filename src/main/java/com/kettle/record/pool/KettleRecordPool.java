package com.kettle.record.pool;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.pentaho.di.core.exception.KettleException;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrEnvironment;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;

/**
 * Kettle任务池
 * 
 * @author chenkw
 *
 */
public class KettleRecordPool {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleRecordPool.class);

	/**
	 * Repeat任务调度工厂
	 */
	private static Scheduler scheduler = null;

	/**
	 * 存储Record的Map
	 */
	private Map<String, KettleRecord> recordCache = new HashMap<String, KettleRecord>();

	/**
	 * 记录队列
	 */
	private final Queue<String> recordQueue = new LinkedBlockingQueue<String>();

	/**
	 * 优先记录队列
	 */
	private final Queue<String> recordPrioritizeQueue = new LinkedBlockingQueue<String>();

	/**
	 * 监听者
	 */
	private List<KettleRecordPoolMonitor> poolMonitors = new LinkedList<KettleRecordPoolMonitor>();

	/**
	 * 数据库
	 */
	private final KettleDBClient dbClient;

	/**
	 * 
	 * @throws Exception
	 */
	public KettleRecordPool() throws Exception {
		dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
		SchedulerFactory schedulerfactory = new StdSchedulerFactory();
		scheduler = schedulerfactory.getScheduler();
		scheduler.start();
		addAllDBSchedulerRecord();
	}

	/**
	 * 加载数据库定义Record
	 * 
	 * @return
	 */
	private void addAllDBSchedulerRecord() {
		try {
			List<KettleRecord> records = dbClient.allSchedulerRecord();
			for (KettleRecord record : records) {
				addSchedulerRecord(record);
			}
		} catch (Exception ex) {
			logger.error("加载数据库调度Record发生异常!", ex);
		}
	}

	/**
	 * @param poolMonitor
	 */
	public void registePoolMonitor(KettleRecordPoolMonitor poolMonitor) {
		poolMonitors.add(poolMonitor);
	}

	/**
	 * @param poolMonitor
	 */
	public void registePoolMonitor(Collection<KettleRecordPoolMonitor> poolMonitors) {
		poolMonitors.addAll(poolMonitors);
	}

	/**
	 * @param record
	 * 
	 */
	private void notifyPoolMonitors() {
		for (KettleRecordPoolMonitor poolMonitor : poolMonitors) {
			poolMonitor.addRecordNotify();
		}
	}

	/**
	 * 添加的转换任务,该任务仅执行一次
	 * 
	 * @param record
	 * @return 是否添加成功
	 * @throws KettleException
	 */
	public synchronized boolean addRecord(KettleRecord record) throws KettleException {
		if (record != null && !recordCache.containsKey(record.getUuid())) {
			if (!isAcceptedRecord(record)) {
				check();
				recordCache.put(record.getUuid(), record);
				if (recordQueue.offer(record.getUuid())) {
					notifyPoolMonitors();
					return true;
				} else {
					recordCache.remove(record.getUuid());
					return false;
				}
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
		if (record != null && !recordCache.containsKey(record.getUuid())) {
			recordCache.put(record.getUuid(), record);
			boolean result = recordPrioritizeQueue.offer(record.getUuid());
			if (result) {
				notifyPoolMonitors();
			}
			return result;
		}
		return false;
	}

	/**
	 * 添加定时任务
	 * 
	 * @param record
	 * @throws SchedulerException
	 */
	private void addSchedulerRecord(KettleRecord record) throws Exception {
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
	public void addOrModifySchedulerRecord(KettleRecord record) throws KettleException {
		if (record == null || record.getCronExpression() == null) {
			throw new KettleException("添加SchedulerRecord[" + record.getName() + "]失败,未找到CRON表达式!");
		}
		TriggerKey triggerKey = new TriggerKey(record.getUuid());
		Trigger trigger = TriggerBuilder.newTrigger().withIdentity(record.getUuid()).startNow()
				.withSchedule(CronScheduleBuilder.cronSchedule(record.getCronExpression())).build();
		try {
			if (scheduler.checkExists(triggerKey)) {
				scheduler.rescheduleJob(triggerKey, trigger);
			} else {
				JobDataMap newJobDataMap = new JobDataMap();
				newJobDataMap.put("RECORD", record);
				newJobDataMap.put("RECORDPOOL", this);
				JobDetail jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).withIdentity(record.getUuid())
						.setJobData(newJobDataMap).build();
				scheduler.scheduleJob(jobDetail, trigger);
			}
		} catch (Exception ex) {
			throw new KettleException("添加SchedulerRecord[" + record.getName() + "]失败!", ex);
		}
	}

	/**
	 * 删除
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public synchronized boolean deleteRecord(String uuid) throws KettleException {
		recordPrioritizeQueue.remove(uuid);
		recordQueue.remove(uuid);
		return recordCache.remove(uuid) != null;
	}

	/**
	 * 移除定时任务
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public synchronized void removeSchedulerRecord(String uuid) throws KettleException {
		try {

			TriggerKey triggerKey = new TriggerKey(uuid);
			JobKey jobKey = new JobKey(uuid, null);
			if (scheduler.checkExists(triggerKey)) {
				scheduler.pauseTrigger(triggerKey);// 停止触发器
				scheduler.unscheduleJob(triggerKey);// 移除触发器
				scheduler.deleteJob(jobKey);// 删除任务
			}
			deleteRecord(uuid);
		} catch (Exception ex) {
			logger.error("RecordPool停止Record[" + uuid + "]的触发器失败!", ex);
		}
	}

	/**
	 * 获取下一个,并在Pool中删除
	 * 
	 * @return
	 */
	public synchronized KettleRecord nextRecord() {
		KettleRecord record = null;
		String recordUUID = null;
		if (!recordPrioritizeQueue.isEmpty()) {
			recordUUID = recordPrioritizeQueue.poll();
		}
		if (recordUUID == null && !recordQueue.isEmpty()) {
			recordUUID = recordQueue.poll();
		}
		if (recordUUID != null) {
			record = recordCache.remove(recordUUID);
		}
		return record;
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
		if (KettleMgrEnvironment.KETTLE_RECORD_POOL_MAX != null
				&& size() > KettleMgrEnvironment.KETTLE_RECORD_POOL_MAX) {
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
