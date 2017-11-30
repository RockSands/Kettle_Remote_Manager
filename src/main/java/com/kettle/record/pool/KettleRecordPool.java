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
		if (record != null) {
			if (!isAcceptedRecord(record)) {
				check();
				record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
				dbClient.updateRecord(record);
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
		if (record != null) {
			if (record.getCronExpression() != null && !recordPrioritizeQueue.contains(record.getUuid())) {// 定时任务
				boolean result = recordPrioritizeQueue.offer(record.getUuid());
				if (result) {
					notifyPoolMonitors();
					record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
					dbClient.updateRecordNE(record);
				}
				return result;
			} else if (!isAcceptedRecord(record)) {// 一般任务
				recordCache.put(record.getUuid(), record);
				boolean result = recordPrioritizeQueue.offer(record.getUuid());
				if (result) {
					notifyPoolMonitors();
					record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
					dbClient.updateRecordNE(record);
				}
				return result;
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
			// 如果Cache不存在,则直接下一个,该Record已经删除或处理完成
			if (!recordCache.containsKey(recordUUID)) {
				deleteRecord(recordUUID);
				record = nextRecord();
			} else {
				record = recordCache.get(recordUUID);
				deleteRecord(recordUUID);
			}
		}
		return record;
	}

	/**
	 * 删除,执行删除,从缓存中删除
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
