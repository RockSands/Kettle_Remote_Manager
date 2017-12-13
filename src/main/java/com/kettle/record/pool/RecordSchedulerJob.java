package com.kettle.record.pool;

import org.pentaho.di.core.exception.KettleException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.record.KettleRecord;

/**
 * 定时任务
 * 
 * @author Administrator
 *
 */
public class RecordSchedulerJob implements Job {
	/**
	 * 日志
	 */
	Logger logger = LoggerFactory.getLogger(RecordSchedulerJob.class);

	/**
	 * 数据库
	 */
	private final KettleDBClient dbClient;

	public RecordSchedulerJob() {
		dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
	}

	@Override
	public synchronized void execute(JobExecutionContext context) throws JobExecutionException {
		KettleRecord record = (KettleRecord) context.getJobDetail().getJobDataMap().get("RECORD");
		KettleRecord recordTmp = null;
		try {
			recordTmp = dbClient.queryRecord(record.getUuid());
		} catch (KettleException e) {
			return;
		}
		if (recordTmp == null || record == null) {// 任务不存在了直接终止任务
			JobExecutionException exception = new JobExecutionException();
			exception.setUnscheduleAllTriggers(true);
			throw exception;
		}
		record.setStatus(recordTmp.getStatus());
		if (record.isRegiste() || record.isFinished() || record.isError()) {
			record.setHostname(null);
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			try {
				dbClient.updateRecord(record);
			} catch (KettleException e) {
				logger.error("Kettle的SchedulerRecord[" + record.getUuid() + "]更新任务状态!", e);
				return;
			}
			KettleRecordPool pool = (KettleRecordPool) context.getJobDetail().getJobDataMap().get("RECORDPOOL");
			pool.addPrioritizeRecord(record);
			logger.debug("Kettle向任务队列添加SchedulerRecord[" + record.getUuid() + "],任务池任务" + pool.size());
		} else {
			logger.debug(
					"Kettle向任务队列添加SchedulerRecord[" + record.getUuid() + "]由于状态为:" + record.getStatus() + "而无法添加!");
		}
	}
}
