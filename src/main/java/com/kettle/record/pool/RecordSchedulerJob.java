package com.kettle.record.pool;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		KettleRecord record = (KettleRecord) context.getJobDetail().getJobDataMap().get("RECORD");
		if (record.isRegiste() || record.isApply() || record.isFinished() || record.isError()) {
			record.setHostname(null);
			KettleRecordPool pool = (KettleRecordPool) context.getJobDetail().getJobDataMap().get("RECORDPOOL");
			pool.addPrioritizeRecord(record);
			logger.debug("Kettle向任务队列添加定时任务[" + record.getName() + "],任务池任务个数:" + pool.size());
		} else {
			logger.debug("Kettle向任务队列添加定时任务[" + record.getName() + "]由于状态为:" + record.getStatus() + "而无法添加!");
		}
	}
}
