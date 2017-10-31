package com.kettle.record;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务
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
		KettleRecordPool pool = (KettleRecordPool) context.getJobDetail().getJobDataMap().get("RECORDPOOL");
		pool.addPrioritizeRecord(record);
		logger.debug("Kettle向任务队列添加定时任务[" + record.getName() + "]!");
	}
}
