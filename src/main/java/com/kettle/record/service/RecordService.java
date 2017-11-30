package com.kettle.record.service;

import java.util.List;
import java.util.UUID;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordRelation;
import com.kettle.record.pool.KettleRecordPool;

/**
 * Record的服务
 * 
 * @author Administrator
 *
 */
public abstract class RecordService {

	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(RecordService.class);

	/**
	 * Kettle资源库
	 */
	protected final KettleRepositoryClient repositoryClient;

	/**
	 * 数据库
	 */
	protected final KettleDBClient dbClient;

	/**
	 * 任务池
	 */
	protected final KettleRecordPool recordPool;

	public RecordService() {
		recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
		dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
		repositoryClient = KettleMgrInstance.kettleMgrEnvironment.getRepositoryClient();
	}

	/**
	 * 注册作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleRecord registeJob(KettleJobEntireDefine jobEntire) throws KettleException {
		JobEntryCopy jec = jobEntire.getMainJob().getStart();
		if (jec == null) {
			throw new KettleException("JobMeta的核心Job[" + jobEntire.getMainJob().getName() + "]没有定义Start,无法受理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (!jobEntire.getDependentJobs().isEmpty()) {
			for (JobMeta meta : jobEntire.getDependentJobs()) {
				if (meta.getStart() != null) {
					throw new KettleException(
							"JobMeta[" + jobEntire.getMainJob().getName() + "]的依赖Job[" + meta.getName() + "]存在Start!");
				}
			}
		}
		if (jobStart.isRepeat() || jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("JobMeta的核心Job[" + jobEntire.getMainJob().getName() + "]必须是即时任务!");
		}
		jobEntire.setUuid(UUID.randomUUID().toString().replace("-", ""));
		repositoryClient.saveJobEntireDefine(jobEntire);
		KettleRecord record = null;
		record = new KettleRecord();
		record.setKettleMeta(jobEntire.getMainJob());
		record.setUuid(jobEntire.getUuid());
		record.setJobid(jobEntire.getMainJob().getObjectId().getId());
		record.setName(jobEntire.getMainJob().getName());
		record.setStatus(KettleVariables.RECORD_STATUS_REGISTE);
		try {
			dbClient.saveDependentsRelation(jobEntire);
			dbClient.insertRecord(record);
		} catch (Exception ex) {
			logger.error("Job[" + jobEntire.getMainJob().getName() + "]执行注册操作发生异常!", ex);
			dbClient.deleteRecordNE(jobEntire.getUuid());
			List<KettleRecordRelation> relations = dbClient.deleteDependentsRelationNE(jobEntire.getUuid());
			repositoryClient.deleteJobEntireDefineNE(relations);
			throw new KettleException("Job[" + jobEntire.getMainJob().getName() + "]执行注册操作发生异常!");
		}
		return record;
	}

	/**
	 * 注册定时任务
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 */
	public KettleRecord applyScheduleJob(KettleJobEntireDefine jobEntire, String cronExpression)
			throws KettleException {
		JobEntryCopy jec = jobEntire.getMainJob().getStart();
		if (jec == null) {
			throw new KettleException("JobMeta的核心Job[" + jobEntire.getMainJob().getName() + "]没有定义Start,无法受理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (!jobEntire.getDependentJobs().isEmpty()) {
			for (JobMeta meta : jobEntire.getDependentJobs()) {
				if (meta.getStart() != null) {
					throw new KettleException(
							"JobMeta[" + jobEntire.getMainJob().getName() + "]的依赖Job[" + meta.getName() + "]存在Start!");
				}
			}
		}
		if (jobStart.isRepeat() || jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("JobMeta的核心Job[" + jobEntire.getMainJob().getName() + "]必须是即时任务!");
		}
		jobEntire.setUuid(UUID.randomUUID().toString().replace("-", ""));
		repositoryClient.saveJobEntireDefine(jobEntire);
		KettleRecord record = null;
		record = new KettleRecord();
		record.setKettleMeta(jobEntire.getMainJob());
		record.setUuid(jobEntire.getUuid());
		record.setJobid(jobEntire.getMainJob().getObjectId().getId());
		record.setName(jobEntire.getMainJob().getName());
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setCronExpression(cronExpression);
		try {
			dbClient.saveDependentsRelation(jobEntire);
			dbClient.insertRecord(record);
			recordPool.addSchedulerRecord(record);
		} catch (Exception ex) {
			logger.error("Job[" + jobEntire.getMainJob().getName() + "]注册轮询任务操作时发生异常!", ex);
			dbClient.deleteRecordNE(jobEntire.getUuid());
			List<KettleRecordRelation> relations = dbClient.deleteDependentsRelationNE(jobEntire.getUuid());
			repositoryClient.deleteJobEntireDefineNE(relations);
			throw new KettleException("Job[" + jobEntire.getMainJob().getName() + "]注册轮询任务操作时发生异常!");
		}
		return record;
	}

	/**
	 * 执行作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleRecord excuteJob(String uuid) throws KettleException {
		KettleRecord record = dbClient.queryRecord(uuid);
		if (record == null) {
			throw new KettleException("Job[" + uuid + "]未找到,请先注册!");
		}
		if (record.isRunning()) {
			throw new KettleException("Job[" + uuid + "]执行中,无法再次执行!");
		}
		if (record.isApply()) {
			throw new KettleException("Job[" + uuid + "]已经在执行队列中,无法再次执行!");
		}
		if (record.getCronExpression() != null) {
			throw new KettleException("Job[" + uuid + "]为定时任务,无法手动执行!");
		}
		record.setKettleMeta(repositoryClient.getJobMeta(record.getJobid()));
		if (record.getKettleMeta() == null) {
			throw new KettleException("Job[" + uuid + "]数据异常,未找到Kettle元数据!");
		}
		if (recordPool.addRecord(record)) {
			return record;
		}
		throw new KettleException("Job[" + uuid + "]申请执行失败,被任务池拒绝加!");
	}

	/**
	 * 更新任务Cron
	 * 
	 * @throws Exception
	 */
	public void modifyRecordSchedule(String uuid, String newCron) throws Exception {
		KettleRecord record = dbClient.queryRecord(uuid);
		if (record == null) {
			throw new KettleException("Kettle不存在UUID为[" + uuid + "]的记录!");
		}
		recordPool.modifySchedulerRecord(uuid, newCron);
		record.setCronExpression(newCron);
	}

	/**
	 * 查询Job
	 * 
	 * @param uuid
	 * @return
	 * @throws KettleException
	 */
	public KettleRecord queryJob(String uuid) throws KettleException {
		KettleRecord record = dbClient.queryRecord(uuid);
		return record;
	}

	/**
	 * 删除工作
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public void deleteJob(String uuid) throws KettleException {
		KettleRecord record = dbClient.queryRecord(uuid);
		if (record != null) {
			recordPool.deleteRecord(uuid);
		}
		dbClient.deleteRecordNE(uuid);
		List<KettleRecordRelation> relations = dbClient.deleteDependentsRelationNE(uuid);
		repositoryClient.deleteJobEntireDefineNE(relations);
	}

	/**
	 * 加载遗留Record
	 */
	protected void attachOldRecord() {
		try {
			for (KettleRecord record : dbClient.allHandleRecord()) {
				if (record.isApply()) {
					record.setKettleMeta(repositoryClient.getJobMeta(record.getJobid()));
					recordPool.addPrioritizeRecord(record);
				} else {
					record.setStatus(KettleVariables.RECORD_STATUS_ERROR);
					record.setErrMsg("系统重启,已经无法追踪任务状态!");
					this.dbClient.updateRecord(record);
				}
			}
		} catch (Exception ex) {
			logger.error("加载遗留Record发生异常!");
		}
	}
}
