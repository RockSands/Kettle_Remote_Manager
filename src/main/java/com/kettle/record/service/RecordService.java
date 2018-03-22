package com.kettle.record.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.RepositoryDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;
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

    /**
     * 构造器
     * 
     * @throws KettleException
     */
    public RecordService() {
	recordPool = KettleMgrInstance.kettleMgrEnvironment.getRecordPool();
	dbClient = KettleMgrInstance.kettleMgrEnvironment.getDbClient();
	repositoryClient = KettleMgrInstance.kettleMgrEnvironment.getRepositoryClient();
	try {
	    for (KettleRecord record : dbClient.allSchedulerRecord()) {
		recordPool.addOrModifySchedulerRecord(record);
	    }
	} catch (Exception ex) {
	    logger.error("RecordService初始化异常,定时任务加载失败!!!", ex);
	}
    }

    /**
     * 清理空的目录
     * 
     * @throws KettleException
     */
    public void deleteEmptyRepoPath() throws KettleException {
	SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
	repositoryClient
		.deleteEmptyRepoPath(Arrays.asList(RepositoryDirectory.DIRECTORY_SEPARATOR + df.format(new Date()),
			RepositoryDirectory.DIRECTORY_SEPARATOR + "ScheduledJobs"));
    }

    /**
     * 检查KettleJobEntireDefine的定义
     * 
     * @param jobEntire
     * @throws KettleException
     */
    private void checkKettleJobEntireDefine(KettleJobEntireDefine jobEntire) throws KettleException {
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
    }

    /**
     * 将KettleJobEntireDefine保存到Kettle资源库
     * 
     * @param jobEntire
     * @return
     * @throws KettleException
     */
    private KettleRecord savejobEntire2KettleRepo(KettleJobEntireDefine jobEntire) throws KettleException {
	SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
	jobEntire.setUuid(UUID.randomUUID().toString().replace("-", ""));
	KettleRecord record = repositoryClient.saveJobEntireDefine(jobEntire, df.format(new Date()));
	return record;
    }

    /**
     * 立即执行
     * 
     * @param jobEntire
     * @return
     * @throws KettleException
     */
    public KettleRecord excuteJobDirectly(KettleJobEntireDefine jobEntire) throws KettleException {
	checkKettleJobEntireDefine(jobEntire);
	KettleRecord record = savejobEntire2KettleRepo(jobEntire);
	record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
	if (recordPool.addRecord(record)) {
	    try {
		dbClient.insertRecord(record);
	    } catch (KettleException ex) {
		recordPool.deleteRecord(record.getUuid());
		throw new KettleException("Job申请执行失败!", ex);
	    }
	} else {
	    throw new KettleException("Job申请执行失败,任务池已满!");
	}
	return record;
    }

    /**
     * 注册作业
     * 
     * @param jobEntire
     * @return
     * @throws KettleException
     */
    public KettleRecord registeJob(KettleJobEntireDefine jobEntire) throws KettleException {
	checkKettleJobEntireDefine(jobEntire);
	KettleRecord record = savejobEntire2KettleRepo(jobEntire);
	record.setStatus(KettleVariables.RECORD_STATUS_REGISTE);
	try {
	    dbClient.insertRecord(record);
	} catch (Exception ex) {
	    logger.error("Job[" + jobEntire.getMainJob().getName() + "]执行注册操作发生异常!", ex);
	    dbClient.deleteRecordNE(jobEntire.getUuid());
	    throw new KettleException("Job[" + jobEntire.getMainJob().getName() + "]执行注册操作发生异常!");
	}
	return record;
    }

    /**
     * 执行作业
     * 
     * @param uuid
     * @throws KettleException
     */
    public void excuteJob(String uuid) throws KettleException {
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
	if (recordPool.addRecord(record)) {
	    record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
	    dbClient.updateRecordStatus(record);
	} else {
	    throw new KettleException("Job[" + uuid + "]申请执行失败,被任务池已满或任务已经存在!");
	}
    }

    /**
     * 更新任务为Cron
     * 
     * @param uuid
     * @param newCron
     * @throws Exception
     */
    public void makeRecordScheduled(String uuid, String newCron) throws KettleException {
	KettleRecord record = dbClient.queryRecord(uuid);
	dbClient.queryRecordRelations(record);
	if (record == null) {
	    throw new KettleException("Kettle不存在UUID为[" + uuid + "]的记录!");
	}
	if (record.getCronExpression() == null && StringUtils.isEmpty(newCron)) {// 什么也不用做
	    return;
	} else if (StringUtils.isEmpty(newCron)) {// 如果是撤销定时任务,将任务迁移至当前目录
	    recordPool.removeSchedulerRecord(uuid);
	    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
	    List<String> oldMetaIDs = repositoryClient.moveJobEntireDefine(record, df.format(new Date()));
	    dbClient.updateRecordNoStatus(record);
	    dbClient.updateRecordRelationsID(record, oldMetaIDs);
	    return;
	} else if (record.getCronExpression() == null) {// 如果record为非定时任务,将任务迁移并处理定时!
	    List<String> oldMetaIDs = repositoryClient.moveJobEntireDefine(record, "ScheduledJobs");
	    dbClient.updateRecordRelationsID(record, oldMetaIDs);
	}
	record.setCronExpression(newCron);
	recordPool.addOrModifySchedulerRecord(record);
	dbClient.updateRecordNoStatus(record);
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
	if (record == null) {
	    return null;
	}
	return record;
    }

    /**
     * 查询
     * 
     * @param uuids
     * @return
     * @throws KettleException
     */
    public List<KettleRecord> queryJobs(List<String> uuids) throws KettleException {
	List<KettleRecord> records = dbClient.queryRecords(uuids);
	KettleRecord roll = null;
	for (Iterator<KettleRecord> it = records.iterator(); it.hasNext();) {
	    roll = it.next();
	    if (roll == null) {
		it.remove();
	    }
	}
	return records;
    }

    /**
     * 返回所有停止的Job
     * 
     * @return
     * @throws KettleException
     */
    public List<KettleRecord> queryStopedJobs() throws KettleException {
	return dbClient.allStopRecord();
    }

    /**
     * 删除工作
     * 
     * @param uuid
     * @throws KettleException
     */
    public void deleteJob(String uuid) throws KettleException {
	KettleRecord record = dbClient.queryRecord(uuid);
	if (record == null) {
	    return;
	}

	if (record.getCronExpression() != null) {
	    recordPool.removeSchedulerRecord(uuid);
	    record = dbClient.queryRecord(uuid);// 定时任务重新查询,避免状态啊为脏数据
	}
	if (record.isError() || record.isFinished() || record.isRegiste()) {
	    dbClient.queryRecordRelations(record);
	    recordPool.deleteRecord(uuid);
	    dbClient.deleteRecord(uuid);
	    repositoryClient.deleteJobEntireDefine(record);
	    return;
	}
	throw new KettleException("Record[" + uuid + "]已被受理,无法删除!");
    }

    /**
     * 立即删除工作,运行中的即可停止
     * 
     * @param uuid
     * @throws KettleException
     */
    public void deleteJobImmediately(String uuid) throws KettleException {
	KettleRecord record = dbClient.queryRecord(uuid);
	if (record == null) {
	    return;
	}
	if (record.getCronExpression() != null) {
	    recordPool.removeSchedulerRecord(uuid);
	    record = dbClient.queryRecord(uuid);// 定时任务重新查询,避免状态啊为脏数据
	}
	dbClient.queryRecordRelations(record);
	if (record.isError() || record.isFinished() || record.isRegiste()) {
	    recordPool.deleteRecord(uuid);
	    dbClient.deleteRecord(uuid);
	    repositoryClient.deleteJobEntireDefine(record);
	    return;
	}
	jobMustDie(record);
    }

    /**
     * 将Job关闭
     * 
     * @throws KettleException
     */
    protected abstract void jobMustDie(KettleRecord record) throws KettleException;

    /**
     * 获取未处理Record
     * 
     * @return
     */
    protected List<KettleRecord> getHandleRecords() {
	try {
	    return dbClient.allHandleRecord();
	} catch (Exception ex) {
	    logger.error("加载遗留Record发生异常!");
	    return new ArrayList<KettleRecord>(0);
	}
    }

    /**
     * 获取遗留Record
     * 
     * @return
     */
    protected List<KettleRecord> getAllSchedulerRecords() {
	try {
	    return dbClient.allSchedulerRecord();
	} catch (Exception ex) {
	    logger.error("加载遗留Record发生异常!");
	    return new ArrayList<KettleRecord>(0);
	}
    }
}
