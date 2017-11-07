package com.kettle.remote;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordDepend;
import com.kettle.record.KettleRecordPool;

/**
 * 保存远程运行池
 * 
 * @author Administrator
 *
 */
public class KettleRemotePool {

	/**
	 * 日志
	 */
	Logger logger = LoggerFactory.getLogger(KettleRemoteClient.class);

	/**
	 * 远程连接
	 */
	private final ConcurrentMap<String, KettleRemoteClient> remoteclients;

	/**
	 * 数据库连接
	 */
	private final KettleDBClient dbClient;

	/**
	 * 资源链接
	 */
	private final KettleRepositoryClient repositoryClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool kettleRecordPool;

	/**
	 * 设备名称
	 */
	private final List<String> hostNames = new LinkedList<String>();

	/**
	 * 资源路径
	 */
	private RepositoryDirectoryInterface repositoryDirectory;

	public KettleRemotePool(KettleRepositoryClient repositoryClient, KettleDBClient dbClient) throws Exception {
		this.remoteclients = new ConcurrentHashMap<String, KettleRemoteClient>();
		this.repositoryClient = repositoryClient;
		this.dbClient = dbClient;
		this.kettleRecordPool = new KettleRecordPool();
		int i = 1;
		for (SlaveServer server : repositoryClient.getRepository().getSlaveServers()) {
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			addRemoteClient(new KettleRemoteClient(this, server, 3 * i));
			hostNames.add(server.getHostname());
			i++;
		}
		logger.info("Kettle远程池已经加载Client" + remoteclients.keySet());
		List<KettleRecord> records = new LinkedList<KettleRecord>();
		records.addAll(dbClient.allHandleRecord());
		for (KettleRecord record : records) {
			if (record.isRunning()) {
				kettleRecordPool.addPrioritizeRecord(record);
				continue;
			}
			if (record.getCronExpression() != null) {
				kettleRecordPool.addRecord(record);
			} else {
				kettleRecordPool.addSchedulerRecord(record);
			}
		}
	}

	/**
	 * 验证池子是否可用
	 * 
	 * @return
	 * @throws KettleException
	 */
	public void checkRemotePoolStatus() throws KettleException {
		boolean remoteClientsStatus = false;
		for (KettleRemoteClient client : remoteclients.values()) {
			remoteClientsStatus = client.isRunning();
			if (remoteClientsStatus) {
				break;
			}
		}
		if (!remoteClientsStatus) {
			throw new KettleException("没有可用的远程Client,无法接受任务!");
		}
	}

	/**
	 * 持久化元数据
	 * 
	 * @param dependentTrans
	 * @param dependentJobs
	 * @param mainJob
	 * @throws KettleException
	 */
	private void saveMetas(List<TransMeta> dependentTrans, List<JobMeta> dependentJobs, JobMeta mainJob)
			throws KettleException {
		try {
			for (TransMeta tran : dependentTrans) {
				repositoryClient.saveTransMeta(tran);
			}
			for (JobMeta job : dependentJobs) {
				repositoryClient.saveJobMeta(job);
			}
			dbClient.saveDependentsRelation(dependentTrans, dependentJobs, mainJob);
		} catch (Exception ex) {
			logger.error("Job[" + mainJob.getName() + "]执行保存元数据时发生异常!", ex);
			throw new KettleException("Job[" + mainJob.getName() + "]执行保存元数据时发生异常!");
		}
	}

	/**
	 * 注册作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleRecord registeJobMeta(List<TransMeta> dependentTrans, List<JobMeta> dependentJobs, JobMeta mainJob)
			throws KettleException {

		JobEntryCopy jec = mainJob.getStart();
		if (jec == null) {
			throw new KettleException("JobMeta的核心Job[" + mainJob.getName() + "]没有定义Start,无法受理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (dependentJobs != null && !dependentJobs.isEmpty()) {
			for (JobMeta meta : dependentJobs) {
				if (meta.getStart() != null) {
					throw new KettleException(
							"JobMeta[" + mainJob.getName() + "]的依赖Job[" + meta.getName() + "]存在Start!");
				}
			}
		}
		if (jobStart.isRepeat() || jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("JobMeta的核心Job[" + mainJob.getName() + "]必须是即时任务!");
		}
		saveMetas(dependentTrans, dependentJobs, mainJob);
		KettleRecord record = null;
		record = new KettleRecord();
		record.setKettleMeta(mainJob);
		record.setId(Long.valueOf(mainJob.getObjectId().getId()));
		record.setName(mainJob.getName());
		record.setStatus(KettleVariables.RECORD_STATUS_REGISTE);
		try {
			dbClient.insertRecord(record);
		} catch (Exception ex) {
			logger.error("Job[" + mainJob.getName() + "]执行注册操作发生异常!", ex);
			deleteJobAndDependentsForce(Long.valueOf(mainJob.getObjectId().getId()));
			throw new KettleException("Job[" + mainJob.getName() + "]执行注册操作发生异常!");
		}
		return record;
	}

	/**
	 * @param mainJobID
	 */
	private void deleteJobAndDependentsForce(long mainJobID) {
		try {
			List<KettleRecordDepend> depends = dbClient.queryDependents(mainJobID);
			for (KettleRecordDepend depend : depends) {
				if (KettleVariables.RECORD_TYPE_JOB.equals(depend.getType())) {
					repositoryClient.deleteJobMeta(depend.getId());
				} else if (KettleVariables.RECORD_TYPE_TRANS.equals(depend.getType())) {
					repositoryClient.deleteTransMeta(depend.getId());
				}
			}
			repositoryClient.deleteJobMeta(mainJobID);
			dbClient.deleteRecord(mainJobID);
		} catch (Exception ex) {
			logger.error("Job[" + mainJobID + "]执行持久化清理操作失败!", ex);
		}
	}

	/**
	 * 注册作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleRecord exuteJobMeta(long jobID) throws KettleException {
		KettleRecord record = dbClient.queryRecord(jobID);
		if (record == null) {
			throw new KettleException("Job[" + jobID + "]未找到,请先注册!");
		}
		if (record.isRunning()) {
			throw new KettleException("Job[" + jobID + "]执行中,无法再次执行!");
		}
		if (record.isApply()) {
			throw new KettleException("Job[" + jobID + "]已经在执行队列中,无法再次执行!");
		}
		if (record.getCronExpression() != null) {
			throw new KettleException("Job[" + jobID + "]为定时任务,无法手动执行!");
		}
		kettleRecordPool.addRecord(record);
		return record;
	}

	/**
	 * 注册作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public void deleteJob(long jobID) throws KettleException {
		kettleRecordPool.deleteRecord(jobID);
		dbClient.deleteRecord(jobID);
	}

	/**
	 * 注册定时任务
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 */
	public KettleRecord applyScheduleJobMeta(List<TransMeta> dependentTrans, List<JobMeta> dependentJobs,
			JobMeta mainJob, String cronExpression) throws KettleException {
		checkRemotePoolStatus();
		saveMetas(dependentTrans, dependentJobs, mainJob);
		KettleRecord record = null;
		record = new KettleRecord();
		record.setKettleMeta(mainJob);
		record.setId(Long.valueOf(mainJob.getObjectId().getId()));
		record.setName(mainJob.getName());
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setCronExpression(cronExpression);
		try {
			dbClient.insertRecord(record);
			kettleRecordPool.addSchedulerRecord(record);
		} catch (Exception ex) {
			logger.error("Job[" + mainJob.getName() + "]注册轮询任务操作时发生异常!", ex);
			throw new KettleException("Job[" + mainJob.getName() + "]注册轮询任务操作时发生异常!");
		}
		return record;

	}

	/**
	 * 更新任务Cron
	 * 
	 * @throws Exception
	 */
	public void modifyRecordSchedule(long id, String newCron) throws Exception {
		KettleRecord record = dbClient.queryRecord(id);
		if (record == null) {
			throw new KettleException("Kettle不存在ID为[" + id + "]的记录!");
		}
		kettleRecordPool.modifySchedulerRecord(id, newCron);
		record.setCronExpression(newCron);

	}

	/**
	 * 添加Kettle远端
	 * 
	 * @param remoteClient
	 */
	private void addRemoteClient(KettleRemoteClient remoteClient) {
		if (remoteclients.containsKey(remoteClient.getHostName())) {
			logger.error("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]失败,该主机已存在!");
		} else {
			remoteclients.put(remoteClient.getHostName(), remoteClient);
			logger.info("Kettle的远程池添加Client[" + remoteClient.getHostName() + "]成功!");
		}
	}

	/**
	 * @return
	 */
	public KettleRecordPool getKettleRecordPool() {
		return kettleRecordPool;
	}

	/**
	 * 获取所有Client
	 * 
	 * @return
	 */
	public Collection<KettleRemoteClient> getAllRemoteclients() {
		return remoteclients.values();
	}

	/**
	 * 获取Record
	 * 
	 * @param id
	 * @return
	 * @throws KettleException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 */
	public KettleRecord getRecord(long id) throws Exception {
		KettleRecord record = dbClient.queryRecord(id);
		if (record == null) {
			return null;
		}
		KettleRecord r_record = new KettleRecord();
		r_record.setCreateTime(record.getCreateTime());
		r_record.setCronExpression(record.getCronExpression());
		r_record.setErrMsg(record.getErrMsg());
		r_record.setHostname(record.getHostname());
		r_record.setId(record.getId());
		r_record.setName(record.getName());
		r_record.setRunID(record.getRunID());
		r_record.setStatus(record.getStatus());
		r_record.setUpdateTime(record.getUpdateTime());
		return r_record;
	}

	/**
	 * 同步当前路径
	 * 
	 * @return
	 * @throws KettleException
	 */
	public RepositoryDirectoryInterface getRepositoryDirectory() throws KettleException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
		String current = df.format(new Date());
		if (repositoryDirectory == null || !repositoryDirectory.getPath().contains(current)) {
			repositoryDirectory = repositoryClient.createDirectory(current);
		}
		return repositoryDirectory;
	}

	/**
	 * @return
	 */
	public KettleDBClient getDbClient() {
		return dbClient;
	}

	/**
	 * @return
	 */
	public KettleRepositoryClient getRepositoryClient() {
		return repositoryClient;
	}

	/**
	 * @return
	 */
	public Repository getRepository() {
		return repositoryClient.getRepository();
	}
}
