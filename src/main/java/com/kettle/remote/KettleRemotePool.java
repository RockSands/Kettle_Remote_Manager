package com.kettle.remote;

import java.text.ParseException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobResult;
import com.kettle.core.bean.KettleTransResult;
import com.kettle.core.repo.KettleDBRepositoryClient;
import com.kettle.record.KettleJobRecord;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleRecordPool;
import com.kettle.record.KettleTransRecord;

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
	 * 资源链接
	 */
	private final KettleDBRepositoryClient dbRepositoryClient;

	/**
	 * 任务池
	 */
	private final KettleRecordPool kettleRecordPool;

	/**
	 * 设备名称
	 */
	private final List<String> hostNames = new LinkedList<String>();

	/**
	 * @param dbRepositoryClient
	 * @param includeServers
	 * @param excludeServers
	 * @throws Exception
	 */
	public KettleRemotePool(final KettleDBRepositoryClient dbRepositoryClient, List<String> includeHostNames,
			List<String> excludeHostNames) throws Exception {
		this.remoteclients = new ConcurrentHashMap<String, KettleRemoteClient>();
		this.dbRepositoryClient = dbRepositoryClient;
		this.kettleRecordPool = new KettleRecordPool();
		int i = 1;
		for (SlaveServer server : dbRepositoryClient.getRepository().getSlaveServers()) {
			if (excludeHostNames != null && excludeHostNames.contains(server.getHostname())) {
				continue;
			}
			if (includeHostNames != null && !includeHostNames.contains(server.getHostname())) {
				continue;
			}
			server.getLogChannel().setLogLevel(LogLevel.ERROR);
			addRemoteClient(new KettleRemoteClient(this, server, 3 * i));
			hostNames.add(server.getHostname());
			i++;
		}
		logger.info("Kettle远程池已经加载Client" + remoteclients.keySet());
		List<KettleJobRecord> jobs = dbRepositoryClient.allHandleJobRecord();
		List<KettleTransRecord> trans = dbRepositoryClient.allHandleTransRecord();
		for (KettleJobRecord job : jobs) {
			kettleRecordPool.addPrioritizeRecord(job);
		}
		for (KettleTransRecord tran : trans) {
			kettleRecordPool.addPrioritizeRecord(tran);
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
		if (kettleRecordPool.size() > 50) {
			throw new KettleException("Kettle的等待任务数量已满,无法接受任务!");
		}
	}

	/**
	 * 申请并执行转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleTransResult applyTransMeta(TransMeta transMeta) throws KettleException {
		checkRemotePoolStatus();
		KettleTransRecord record = null;
		try {
			dbRepositoryClient.saveTransMeta(transMeta);
			record = new KettleTransRecord(transMeta);
			record.setId(Long.valueOf(transMeta.getObjectId().getId()));
			record.setName(transMeta.getName());
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			record.setUuid(UUID.randomUUID().toString().replace("-", ""));
			dbRepositoryClient.insertTransRecord(record);
			kettleRecordPool.addRecord(record);
			KettleTransResult result = new KettleTransResult();
			result.setStatus(record.getStatus());
			result.setTransID(record.getId());
			return result;
		} catch (KettleException e) {
			logger.error("Trans[" + transMeta.getName() + "]执行Apply发生异常!", e);
			DeleteTransMetaForce(transMeta);
			throw new KettleException("Trans[" + transMeta.getName() + "]执行Apply发生异常!");
		}
	}

	/**
	 * 申请定时转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 * @throws ParseException
	 */
	public KettleTransResult applyScheduleTransMeta(TransMeta transMeta, String cronExpression) throws Exception {
		checkRemotePoolStatus();
		KettleTransRecord record = null;
		try {
			dbRepositoryClient.saveTransMeta(transMeta);
			record = new KettleTransRecord(transMeta);
			record.setId(Long.valueOf(transMeta.getObjectId().getId()));
			record.setName(transMeta.getName());
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			record.setCronExpression(cronExpression);
			record.setUuid(UUID.randomUUID().toString().replace("-", ""));
			dbRepositoryClient.insertTransRecord(record);
			kettleRecordPool.addSchedulerRecord(record);
			KettleTransResult result = new KettleTransResult();
			result.setStatus(record.getStatus());
			result.setTransID(record.getId());
			return result;
		} catch (Exception e) {
			logger.error("Trans[" + transMeta.getName() + "]执行Apply操作发生异常!", e);
			DeleteTransMetaForce(transMeta);
			throw new KettleException("Trans[" + transMeta.getName() + "]执行Apply操作发生异常!");
		}
	}

	/**
	 * 更新任务Cron
	 * 
	 * @throws Exception
	 */
	public void modifyRecordSchedule(String uuid, String newCron) throws Exception {
		KettleRecord record = dbRepositoryClient.queryRecord(uuid);
		if (record == null) {
			throw new KettleException("Kettle不存在UUID为[" + uuid + "]的记录!");
		}
		kettleRecordPool.modifySchedulerRecord(uuid, newCron);
		record.setCronExpression(newCron);

	}

	/**
	 * 接受并执行作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleJobResult applyJobMeta(JobMeta jobMeta) throws KettleException {
		checkRemotePoolStatus();
		JobEntryCopy jec = jobMeta.getStart();
		if (jec == null) {
			throw new KettleException("JobMeta[" + jobMeta.getName() + "]没有定义Start,无法受理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (jobStart.isRepeat() || jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("Kettle远程池仅受理即时任务!");
		}
		KettleJobRecord record = null;
		try {
			dbRepositoryClient.saveJobMeta(jobMeta);
			record = new KettleJobRecord(jobMeta);
			record.setId(Long.valueOf(jobMeta.getObjectId().getId()));
			record.setUuid(UUID.randomUUID().toString().replace("-", ""));
			record.setName(jobMeta.getName());
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			dbRepositoryClient.insertJobRecord(record);
			kettleRecordPool.addRecord(record);
		} catch (Exception ex) {
			logger.error("Job[" + jobMeta.getName() + "]执行Apply操作发生异常!", ex);
			DeleteJobMetaForce(jobMeta);
			throw new KettleException("Job[" + jobMeta.getName() + "]执行Apply操作发生异常!");
		}
		KettleJobResult result = new KettleJobResult();
		result.setStatus(record.getStatus());
		result.setJobID(record.getId());
		return result;
	}

	/**
	 * 接受并执行作业
	 * 
	 * @param transMeta
	 * @return
	 * @throws Exception
	 */
	public KettleJobResult applyScheduleJobMeta(JobMeta jobMeta, String cronExpression) throws KettleException {
		checkRemotePoolStatus();
		JobEntryCopy jec = jobMeta.getStart();
		if (jec == null) {
			throw new KettleException("JobMeta[" + jobMeta.getName() + "]没有定义Start,无法受理!");
		}
		JobEntrySpecial jobStart = (JobEntrySpecial) jec.getEntry();
		if (jobStart.isRepeat() || jobStart.getSchedulerType() != JobEntrySpecial.NOSCHEDULING) {
			throw new KettleException("Kettle远程池仅受理即时任务!");
		}
		KettleJobRecord record = null;
		try {
			dbRepositoryClient.saveJobMeta(jobMeta);
			record = new KettleJobRecord(jobMeta);
			record.setId(Long.valueOf(jobMeta.getObjectId().getId()));
			record.setUuid(UUID.randomUUID().toString().replace("-", ""));
			record.setName(jobMeta.getName());
			record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
			record.setCronExpression(cronExpression);
			dbRepositoryClient.insertJobRecord(record);
			kettleRecordPool.addSchedulerRecord(record);
		} catch (Exception ex) {
			logger.error("Job[" + jobMeta.getName() + "]执行ApplySchedule操作发生异常!", ex);
			DeleteJobMetaForce(jobMeta);
			throw new KettleException("Job[" + jobMeta.getName() + "]执行ApplySchedule操作发生异常!");
		}
		KettleJobResult result = new KettleJobResult();
		result.setStatus(record.getStatus());
		result.setJobID(record.getId());
		return result;
	}

	/**
	 * 强制删除TransMeta
	 * 
	 * @param transMeta
	 */
	private void DeleteTransMetaForce(TransMeta transMeta) {
		if (transMeta != null) {
			try {
				dbRepositoryClient.deleteTransMeta(Long.valueOf(transMeta.getObjectId().getId()));
			} catch (Exception ex) {
				logger.error("Trans[" + transMeta.getName() + "]持久化发生异常,无法被删除!");
			}
		}
	}

	/**
	 * 强制删除JobMeta
	 * 
	 * @param jobMeta
	 */
	private void DeleteJobMetaForce(JobMeta jobMeta) {
		if (jobMeta != null && jobMeta.getObjectId() != null) {
			try {
				dbRepositoryClient.deleteJobMeta(Long.valueOf(jobMeta.getObjectId().getId()));
			} catch (Exception ex) {
				logger.error("Job[" + jobMeta.getName() + "]持久化发生异常,无法被删除!");
			}
		}
	}

	/**
	 * 执行转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleTransResult excuteTransMeta(long transID) throws KettleException {
		KettleTransRecord record = dbRepositoryClient.queryTransRecord(transID);
		if (record == null || record.getKettleMeta() == null) {
			throw new KettleException("Trans[" + transID + "]未为执行Apply操作!");
		}
		if (record.isApply() || record.isRunning()) {
			throw new KettleException("Trans[" + transID + "]仍在执行,并未完成,无法重新执行!");
		}
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setErrMsg(null);
		record.setRunID(null);
		kettleRecordPool.addRecord(record);
		KettleTransResult result = new KettleTransResult();
		result.setStatus(record.getStatus());
		result.setTransID(record.getId());
		return result;
	}

	/**
	 * 执行转换
	 * 
	 * @param transMeta
	 * @return
	 * @throws KettleException
	 */
	public KettleJobResult excuteJobMeta(long jobID) throws KettleException {
		KettleJobRecord record = dbRepositoryClient.queryJobRecord(jobID);
		if (record == null || record.getKettleMeta() == null) {
			throw new KettleException("Job[" + jobID + "]未执行Apply操作!");
		}
		if (record.isApply() || record.isRunning()) {
			throw new KettleException("Job[" + jobID + "]仍在执行,并未完成,无法重新执行!");
		}
		record.setStatus(KettleVariables.RECORD_STATUS_APPLY);
		record.setErrMsg(null);
		record.setRunID(null);
		kettleRecordPool.addRecord(record);
		KettleJobResult result = new KettleJobResult();
		result.setStatus(record.getStatus());
		result.setJobID(record.getId());
		return result;
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
	 * @return
	 */
	public KettleDBRepositoryClient getDbRepositoryClient() {
		return dbRepositoryClient;
	}

	/**
	 * @return
	 */
	public KettleDatabaseRepository getDbRepository() {
		return dbRepositoryClient.getRepository();
	}

	/**
	 * 获取所有Client
	 * 
	 * @return
	 */
	public Collection<KettleRemoteClient> getAllRemoteclients() {
		return remoteclients.values();
	}
}
