
package com.kettle.core.repo;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.record.KettleRecordRelation;

/**
 * Kettle资源库
 * 
 * @author Administrator
 *
 */
public class KettleRepositoryClient {
	/**
	 * 日志
	 */
	private static Logger logger = LoggerFactory.getLogger(KettleRepositoryClient.class);
	/**
	 * 资源库
	 */
	private final Repository repository;

	/**
	 * 资源路径
	 */
	private RepositoryDirectoryInterface directory = null;

	public KettleRepositoryClient(Repository repository) throws KettleException {
		this.repository = repository;
	}

	/**
	 * @return
	 * @throws KettleException
	 */
	private synchronized void syncCurrentDirectory() throws KettleException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
		String current = df.format(new Date());
		if (directory == null || !directory.getPath().contains(current)) {
			connect();
			directory = repository.createRepositoryDirectory(repository.findDirectory(""), current);
		}
	}

	public synchronized void connect() {
		if (!repository.isConnected()) {
			try {
				repository.connect(EnvUtil.getSystemProperty("KETTLE_REPOSITORY_USER"),
						EnvUtil.getSystemProperty("KETTLE_REPOSITORY_PASSWD"));
			} catch (KettleException e) {
				throw new RuntimeException("Kettle的资源池无法连接!");
			}
		}
	}

	/**
	 * 关闭连接
	 */
	public synchronized void disconnect() {
		if (repository.isConnected()) {
			repository.disconnect();
		}
	}

	/**
	 * 重新连接
	 */
	@SuppressWarnings("unused")
	private synchronized void reconnect() {
		try {
			if (repository.isConnected()) {
				repository.disconnect();
			}
			repository.connect("admin", "admin");
		} catch (KettleException e) {
			throw new RuntimeException("Kettle的资源池无法连接!");
		}
	}

	/**
	 * @return
	 * @throws KettleException
	 */
	public RepositoryDirectoryInterface getDirectory() throws KettleException {
		syncCurrentDirectory();
		return directory;
	}

	/**
	 * @param id
	 * @return
	 */
	private ObjectId toObjectID(String id) {
		if (KettleFileRepository.class.isInstance(repository)) {// 文件
			return new StringObjectId(id);
		} else { // 数据库
			return new LongObjectId(Long.valueOf(id));
		}
	}

	/**
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @param repositoryDirectory
	 * @throws KettleException
	 */
	private synchronized void saveTransMeta(TransMeta transMeta) throws KettleException {
		repository.save(transMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 向资源库保存TransMeta
	 * 
	 * @param repositoryDirectory
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	private synchronized void saveJobMeta(JobMeta jobMeta) throws KettleException {
		repository.save(jobMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 从资源库获取TransMeta
	 *
	 * @param name
	 * @return
	 * @throws KettleException
	 */
	public TransMeta getTransMeta(String transID) throws KettleException {
		connect();
		TransMeta transMeta = repository.loadTransformation(toObjectID(transID), null);
		return transMeta;
	}

	/**
	 * 从资源库获取JobMeta
	 *
	 * @param jobID
	 * @return
	 * @throws KettleException
	 */
	public JobMeta getJobMeta(String jobId) throws KettleException {
		connect();
		JobMeta jobMeta = repository.loadJob(toObjectID(jobId), null);
		return jobMeta;
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void deleteTransMeta(String transID) throws KettleException {
		connect();
		repository.deleteTransformation(toObjectID(transID));
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public void deleteTransMetaNE(String transID) {
		try {
			deleteTransMeta(transID);
		} catch (KettleException e) {
			logger.error("资源池删除Trans[" + transID + "]发生异常", e);
		}
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteJobMeta(String jobID) throws KettleException {
		connect();
		repository.deleteJob(toObjectID(jobID));
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public void deleteJobMetaNE(String jobID) {
		try {
			deleteJobMeta(jobID);
		} catch (KettleException e) {
			logger.error("资源池删除JOB[" + jobID + "]发生异常", e);
		}
	}

	/**
	 * 获取资源
	 * 
	 * @return
	 */
	public Repository getRepository() {
		return repository;
	}

	/**
	 * @param jobEntire
	 * @throws KettleException
	 */
	public synchronized void saveJobEntireDefine(KettleJobEntireDefine jobEntire) throws KettleException {
		connect();
		saveJobMeta(jobEntire.getMainJob());
		for (TransMeta transMeta : jobEntire.getDependentTrans()) {
			saveTransMeta(transMeta);
		}
		for (JobMeta jobMeta : jobEntire.getDependentJobs()) {
			saveJobMeta(jobMeta);
		}
	}

	/**
	 * 删除
	 * 
	 * @param depends
	 */
	public synchronized void deleteJobEntireDefineNE(List<KettleRecordRelation> relations) {
		connect();
		for (KettleRecordRelation relation : relations) {
			if (KettleVariables.RECORD_TYPE_TRANS.equals(relation.getType())) {
				deleteTransMetaNE(relation.getMetaid());
			} else if (KettleVariables.RECORD_TYPE_JOB.equals(relation.getType())) {
				deleteJobMetaNE(relation.getMetaid());
			}
		}
	}
}
