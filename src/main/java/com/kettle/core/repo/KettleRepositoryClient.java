
package com.kettle.core.repo;

import java.util.Calendar;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.trans.TransMeta;

/**
 * 数据库工具
 * 
 * @author Administrator
 *
 */
public class KettleRepositoryClient {
	/**
	 * 资源库
	 */
	private final Repository repository;

	/**
	 * 资源路径
	 */
	private RepositoryDirectoryInterface baseDirectory = null;

	public KettleRepositoryClient(Repository repository) throws KettleException {
		this.repository = repository;
		baseDirectory = repository.findDirectory("/");
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
	public synchronized void reconnect() {
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
	 * 从资源库获取TransMeta
	 *
	 * @param name
	 * @return
	 * @throws KettleException
	 */
	public synchronized TransMeta getTransMeta(String transID) throws KettleException {
		connect();
		TransMeta transMeta = repository.loadTransformation(new StringObjectId(transID), null);
		return transMeta;
	}

	/**
	 * 从资源库获取JobMeta
	 *
	 * @param jobID
	 * @return
	 * @throws KettleException
	 */
	public synchronized JobMeta getJobMeta(String jobId) throws KettleException {
		connect();
		JobMeta jobMeta = repository.loadJob(new StringObjectId(jobId), null);
		return jobMeta;
	}

	/**
	 * 向资源库保存TransMeta
	 *
	 * @param transMeta
	 * @param repositoryDirectory
	 * @throws KettleException
	 */
	public synchronized void saveTransMeta(TransMeta transMeta) throws KettleException {
		if (!repository.isConnected()) {
			connect();
		}
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
	public synchronized void saveJobMeta(JobMeta jobMeta) throws KettleException {
		connect();
		repository.save(jobMeta, "1", Calendar.getInstance(), null, true);
	}

	/**
	 * 资源库删除TransMeta
	 *
	 * @param transMeta
	 * @throws KettleException
	 */
	public synchronized void deleteTransMeta(String transID) throws KettleException {
		connect();
		repository.deleteTransformation(new StringObjectId(transID));
	}

	/**
	 * 资源库删除JobMeta
	 * 
	 * @param jobID
	 * @throws KettleException
	 */
	public synchronized void deleteJobMeta(String jobID) throws KettleException {
		connect();
		repository.deleteJob(new StringObjectId(jobID));
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
	 * 获取基础路径
	 * 
	 * @return
	 */
	public RepositoryDirectoryInterface getBaseDirectory() {
		return baseDirectory;
	}

	/**
	 * 获取基础路径
	 * 
	 * @return
	 * @throws KettleException
	 */
	public synchronized RepositoryDirectoryInterface createDirectory(String patch) throws KettleException {
		this.connect();
		RepositoryDirectoryInterface rei = repository.findDirectory("/" + patch);
		if (rei == null) {
			rei = repository.createRepositoryDirectory(this.baseDirectory, patch);
		}
		return rei;
	}
}
