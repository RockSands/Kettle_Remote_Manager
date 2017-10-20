package com.kettle.remote.record.distribute;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import com.kettle.core.KettleVariables;
import com.kettle.core.repo.KettleDBRepositoryClient;
import com.kettle.record.KettleJobRecord;
import com.kettle.record.KettleRecord;
import com.kettle.record.KettleTransRecord;
import com.kettle.remote.KettleRemotePool;

public class RecordDistributer {
	/**
	 * Tran/Job 记录
	 */
	private final ConcurrentMap<String, KettleRecord> applyRecords = new ConcurrentHashMap<String, KettleRecord>();

	/**
	 * Tran/Job 记录
	 */
	private final ConcurrentMap<String, RepositoryElementInterface> metas = new ConcurrentHashMap<String, RepositoryElementInterface>();

	/**
	 * Tran/Job 保存记录名称
	 */
	private final BlockingQueue<String> recordsFlags = new LinkedBlockingQueue<String>();

	/**
	 * 空闲的线程
	 */
	private final BlockingQueue<RemoteRecordDaemon> freeDaemons = new LinkedBlockingQueue<RemoteRecordDaemon>();

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(4);

	/**
	 * 远程池
	 */
	private final KettleRemotePool kettleRemotePool;

	private final KettleDBRepositoryClient dbRepositoryClient = null;

	public RecordDistributer(KettleRemotePool kettleRemotePool) {
		this.kettleRemotePool = kettleRemotePool;
	}

	/**
	 * 添加的转换任务
	 * 
	 * @param transMeta
	 * @param record
	 */
	public void addRecords(TransMeta transMeta, KettleTransRecord record) {
		String recordFlag = recordFlag(record);
		metas.put(recordFlag, transMeta);
		applyRecords.put(recordFlag, record);
		recordsFlags.offer(recordFlag);
		remoteSendOnceRecord(null);
	}

	/**
	 * 添加的工作任务
	 * 
	 * @param jobMeta
	 * @param record
	 */
	public void addRecords(JobMeta jobMeta, KettleJobRecord record) {
		String recordFlag = recordFlag(record);
		metas.put(recordFlag, jobMeta);
		applyRecords.put(recordFlag, record);
		recordsFlags.offer(recordFlag);
		remoteSendOnceRecord(null);
	}

	private String recordFlag(KettleRecord kettleRecord) {
		if (KettleJobRecord.class.isInstance(kettleRecord)) {
			return "Job_" + kettleRecord.getId();
		} else if (KettleTransRecord.class.isInstance(kettleRecord)) {
			return "Trans_" + kettleRecord.getId();
		}
		throw new RuntimeException("KettleRecord类型无法识别!");
	}

	/**
	 * 远程记录处理者
	 * 
	 * @author Administrator
	 *
	 */
	public void dealRecordDaemon(RemoteRecordDaemon daemon) {
		if (daemon == null || daemon.getRecord() == null) {
			return;
		}
		if (KettleVariables.RECORD_STATUS_FINISHED.equals(daemon.getRecord().getStatus())) {
			recordIsFinished(daemon);
		} else if (KettleVariables.RECORD_STATUS_RUNNING.equals(daemon.getRecord().getStatus())) {
			recordIsRunning(daemon);
		} else if (KettleVariables.RECORD_STATUS_ERROR.equals(daemon.getRecord().getStatus())) {
			recordIsError(daemon);
		}
	}

	private synchronized RemoteRecordDaemon getOneFreeDaemon() {
		if (!freeDaemons.isEmpty()) {
			try {
				RemoteRecordDaemon daemon = freeDaemons.take();
				return daemon;
			} catch (InterruptedException e) {
				return null;
			}
		} else {
			return null;
		}
	}

	/**
	 * 一次性的使用该方法
	 * 
	 * @param daemon
	 */
	private synchronized void remoteSendOnceRecord(RemoteRecordDaemon daemon) {
		RemoteRecordDaemon selectDaemon = daemon;
		/*
		 * 随机选择一个Daemon
		 */
		if (selectDaemon == null) {
			selectDaemon = getOneFreeDaemon();
		}
		if (selectDaemon == null) {
			return;
		}
		if (daemon.isFree() && !recordsFlags.isEmpty()) {
			try {
				String recordFlag = recordsFlags.take();
				RepositoryElementInterface rei = metas.get(recordFlag);
				String runID = null;
				runID = daemon.getRemote().remoteSendREI(rei);
				KettleRecord record = applyRecords.get(recordFlag);
				record.setRunID(runID);
				record.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
				daemon.setRecord(record);
				if (JobMeta.class.isInstance(rei)) {
					runID = daemon.getRemote().remoteSendJob((JobMeta) rei);
				} else if (Trans.class.isInstance(rei)) {
					runID = daemon.getRemote().remoteSendTrans((TransMeta) rei);
				} else {
					return;
				}
				threadPool.schedule(daemon, 20, TimeUnit.SECONDS);
			} catch (InterruptedException e) {

			} catch (KettleException e) {
			}
		} else {
			freeDaemons.offer(daemon);
		}
	}

	/**
	 * 完成状态
	 * 
	 * @param record
	 */
	public void recordIsFinished(RemoteRecordDaemon daemon) {
		/*
		 * 清理本地资源
		 */
		applyRecords.remove(daemon.getRecord().getName());
		metas.remove(recordFlag(daemon.getRecord()));
		/*
		 * 情况进程的Record
		 */
		try {
			dbRepositoryClient.updateRecord(daemon.getRecord());
		} catch (KettleException e) {

		}
		daemon.setRecord(null);
		remoteSendOnceRecord(daemon);

	}

	public void recordIsError(RemoteRecordDaemon daemon) {
		applyRecords.remove(daemon.getRecord().getName());
		metas.remove(recordFlag(daemon.getRecord()));
		/*
		 * 情况进程的Record
		 */
		daemon.setRecord(null);
		remoteSendOnceRecord(daemon);
	}

	/**
	 * 继续进行
	 * 
	 * @param record
	 */
	public void recordIsRunning(RemoteRecordDaemon daemon) {
		// 进行下次查看
		threadPool.schedule(daemon, 20, TimeUnit.SECONDS);
	}
}
