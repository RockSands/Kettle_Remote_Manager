package com.kettle.remote;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoryElementInterface;
import org.pentaho.di.trans.TransMeta;

import com.kettle.bean.KettleRecord;

public class RecordDistribute {
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
	private final BlockingQueue<String> recordsNames = new LinkedBlockingQueue<String>();

	/**
	 * Tran/Job 运行中记录名称
	 */
	private final BlockingQueue<String> runningRecords = new LinkedBlockingQueue<String>();

	/**
	 * 线程池
	 */
	private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(20);

	/**
	 * 远程池
	 */
	private final KettleRemotePool kettleRemotePool;

	public RecordDistribute(KettleRemotePool kettleRemotePool) {
		this.kettleRemotePool = kettleRemotePool;
	}

	public void addTransRecords(TransMeta transMeta, KettleRecord record) {
		metas.put("Trans_" + transMeta.getName(), transMeta);
		applyRecords.put("Trans_" + transMeta.getName(), record);
		recordsNames.offer("Trans_" + transMeta.getName());
	}

	public void addJobRecords(JobMeta jobMeta, KettleRecord record) {
		metas.put("Job_" + jobMeta.getName(), jobMeta);
		applyRecords.put("Job_" + jobMeta.getName(), record);
		recordsNames.offer("Job_" + jobMeta.getName());
	}

	class Daemon0 implements Runnable {
		@Override
		public void run() {
			try {
				for (KettleRemoteClient remote : kettleRemotePool.getAllRemoteclients()) {

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
