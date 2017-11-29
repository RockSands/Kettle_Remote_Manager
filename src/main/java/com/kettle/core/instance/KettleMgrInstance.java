package com.kettle.core.instance;

import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.bean.KettleResult;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;
import com.kettle.record.pool.KettleRecordPool;
import com.kettle.record.service.RecordService;
import com.kettle.remote.KettleRemotePool;
import com.kettle.remote.record.service.RemoteParallelRecordService;

/**
 * Kettle数据迁移管理者
 * 
 * @author Administrator
 *
 */
public class KettleMgrInstance {
	/**
	 * 日志
	 */
	Logger logger = LoggerFactory.getLogger(KettleMgrInstance.class);

	/**
	 * 实例
	 */
	private static KettleMgrInstance instance = null;

	/**
	 * 远程执行池
	 */
	;
	public static KettleMgrEnvironment kettleMgrEnvironment;

	/**
	 * 主进程
	 */
	private RecordService recordService;

	/**
	 * 定时任务
	 */
	private ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor();

	static {
		getInstance();
	}

	public static KettleMgrInstance getInstance() {
		if (instance == null) {
			instance = new KettleMgrInstance();
		}
		return instance;
	}

	private KettleMgrInstance() {
		init();
		if (KettleMgrEnvironment.KETTLE_RECORD_PERSIST_MAX_HOUR != null
				&& KettleMgrEnvironment.KETTLE_RECORD_PERSIST_MAX_HOUR > 0) {
			Calendar now = Calendar.getInstance();
			now.setTime(new Date());
			int initialDelay = 24 - now.get(Calendar.HOUR_OF_DAY) + 1;
			threadPool.scheduleAtFixedRate(new DelAbandonedRecord(), initialDelay, 24, TimeUnit.HOURS);
		}
	}

	/**
	 * 初始化
	 */
	private void init() {
		try {
			// Kettle本地初始化
			KettleEnvironment.init();
			// 加载本地资源文件
			InputStream is = getClass().getClassLoader().getResourceAsStream("kettl_env.properties");
			Properties properties = new Properties();
			properties.load(is);
			EnvUtil.applyKettleProperties(properties, true);
			// 初始本地环境
			kettleMgrEnvironment = new KettleMgrEnvironment();
			// 定义Kettle资源库0
			KettleFileRepository repository = new KettleFileRepository();
			RepositoryMeta dbrepositoryMeta = new KettleFileRepositoryMeta(
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_ID"),
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_NAME"),
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_DESCRIPTION"),
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_PATH"));
			repository.init(dbrepositoryMeta);
			kettleMgrEnvironment.setRepositoryClient(new KettleRepositoryClient(repository));
			kettleMgrEnvironment.getRepositoryClient().connect();
			// 数据库
			DatabaseMeta databaseMeta = new DatabaseMeta(EnvUtil.getSystemProperty("KETTLE_RECORD_DB_NAME"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_TYPE"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_ACCESS"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_HOST"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_DATABASENAME"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_PORT"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_USER"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_PASSWD"));
			kettleMgrEnvironment.setDbClient(new KettleDBClient(databaseMeta));
			// 任务池
			KettleRecordPool recordPool = new KettleRecordPool();
			kettleMgrEnvironment.setRecordPool(recordPool);
			// 远程池
			KettleRemotePool remotePool = new KettleRemotePool();
			kettleMgrEnvironment.setRemotePool(remotePool);
			// 服务
			recordService = new RemoteParallelRecordService();
		} catch (Exception ex) {
			throw new RuntimeException("KettleMgrInstance初始化失败", ex);
		}
	}

	/**
	 * 注册
	 *
	 * @param jobEntire
	 * @return
	 * @throws KettleException.
	 */
	public KettleResult registeJob(KettleJobEntireDefine jobEntire) throws KettleException {
		logger.info("Kettle注册Job[" + jobEntire.getMainJob().getName() + "]");
		KettleRecord record = recordService.registeJob(jobEntire);
		KettleResult result = new KettleResult();
		result.setUuid(record.getUuid());
		result.setStatus(record.getStatus());
		result.setErrMsg(record.getErrMsg());
		return result;
	}

	/**
	 * 注册定时任务
	 *
	 * @param jobEntire
	 * @param cronExpression
	 * @return
	 * @throws KettleException.
	 */
	public KettleResult applyScheduleJob(KettleJobEntireDefine jobEntire, String cronExpression)
			throws KettleException {
		logger.info("Kettle注册Repeat_Job[" + jobEntire.getMainJob().getName() + "]");
		KettleRecord record = recordService.applyScheduleJob(jobEntire, cronExpression);
		KettleResult result = new KettleResult();
		result.setUuid(record.getUuid());
		result.setStatus(record.getStatus());
		result.setErrMsg(record.getErrMsg());
		return result;
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public void modifySchedule(String uuid, String newCron) throws KettleException {
		logger.info("Kettle修改Repeat_Job[" + uuid + "]的Cron表达式[" + newCron + "]");
		try {
			recordService.modifyRecordSchedule(uuid, newCron);
		} catch (Exception e) {
			logger.error("Kettle环境更新定时任务[" + uuid + "]失败!", e);
			throw new KettleException("Kettle环境更新定时任务[" + uuid + "]失败!", e);
		}
	}

	/**
	 * 执行
	 * 
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult excuteJob(String uuid) throws KettleException {
		logger.info("Kettle开始执行Job[" + uuid + "]");
		KettleRecord record = recordService.excuteJob(uuid);
		KettleResult result = new KettleResult();
		result.setUuid(record.getUuid());
		result.setStatus(record.getStatus());
		result.setErrMsg(record.getErrMsg());
		return result;
	}

	/**
	 * 查询Job
	 * 
	 * @param uuid
	 * @return
	 * @throws KettleException
	 */
	public KettleResult queryJob(String uuid) throws KettleException {
		logger.info("Kettle开始查询Job[" + uuid + "]");
		KettleRecord record = recordService.queryJob(uuid);
		KettleResult result = new KettleResult();
		result.setUuid(record.getUuid());
		result.setStatus(record.getStatus());
		result.setErrMsg(record.getErrMsg());
		return result;
	}

	/**
	 * 查询Job
	 * 
	 * @param uuid
	 * @throws KettleException
	 */
	public void deleteJob(String uuid) throws KettleException {
		logger.info("Kettle开始查询Job[" + uuid + "]");
		recordService.deleteJob(uuid);
	}

	private class DelAbandonedRecord implements Runnable {
		@Override
		public void run() {
			try {
				List<KettleRecord> records = kettleMgrEnvironment.getDbClient().allStopRecord();
				Long current = System.currentTimeMillis();
				for (KettleRecord record : records) {
					if ((current - record.getUpdateTime().getTime()) / 1000 / 60
							/ 60 > KettleMgrEnvironment.KETTLE_RECORD_PERSIST_MAX_HOUR) {
						deleteJob(record.getUuid());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
