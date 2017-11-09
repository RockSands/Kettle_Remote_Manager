package com.kettle.core.instance;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.sql.JobEntrySQL;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.KettleVariables;
import com.kettle.core.bean.KettleResult;
import com.kettle.core.db.KettleDBClient;
import com.kettle.core.instance.metas.CompareTablesDatas;
import com.kettle.core.instance.metas.KettleSQLSMeta;
import com.kettle.core.instance.metas.KettleTableMeta;
import com.kettle.core.instance.metas.SyncTablesDatas;
import com.kettle.core.instance.metas.TableDataMigration;
import com.kettle.core.repo.KettleRepositoryClient;
import com.kettle.record.KettleRecord;
import com.kettle.remote.KettleRemotePool;

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
	public static KettleMgrEnvironment kettleMgrEnvironment = new KettleMgrEnvironment();

	/**
	 * 资源池数据库连接
	 */
	private KettleRepositoryClient repositoryClient;

	/**
	 * 数据库连接
	 */
	private KettleDBClient dbClient;

	/**
	 * 定时任务
	 */
	private ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor();

	/**
	 * Record保留最长时间
	 */
	private Integer recordPersistMax;

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
		if (EnvUtil.getSystemProperty("KETTLE_RECORD_PERSIST_MAX_HOUR") != null) {
			recordPersistMax = Integer.valueOf(EnvUtil.getSystemProperty("KETTLE_RECORD_PERSIST_MAX_HOUR"));
		}
		if (recordPersistMax != null && recordPersistMax > 0) {
			Calendar now = Calendar.getInstance();
			now.setTime(new Date());
			int initialDelay = 24 - now.get(Calendar.HOUR_OF_DAY) + 1;
			threadPool.scheduleAtFixedRate(new DelAbandonedRecord(), initialDelay, 24, TimeUnit.HOURS);
		}
	}

	private void init() {
		try {
			KettleEnvironment.init();
			// 加载本地资源文件
			InputStream is = getClass().getClassLoader().getResourceAsStream("kettl_env.properties");
			Properties properties = new Properties();
			properties.load(is);
			EnvUtil.applyKettleProperties(properties, true);
			KettleFileRepository repository = new KettleFileRepository();
			RepositoryMeta dbrepositoryMeta = new KettleFileRepositoryMeta(
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_ID"),
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_NAME"),
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_DESCRIPTION"),
					EnvUtil.getSystemProperty("KETTLE_FILE_REPOSITORY_META_PATH"));
			repository.init(dbrepositoryMeta);
			repository.connect(EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_USER"),
					EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_PASSWD"));
			repositoryClient = new KettleRepositoryClient(repository);
			DatabaseMeta databaseMeta = new DatabaseMeta(EnvUtil.getSystemProperty("KETTLE_RECORD_DB_NAME"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_TYPE"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_ACCESS"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_HOST"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_DATABASENAME"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_PORT"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_USER"),
					EnvUtil.getSystemProperty("KETTLE_RECORD_DB_PASSWD"));
			dbClient = new KettleDBClient(databaseMeta);
			kettleRemotePool = new KettleRemotePool(repositoryClient, dbClient);
		} catch (Exception ex) {
			throw new RuntimeException("KettleMgrInstance初始化失败", ex);
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult registeSyncTablesDatas(KettleTableMeta source, KettleTableMeta target) throws KettleException {
		try {
			// 路径
			RepositoryDirectoryInterface directory = kettleRemotePool.getRepositoryDirectory();
			// TransMeta
			TransMeta transMeta = SyncTablesDatas.create(source, target);
			transMeta.setRepositoryDirectory(directory);
			// MainJob
			JobMeta mainJob = new JobMeta();
			mainJob.setRepositoryDirectory(directory);
			mainJob.setName(UUID.randomUUID().toString().replace("-", ""));
			// 启动
			JobEntryCopy start = new JobEntryCopy(new JobEntrySpecial("START", true, false));
			start.setLocation(150, 100);
			start.setDrawn(true);
			start.setDescription("START");
			mainJob.addJobEntry(start);
			// 主执行
			JobEntryTrans trans = new JobEntryTrans(transMeta.getName());
			trans.setTransObjectId(transMeta.getObjectId());
			trans.setTransname(transMeta.getName());
			trans.setDirectory(directory.getPath());
			JobEntryCopy excuter = new JobEntryCopy(trans);
			excuter.setLocation(300, 100);
			excuter.setDrawn(true);
			excuter.setDescription("MAINJOB");
			mainJob.addJobEntry(excuter);
			// 连接
			JobHopMeta hop = new JobHopMeta(start, excuter);
			mainJob.addJobHop(hop);
			KettleRecord record = kettleRemotePool.registeJobMeta(Arrays.asList(transMeta), null, mainJob);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境注册SyncTablesDatas发生异常!", e);
			throw new KettleException("Kettle环境注册SyncTablesDatas发生异常!");
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult registeCompareTablesDatas(KettleTableMeta base, KettleTableMeta compare,
			KettleTableMeta newOption) throws KettleException {
		try {
			// 路径
			RepositoryDirectoryInterface directory = kettleRemotePool.getRepositoryDirectory();
			TransMeta transMeta = CompareTablesDatas.create(base, compare, newOption);
			transMeta.setRepositoryDirectory(directory);
			JobMeta mainJob = new JobMeta();
			mainJob.setRepositoryDirectory(directory);
			mainJob.setName(UUID.randomUUID().toString().replace("-", ""));
			// 启动
			JobEntryCopy start = new JobEntryCopy(new JobEntrySpecial("START", true, false));
			start.setLocation(150, 100);
			start.setDrawn(true);
			start.setDescription("START");
			mainJob.addJobEntry(start);
			// 主执行
			JobEntryTrans trans = new JobEntryTrans(transMeta.getName());
			trans.setTransObjectId(transMeta.getObjectId());
			trans.setTransname(transMeta.getName());
			trans.setDirectory(directory.getPath());
			JobEntryCopy excuter = new JobEntryCopy(trans);
			excuter.setLocation(300, 100);
			excuter.setDrawn(true);
			excuter.setDescription("MAINJOB");
			mainJob.addJobEntry(excuter);
			// 连接
			JobHopMeta hop = new JobHopMeta(start, excuter);
			mainJob.addJobHop(hop);

			KettleRecord record = kettleRemotePool.registeJobMeta(Arrays.asList(transMeta), null, mainJob);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境注册CompareTablesDatas发生异常!", e);
			throw new KettleException("Kettle环境注册CompareTablesDatas发生异常!");
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult scheduleSyncTablesData(KettleTableMeta source, KettleTableMeta target, String cron)
			throws KettleException {
		try {
			// 路径
			RepositoryDirectoryInterface directory = kettleRemotePool.getRepositoryDirectory();
			TransMeta transMeta = SyncTablesDatas.create(source, target);
			transMeta.setRepositoryDirectory(directory);
			JobMeta mainJob = new JobMeta();
			mainJob.setRepositoryDirectory(directory);
			mainJob.setName(UUID.randomUUID().toString().replace("-", ""));
			// 启动
			JobEntryCopy start = new JobEntryCopy(new JobEntrySpecial("START", true, false));
			start.setLocation(150, 100);
			start.setDrawn(true);
			start.setDescription("START");
			mainJob.addJobEntry(start);
			// 主执行
			JobEntryTrans trans = new JobEntryTrans(transMeta.getName());
			trans.setTransObjectId(transMeta.getObjectId());
			trans.setTransname(transMeta.getName());
			trans.setDirectory(directory.getPath());
			JobEntryCopy excuter = new JobEntryCopy(trans);
			excuter.setLocation(300, 100);
			excuter.setDrawn(true);
			excuter.setDescription("MAINJOB");
			mainJob.addJobEntry(excuter);
			// 连接
			JobHopMeta hop = new JobHopMeta(start, excuter);
			mainJob.addJobHop(hop);

			KettleRecord record = kettleRemotePool.applyScheduleJobMeta(Arrays.asList(transMeta), null, mainJob, cron);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境执行scheduleSyncTablesData发生异常!", e);
			throw new KettleException("Kettle环境执行scheduleSyncTablesData发生异常!");
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult tableDataMigration(KettleTableMeta source, KettleTableMeta target, KettleSQLSMeta success,
			KettleSQLSMeta error) throws KettleException {
		try {
			RepositoryDirectoryInterface directory = kettleRemotePool.getRepositoryDirectory();
			TransMeta transMeta = TableDataMigration.createTableDataMigration(source, target);
			transMeta.setRepositoryDirectory(directory);
			JobMeta mainJob = new JobMeta();
			mainJob.setRepositoryDirectory(directory);
			mainJob.setName(UUID.randomUUID().toString().replace("-", ""));
			// 启动
			JobEntryCopy start = new JobEntryCopy(new JobEntrySpecial("START", true, false));
			start.setLocation(150, 100);
			start.setDrawn(true);
			start.setDescription("START");
			mainJob.addJobEntry(start);
			// 主执行
			JobEntryTrans trans = new JobEntryTrans(transMeta.getName());
			trans.setTransObjectId(transMeta.getObjectId());
			trans.setWaitingToFinish(true);
			trans.setDirectory(directory.getPath());
			JobEntryCopy excuter = new JobEntryCopy(trans);
			excuter.setLocation(300, 100);
			excuter.setDrawn(true);
			excuter.setDescription("MAINJOB");
			mainJob.addJobEntry(excuter);
			// 连接
			JobHopMeta hop = new JobHopMeta(start, excuter);
			mainJob.addJobHop(hop);
			// 设置异常处理
			if (error != null && error.getSqls() != null && !error.getSqls().isEmpty()) {
				DatabaseMeta errDataBase = new DatabaseMeta(
						error.getHost() + "_" + error.getDatabase() + "_" + error.getUser(), error.getType(), "Native",
						error.getHost(), error.getDatabase(), error.getPort(), error.getUser(), error.getPasswd());
				mainJob.addDatabase(errDataBase);
				StringBuffer sqlStr = new StringBuffer();
				for (String sql : error.getSqls()) {
					sqlStr.append(sql).append(";");
				}
				JobEntrySQL errSQL = new JobEntrySQL();
				errSQL.setDatabase(errDataBase);
				errSQL.setSQL(sqlStr.toString());
				errSQL.setSendOneStatement(false);
				JobEntryCopy errJEC = new JobEntryCopy(errSQL);
				errJEC.setName("error");
				errJEC.setLocation(200, 200);
				errJEC.setDrawn(true);
				errJEC.setDescription("errdeal");
				mainJob.addJobEntry(errJEC);
				JobHopMeta errhop = new JobHopMeta(excuter, errJEC);
				errhop.setUnconditional(false);
				errhop.setEvaluation(false);
				mainJob.addJobHop(errhop);
			}
			// SUCCESS处理
			if (success != null && success.getSqls() != null && !success.getSqls().isEmpty()) {
				DatabaseMeta successDataBase = new DatabaseMeta(
						success.getHost() + "_" + success.getDatabase() + "_" + success.getUser(), success.getType(),
						"Native", success.getHost(), success.getDatabase(), success.getPort(), success.getUser(),
						success.getPasswd());
				mainJob.addDatabase(successDataBase);
				StringBuffer sqlStr = new StringBuffer();
				for (String sql : success.getSqls()) {
					sqlStr.append(sql).append(";");
				}
				JobEntrySQL successSQL = new JobEntrySQL();
				successSQL.setDatabase(successDataBase);
				successSQL.setSQL(sqlStr.toString());
				successSQL.setSendOneStatement(false);
				JobEntryCopy successJEC = new JobEntryCopy(successSQL);
				successJEC.setName("SUCCESS");
				successJEC.setLocation(400, 200);
				successJEC.setDrawn(true);
				successJEC.setDescription("errdeal");
				mainJob.addJobEntry(successJEC);
				JobHopMeta successHop = new JobHopMeta(excuter, successJEC);
				successHop.setUnconditional(false);
				successHop.setEvaluation(true);
				mainJob.addJobHop(successHop);
			}
			KettleRecord record = kettleRemotePool.registeJobMeta(Arrays.asList(transMeta), null, mainJob);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境注册tableDataMigration发生异常!", e);
			throw new KettleException("Kettle环境注册tableDataMigration发生异常!");
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public void modifySchedule(String uuid, String newCron) throws KettleException {
		try {
			kettleRemotePool.modifyRecordSchedule(uuid, newCron);
		} catch (Exception e) {
			logger.error("Kettle环境更新定时任务[" + uuid + "]失败!", e);
			throw new KettleException("Kettle环境更新定时任务[" + uuid + "]失败!");
		}
	}

	/**
	 * 查询数据迁移
	 * 
	 * @param transID
	 * @return
	 * @throws KettleException
	 */
	public KettleResult excuteJob(String uuid) throws KettleException {
		KettleRecord bean = kettleRemotePool.exuteJobMeta(uuid);
		KettleResult result = new KettleResult();
		result.setUuid(bean.getUuid());
		result.setStatus(bean.getStatus());
		result.setErrMsg(bean.getErrMsg());
		return result;
	}

	/**
	 * 查询数据迁移
	 * 
	 * @param transID
	 * @return
	 * @throws KettleException
	 */
	public KettleResult queryResult(String uuid) throws Exception {
		KettleRecord bean = kettleRemotePool.getRecord(uuid);
		if (bean == null) {
			return null;
		}
		KettleResult result = new KettleResult();
		result.setUuid(bean.getUuid());
		if (bean.isApply() || bean.isRepeat()) {
			result.setStatus(KettleVariables.RECORD_STATUS_RUNNING);
		}
		result.setStatus(bean.getStatus());
		result.setErrMsg(bean.getErrMsg());
		return result;
	}

	/**
	 * 查询数据迁移
	 * 
	 * @param transID
	 * @return
	 * @throws KettleException
	 */
	public void deleteJob(String uuid) throws Exception {
		kettleRemotePool.deleteJob(uuid);
	}

	private class DelAbandonedRecord implements Runnable {
		@Override
		public void run() {
			try {
				List<KettleRecord> records = dbClient.allStopRecord();
				Long current = System.currentTimeMillis();
				for (KettleRecord record : records) {
					if ((current - record.getUpdateTime().getTime()) / 1000 / 60 / 60 > recordPersistMax) {
						deleteJob(record.getUuid());
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
