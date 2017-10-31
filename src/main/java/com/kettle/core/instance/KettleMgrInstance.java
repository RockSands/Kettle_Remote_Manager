package com.kettle.core.instance;

import java.util.UUID;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.bean.KettleResult;
import com.kettle.core.instance.metas.KettleSelectMeta;
import com.kettle.core.instance.metas.SyncTablesDatas;
import com.kettle.core.instance.metas.TableDataMigration;
import com.kettle.core.repo.KettleDBRepositoryClient;
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
	 * 资源库
	 */
	private KettleDatabaseRepository repository = null;

	/**
	 * 远程执行池
	 */
	private KettleRemotePool kettleRemotePool;

	/**
	 * 资源池数据库连接
	 */
	private KettleDBRepositoryClient dbRepositoryClient;

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
	}

	private void init() {
		try {
			KettleEnvironment.init();
			repository = new KettleDatabaseRepository();
			RepositoryMeta dbrepositoryMeta = new KettleDatabaseRepositoryMeta(
					EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_META_ID"),
					EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_META_NAME"),
					EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_META_DESCRIPTION"),
					new DatabaseMeta(EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_NAME"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_TYPE"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_ACCESS"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_HOST"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_DATABASENAME"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_PORT"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_USER"),
							EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_DB_PASSWD")));
			repository.init(dbrepositoryMeta);
			repository.connect(EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_USER"),
					EnvUtil.getSystemProperty("KETTLE_DATABASE_REPOSITORY_PASSWD"));
			dbRepositoryClient = new KettleDBRepositoryClient(repository);
			kettleRemotePool = new KettleRemotePool(dbRepositoryClient, null, null);
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
	public KettleResult syncTablesDatas(KettleSelectMeta source, KettleSelectMeta target) throws KettleException {
		try {
			TransMeta transMeta = SyncTablesDatas.create(source, target);
			kettleRemotePool.getDbRepositoryClient().saveTransMeta(transMeta);
			JobMeta jobMeta = new JobMeta();
			jobMeta.setName(UUID.randomUUID().toString().replace("-", ""));
			// 启动
			JobEntryCopy start = new JobEntryCopy(new JobEntrySpecial("START", true, false));
			start.setLocation(150, 100);
			start.setDrawn(true);
			start.setDescription("START");
			jobMeta.addJobEntry(start);
			// 主执行
			JobEntryTrans trans = new JobEntryTrans(transMeta.getName());
			trans.setTransObjectId(transMeta.getObjectId());
			trans.setDirectory("${Internal.Entry.Current.Directory}");
			JobEntryCopy excuter = new JobEntryCopy(trans);
			excuter.setLocation(300, 100);
			excuter.setDrawn(true);
			excuter.setDescription("START");
			jobMeta.addJobEntry(excuter);
			// 连接
			JobHopMeta hop = new JobHopMeta(start, excuter);
			jobMeta.addJobHop(hop);

			KettleRecord record = kettleRemotePool.applyJobMeta(jobMeta);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境执行SyncTable发生异常!", e);
			throw new KettleException("Kettle环境执行SyncTable发生异常!");
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult syncTablesDataSchedule(KettleSelectMeta source, KettleSelectMeta target, String cron)
			throws KettleException {
		try {
			TransMeta transMeta = SyncTablesDatas.create(source, target);
			KettleRecord record = kettleRemotePool.applyScheduleTransMeta(transMeta, cron);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境执行SyncTable发生异常!", e);
			throw new KettleException("Kettle环境执行SyncTable发生异常!");
		}
	}

	/**
	 * @param source
	 * @param target
	 * @return
	 * @throws KettleException
	 */
	public KettleResult tableDataMigration(KettleSelectMeta source, KettleSelectMeta target) throws KettleException {
		try {
			TransMeta transMeta = TableDataMigration.create(source, target);
			JobMeta jobMeta = new JobMeta();
			// 启动
			JobEntryCopy start = new JobEntryCopy(new JobEntrySpecial("START", true, false));
			start.setLocation(150, 100);
			start.setDrawn(true);
			start.setDescription("START");
			// 主执行
			JobEntryTrans jet = new JobEntryTrans();
			jet.setWaitingToFinish(true);
			KettleRecord record = kettleRemotePool.applyJobMeta(jobMeta);
			KettleResult result = new KettleResult();
			result.setErrMsg(record.getErrMsg());
			result.setStatus(record.getStatus());
			result.setUuid(record.getUuid());
			return result;
		} catch (Exception e) {
			logger.error("Kettle环境执行SyncTable发生异常!", e);
			throw new KettleException("Kettle环境执行SyncTable发生异常!");
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
	public KettleResult queryResult(String uuid) throws KettleException {
		KettleRecord bean = dbRepositoryClient.queryRecord(uuid);
		if (bean == null) {
			return null;
		}
		KettleResult result = new KettleResult();
		result.setUuid(bean.getUuid());
		result.setStatus(bean.getStatus());
		result.setErrMsg(bean.getErrMsg());
		return result;
	}
}
