package com.kettle.core.instance;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kettle.core.bean.KettleTransResult;
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
	public KettleTransResult syncTablesDatas(KettleDBTranDescribe source, KettleDBTranDescribe target)
			throws KettleException {
		try {
			TransMeta transMeta = SyncTablesDatas.create(source, target);
			KettleTransResult result = kettleRemotePool.applyTransMeta(transMeta);
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
	public KettleTransResult syncTablesDataSchedule(KettleDBTranDescribe source, KettleDBTranDescribe target,
			String cron) throws KettleException {
		try {
			TransMeta transMeta = SyncTablesDatas.create(source, target);
			KettleTransResult result = kettleRemotePool.applyScheduleTransMeta(transMeta, cron);
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
	public KettleTransResult queryDataTransfer(long transID) throws KettleException {
		KettleRecord bean = null;
		bean = dbRepositoryClient.queryTransRecord(transID);
		if (bean == null) {
			return null;
		}
		KettleTransResult result = new KettleTransResult();
		result.setTransID(transID);
		result.setStatus(bean.getStatus());
		return result;
	}

	/**
	 * 删除数据迁移
	 * 
	 * @param transID
	 * @return
	 * @throws KettleException
	 */
	public void deleteDataTransfer(long transID) throws KettleException {
		repository.connect("admin", "admin");
		dbRepositoryClient.deleteTransRecord(transID);
	}
}
