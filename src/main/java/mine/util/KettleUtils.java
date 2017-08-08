package mine.util;

import java.util.List;

import org.apache.log4j.Logger;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleSecurityException;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryBase;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.AbstractRepository;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.StringObjectId;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.jobexecutor.JobExecutorMeta;
import org.pentaho.di.trans.steps.transexecutor.TransExecutorMeta;

/**
 * ClassName: KettleUtils <br/>
 * Function: kettle定制化开发工具集. <br/>
 * date: 2015年4月29日 上午8:56:24 <br/>
 * 
 * @author jingma
 * @version 0.0.1
 * @since JDK 1.6
 */
public class KettleUtils {
	/**
	 * LOG:日志
	 */
	public static Logger log = Logger.getLogger(KettleUtils.class);
	/**
	 * repository:kettle资源库
	 */
	private static AbstractRepository repository;
	/**
	 * 转换模板
	 */
	private static TransMeta transMetaTemplate;
	/**
	 * 作业模板
	 */
	private static JobMeta jobMetaTemplate;

	/**
	 * getInstance:获取的单例资源库. <br/>
	 * 
	 * @author jingma
	 * @return 已经初始化的资源库
	 * @throws KettleException
	 *             若没有初始化则抛出异常
	 * @since JDK 1.6
	 */
	public static AbstractRepository getInstanceRep() throws KettleException {
		if (repository != null) {
			return repository;
		} else {
			throw new KettleException("没有初始化资源库");
		}
	}

	/**
	 * createFileRep:创建文件资源库. <br/>
	 * 
	 * @author jingma
	 * @param id
	 *            资源库id
	 * @param name
	 *            资源库名称
	 * @param description
	 *            资源库描述
	 * @param baseDirectory
	 *            资源库目录
	 * @return 已经初始化的资源库
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static AbstractRepository createFileRep(String id, String name, String description, String baseDirectory)
			throws KettleException {
		destroy();
		// 初始化kettle环境
		if (!KettleEnvironment.isInitialized()) {
			KettleEnvironment.init();
		}
		repository = new KettleFileRepository();
		KettleFileRepositoryMeta fileRepMeta = new KettleFileRepositoryMeta(id, name, description, baseDirectory);
		repository.init(fileRepMeta);
		log.info(repository.getName() + "资源库初始化成功");
		return repository;
	}

	/**
	 * createDBRep:创建数据库资源库. <br/>
	 * 
	 * @author jingma
	 * @param name
	 *            数据库连接名称
	 * @param type
	 *            数据库类型
	 * @param access
	 *            访问类型
	 * @param host
	 *            ip地址
	 * @param db
	 *            数据库名称
	 * @param port
	 *            端口
	 * @param user
	 *            数据库用户名
	 * @param pass
	 *            数据库密码
	 * @return 初始化的资源库
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static AbstractRepository createDBRep(String name, String type, String access, String host, String db,
			String port, String user, String pass) throws KettleException {
		return createDBRep(name, type, access, host, db, port, user, pass, "DBRep", "DBRep", "数据库资源库");
	}

	/**
	 * createDBRep:创建数据库资源库. <br/>
	 * 
	 * @author jingma
	 * @param name
	 *            数据库连接名称
	 * @param type
	 *            数据库类型
	 * @param access
	 *            访问类型
	 * @param host
	 *            ip地址
	 * @param db
	 *            数据库名称
	 * @param port
	 *            端口
	 * @param user
	 *            数据库用户名
	 * @param pass
	 *            数据库密码
	 * @param id
	 *            资源库id
	 * @param repName
	 *            资源库名称
	 * @param description
	 *            资源库描述
	 * @return 已经初始化的资源库
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static AbstractRepository createDBRep(String name, String type, String access, String host, String db,
			String port, String user, String pass, String id, String repName, String description)
			throws KettleException {
		destroy();
		// 初始化kettle环境
		if (!KettleEnvironment.isInitialized()) {
			KettleEnvironment.init();
		}
		// 创建资源库对象
		repository = new KettleDatabaseRepository();
		// 创建资源库数据库对象，类似我们在spoon里面创建资源库
		DatabaseMeta dataMeta = new DatabaseMeta(name, type, access, host, db, port, user, pass);
		// 资源库元对象
		KettleDatabaseRepositoryMeta kettleDatabaseMeta = new KettleDatabaseRepositoryMeta(id, repName, description,
				dataMeta);
		// 给资源库赋值
		repository.init(kettleDatabaseMeta);
		log.info(repository.getName() + "资源库初始化成功");
		return repository;
	}

	/**
	 * connect:连接资源库. <br/>
	 * 
	 * @author jingma
	 * @return 连接后的资源库
	 * @throws KettleSecurityException
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static AbstractRepository connect() throws KettleSecurityException, KettleException {
		return connect(null, null);
	}

	/**
	 * connect:连接资源库. <br/>
	 * 
	 * @author jingma
	 * @param username
	 *            资源库用户名
	 * @param password
	 *            资源库密码
	 * @return 连接后的资源库
	 * @throws KettleSecurityException
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static AbstractRepository connect(String username, String password)
			throws KettleSecurityException, KettleException {
		repository.connect(username, password);
		log.info(repository.getName() + "资源库连接成功");
		return repository;
	}

	/**
	 * setRepository:设置资源库. <br/>
	 * 
	 * @author jingma
	 * @param repository
	 *            外部注入资源库
	 * @since JDK 1.6
	 */
	public static void setRepository(AbstractRepository repository) {
		KettleUtils.repository = repository;
	}

	/**
	 * destroy:释放资源库. <br/>
	 * 
	 * @author jingma
	 * @since JDK 1.6
	 */
	public static void destroy() {
		if (repository != null) {
			repository.disconnect();
			log.info(repository.getName() + "资源库释放成功");
		}
	}

	/**
	 * loadJob:通过id加载job. <br/>
	 * 
	 * @author jingma
	 * @param jobId
	 *            数字型job的id，数据库资源库时用此方法
	 * @return job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(long jobId) throws KettleException {
		return repository.loadJob(new LongObjectId(jobId), null);
	}

	/**
	 * loadJob:通过id加载job. <br/>
	 * 
	 * @author jingma
	 * @param jobId
	 *            字符串job的id，文件资源库时用此方法
	 * @return job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobId) throws KettleException {
		return repository.loadJob(new StringObjectId(jobId), null);
	}

	/**
	 * loadTrans:加载作业. <br/>
	 * 
	 * @author jingma
	 * @param jobname
	 *            作业名称
	 * @param directory
	 *            作业路径
	 * @return 作业元数据
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobname, String directory) {
		return loadJob(jobname, directory, repository);
	}

	/**
	 * loadTrans:加载作业. <br/>
	 * 
	 * @author jingma
	 * @param jobname
	 *            作业名称
	 * @param directory
	 *            作业路径
	 * @param repository
	 *            资源库
	 * @return 作业元数据
	 * @since JDK 1.6
	 */
	public static JobMeta loadJob(String jobname, String directory, AbstractRepository repository) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directory);
			return repository.loadJob(jobname, dir, null, null);
		} catch (KettleException e) {
			log.error("获取作业失败,jobname:" + jobname + ",directory:" + directory, e);
		}
		return null;
	}

	/**
	 * loadTrans:加载转换. <br/>
	 * 
	 * @author jingma
	 * @param transname
	 *            转换名称
	 * @param directory
	 *            转换路径
	 * @return 转换元数据
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(String transname, String directory) {
		return loadTrans(transname, directory, repository);
	}

	/**
	 * loadTrans:加载转换. <br/>
	 * 
	 * @author jingma
	 * @param transname
	 *            转换名称
	 * @param directory
	 *            转换路径
	 * @param repository
	 *            资源库
	 * @return 转换元数据
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(String transname, String directory, AbstractRepository repository) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directory);
			return repository.loadTransformation(transname, dir, null, true, null);
		} catch (KettleException e) {
			log.error("获取转换失败,transname:" + transname + ",directory:" + directory, e);
		}
		return null;
	}

	/**
	 * loadTrans:根据job元数据获取指定转换元数据. <br/>
	 * 
	 * @author jingma
	 * @param jobMeta
	 *            job元数据
	 * @param teansName
	 *            转换名称
	 * @return 转换元数据
	 * @since JDK 1.6
	 */
	public static TransMeta loadTrans(JobMeta jobMeta, String teansName) {
		JobEntryTrans trans = (JobEntryTrans) (jobMeta.findJobEntry(teansName).getEntry());
		TransMeta transMeta = KettleUtils.loadTrans(trans.getTransname(), trans.getDirectory());
		return transMeta;
	}

	/**
	 * 根据转换元数据和步骤名称获取具体的步骤元数据的复制. <br/>
	 * 一般是不需要这用这个方法的，该方法获取的实体不属于该转换，相当于一个复制 ，修改了直接保存transMeta是没有保存到修改的。<br/>
	 * 若需要修改转换，可以使用：(T)transMeta.findStep(stepName).getStepMetaInterface()，
	 * 这个方法获取的步骤是属于该转换的，修改后，直接保存transMeta就能实现转换修改<br/>
	 * 
	 * @author jingma
	 * @param transMeta
	 *            转换元数据
	 * @param stepName
	 *            步骤名称
	 * @param stepMeta
	 *            具体的步骤元数据对象
	 * @return 从资源库获取具体数据的步骤元数据
	 * @since JDK 1.6
	 */
	public static <T extends BaseStepMeta> T loadStep(TransMeta transMeta, String stepName, T stepMeta) {
		StepMeta step = transMeta.findStep(stepName);
		try {
			stepMeta.readRep(KettleUtils.getInstanceRep(), null, step.getObjectId(),
					KettleUtils.getInstanceRep().readDatabases());
		} catch (KettleException e) {
			log.error("获取步骤失败", e);
		}
		return stepMeta;
	}

	/**
	 * 根据作业元数据和作业实体名称获取具体的作业实体元数据的复制。<br/>
	 * 一般是不需要这用这个方法的，该方法获取的实体不属于该job了，相当于一个复制 ，修改了直接保存jobMeta是没有保存到修改的。<br/>
	 * 若需要修改job，可以使用：(T)jobMeta.findJobEntry(jobEntryName).getEntry()，
	 * 这个方法获取的实体是属于job，修改后，直接保存jobMeta就能实现job修改<br/>
	 * 
	 * @author jingma
	 * @param jobMeta
	 *            作业元数据
	 * @param jobEntryName
	 *            作业实体名称
	 * @param jobEntryMeta
	 *            要获取的作业实体对象
	 * @return 加载了数据的作业实体对象
	 */
	public static <T extends JobEntryBase> T loadJobEntry(JobMeta jobMeta, String jobEntryName, T jobEntryMeta) {
		try {
			jobEntryMeta.loadRep(KettleUtils.getInstanceRep(), null,
					jobMeta.findJobEntry(jobEntryName).getEntry().getObjectId(),
					KettleUtils.getInstanceRep().readDatabases(), null);
		} catch (KettleException e) {
			log.error("获取作业控件失败", e);
		}
		return jobEntryMeta;
	}

	/**
	 * saveTrans:保存转换. <br/>
	 * 
	 * @author jingma
	 * @param transMeta
	 *            转换元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static void saveTrans(TransMeta transMeta) throws KettleException {
		// repository.save(transMeta, null, new RepositoryImporter(repository),
		// true );
		repository.save(transMeta, null, null, true);
	}

	/**
	 * saveJob:保存job. <br/>
	 * 
	 * @author jingma
	 * @param jobMeta
	 *            job元数据
	 * @throws KettleException
	 * @since JDK 1.6
	 */
	public static void saveJob(JobMeta jobMeta) throws KettleException {
		// repository.save(jobMeta, null, new RepositoryImporter(repository),
		// true );
		repository.save(jobMeta, null, null, true);
	}

	/**
	 * isDirectoryExist:判断指定的job目录是否存在. <br/>
	 * 
	 * @author jingma
	 * @param directoryName
	 * @return
	 * @since JDK 1.6
	 */
	public static boolean isDirectoryExist(String directoryName) {
		try {
			RepositoryDirectoryInterface dir = repository.findDirectory(directoryName);
			if (dir == null) {
				return false;
			} else {
				return true;
			}
		} catch (KettleException e) {
			log.error("判断job目录是否存在失败！", e);
		}
		return false;
	}

	/**
	 * 将步骤smi设置到转换trans中<br/>
	 * 
	 * @author jingma
	 * @param teans
	 *            转换元数据
	 * @param stepName
	 *            步骤名称
	 * @param smi
	 *            步骤
	 */
	public static void setStepToTrans(TransMeta teans, String stepName, StepMetaInterface smi) {
		try {
			StepMeta step = teans.findStep(stepName);
			step.setStepMetaInterface(smi);
		} catch (Exception e) {
			log.error("将步骤smi设置到转换trans中-失败", e);
		}
	}

	/**
	 * 将步骤smi设置到转换trans中并保存到资源库 <br/>
	 * 
	 * @author jingma
	 * @param teans
	 *            转换元数据
	 * @param stepName
	 *            步骤名称
	 * @param smi
	 *            步骤
	 */
	public static void setStepToTransAndSave(TransMeta teans, String stepName, StepMetaInterface smi) {
		setStepToTrans(teans, stepName, smi);
		try {
			KettleUtils.saveTrans(teans);
		} catch (KettleException e) {
			log.error("将步骤smi设置到转换trans中并保存到资源库-失败", e);
		}
	}

	/**
	 * 步骤数据预览 <br/>
	 * 
	 * @author jingma
	 * @param teans
	 *            转换
	 * @param testStep
	 *            步骤名称
	 * @param smi
	 *            步骤实体
	 * @param previewSize
	 *            预览的条数
	 * @return 预览结果
	 */
	public static List<List<Object>> stepPreview(TransMeta teans, String testStep, StepMetaInterface smi,
			int previewSize) {
		TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation(teans, smi, testStep);
		TransPreviewUtil tpu = new TransPreviewUtil(previewMeta, new String[] { testStep }, new int[] { previewSize });
		tpu.doPreview();
		return TransPreviewUtil.getData(tpu.getPreviewRowsMeta(testStep), tpu.getPreviewRows(testStep));
	}

	/**
	 * 将指定job复制到KettleUtils中的资源库 <br/>
	 * 
	 * @author jingma
	 * @param jobName
	 *            job名称
	 * @param jobPath
	 *            job路径
	 * @param repository
	 *            来源资源库
	 * @throws KettleException
	 */
	public static void jobCopy(String jobName, String jobPath, AbstractRepository repository) throws KettleException {
		JobMeta jobMeta = KettleUtils.loadJob(jobName, jobPath, repository);
		for (JobEntryCopy jec : jobMeta.getJobCopies()) {
			if (jec.isTransformation()) {
				JobEntryTrans jet = (JobEntryTrans) jec.getEntry();
				transCopy(jet.getObjectName(), jet.getDirectory(), repository);
			} else if (jec.isJob()) {
				JobEntryJob jej = (JobEntryJob) jec.getEntry();
				jobCopy(jej.getObjectName(), jej.getDirectory(), repository);
			}
		}
		jobMeta.setRepository(KettleUtils.getInstanceRep());
		jobMeta.setMetaStore(KettleUtils.getInstanceRep().getMetaStore());
		if (!isDirectoryExist(jobPath)) {
			// 所在目录不存在则创建
			KettleUtils.repository.createRepositoryDirectory(KettleUtils.repository.findDirectory("/"), jobPath);
		}
		KettleUtils.saveJob(jobMeta);
	}

	/**
	 * 将指定转换复制到KettleUtils中的资源库 <br/>
	 * 
	 * @author jingma
	 * @param jobName
	 *            转换名称
	 * @param jobPath
	 *            转换路径
	 * @param repository
	 *            来源资源库
	 * @throws KettleException
	 */
	public static void transCopy(String transName, String transPath, AbstractRepository repository)
			throws KettleException {
		TransMeta tm = KettleUtils.loadTrans(transName, transPath, repository);
		for (StepMeta sm : tm.getSteps()) {
			if (sm.isJobExecutor()) {
				JobExecutorMeta jem = (JobExecutorMeta) sm.getStepMetaInterface();
				jobCopy(jem.getJobName(), jem.getDirectoryPath(), repository);
			} else if (sm.getStepMetaInterface() instanceof TransExecutorMeta) {
				TransExecutorMeta te = (TransExecutorMeta) sm.getStepMetaInterface();
				transCopy(te.getTransName(), te.getDirectoryPath(), repository);
			}
		}
		if (!isDirectoryExist(transPath)) {
			// 所在目录不存在则创建
			KettleUtils.repository.createRepositoryDirectory(KettleUtils.repository.findDirectory("/"), transPath);
		}
		tm.setRepository(KettleUtils.getInstanceRep());
		tm.setMetaStore(KettleUtils.getInstanceRep().getMetaStore());
		KettleUtils.saveTrans(tm);
	}

	/**
	 * @return transMetaTemplate
	 */
	public static TransMeta getTransMetaTemplate() {
		// if(transMetaTemplate==null){
		// setTransMetaTemplate(KettleUtils.loadTrans(SysCode.TRANS_TEMPLATE_NAME,
		// SysCode.TEMPLATE_DIR));
		// }
		return transMetaTemplate;
	}

	/**
	 * @param transMetaTemplate
	 *            the transMetaTemplate to set
	 */
	public static void setTransMetaTemplate(TransMeta transMetaTemplate) {
		KettleUtils.transMetaTemplate = transMetaTemplate;
	}

	/**
	 * @return jobMetaTemplate
	 */
	public static JobMeta getJobMetaTemplate() {
		// if(jobMetaTemplate==null){
		// setJobMetaTemplate(KettleUtils.loadJob(SysCode.JOB_TEMPLATE_NAME,
		// SysCode.TEMPLATE_DIR));
		// }
		return jobMetaTemplate;
	}

	/**
	 * @param jobMetaTemplate
	 *            the jobMetaTemplate to set
	 */
	public static void setJobMetaTemplate(JobMeta jobMetaTemplate) {
		KettleUtils.jobMetaTemplate = jobMetaTemplate;
	}

}