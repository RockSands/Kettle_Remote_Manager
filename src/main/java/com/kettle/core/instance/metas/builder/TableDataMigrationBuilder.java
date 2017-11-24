package com.kettle.core.instance.metas.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.sql.JobEntrySQL;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleSQLSMeta;
import com.kettle.core.instance.metas.KettleTableMeta;
import com.kettle.core.repo.KettleRepositoryClient;

/**
 * Table迁移构建器
 * @author Administrator
 *
 */
public class TableDataMigrationBuilder {

	/**
	 * 资源链接
	 */
	private final KettleRepositoryClient repositoryClient = KettleMgrInstance.kettleMgrEnvironment
			.getRepositoryClient();

	/**
	 * 源
	 */
	private KettleTableMeta source = null;

	/**
	 * 目标
	 */
	private KettleTableMeta target = null;

	/**
	 * 成功的操作
	 */
	private KettleSQLSMeta success = null;

	/**
	 * 失败的操作
	 */
	private KettleSQLSMeta error = null;

	/**
	 * 源
	 * 
	 * @param source
	 * @return
	 */
	public TableDataMigrationBuilder source(KettleTableMeta source) {
		this.source = source;
		return this;
	}

	/**
	 * 目标
	 * 
	 * @param target
	 * @return
	 */
	public TableDataMigrationBuilder target(KettleTableMeta target) {
		this.target = target;
		return this;
	}

	/**
	 * 成功的操作
	 * 
	 * @param source
	 * @return
	 */
	public TableDataMigrationBuilder success(KettleSQLSMeta success) {
		this.success = success;
		return this;
	}

	/**
	 * 失败的操作
	 * 
	 * @param source
	 * @return
	 */
	public TableDataMigrationBuilder error(KettleSQLSMeta error) {
		this.error = error;
		return this;
	}

	/**
	 * 创建Trans
	 * 
	 * @return
	 * @throws KettleException
	 */
	private TransMeta createTrans() throws KettleException {
		final String uuid = UUID.randomUUID().toString().replace("-", "");
		TransMeta transMeta = null;
		transMeta = new TransMeta();
		transMeta.setName("TDM-" + uuid);
		final DatabaseMeta sourceDataBase = new DatabaseMeta(
				source.getHost() + "_" + source.getDatabase() + "_" + source.getUser(), source.getType(), "Native",
				source.getHost(), source.getDatabase(), source.getPort(), source.getUser(), source.getPasswd());
		transMeta.addDatabase(sourceDataBase);
		final DatabaseMeta targetDatabase = new DatabaseMeta(
				target.getHost() + "_" + target.getDatabase() + "_" + target.getUser(), target.getType(), "Native",
				target.getHost(), target.getDatabase(), target.getPort(), target.getUser(), target.getPasswd());
		transMeta.addDatabase(targetDatabase);
		/*
		 * 获取所有列
		 */
		final List<String> valueFields = new ArrayList<String>();
		valueFields.addAll(target.getColumns());
		/*
		 * Note
		 */
		final String startNote = "Start " + transMeta.getName();
		final NotePadMeta ni = new NotePadMeta(startNote, 150, 10, -1, -1);
		transMeta.addNote(ni);
		/*
		 * source
		 */
		final TableInputMeta tii = new TableInputMeta();
		tii.setDatabaseMeta(sourceDataBase);
		final String selectSQL = source.getSql();
		tii.setSQL(selectSQL);
		final StepMeta query = new StepMeta("source", tii);
		query.setLocation(150, 100);
		query.setDraw(true);
		query.setDescription("STEP-SOURCE");
		transMeta.addStep(query);

		/*
		 * 转换名称
		 */
		final String[] sourceFields = source.getColumns().toArray(new String[0]);
		final String[] targetFields = target.getColumns() == null ? sourceFields
				: target.getColumns().toArray(new String[0]);
		final int[] targetPrecisions = new int[sourceFields.length];
		final int[] targetLengths = new int[targetFields.length];
		final SelectValuesMeta svi = new SelectValuesMeta();
		svi.setSelectLength(targetLengths);
		svi.setSelectPrecision(targetPrecisions);
		svi.setSelectName(sourceFields);
		svi.setSelectRename(targetFields);
		svi.setDeleteName(new String[0]);
		svi.setMeta(new SelectMetadataChange[0]);
		final StepMeta chose = new StepMeta("chose", svi);
		chose.setLocation(350, 100);
		chose.setDraw(true);
		chose.setDescription("STEP-CHOSE");
		transMeta.addStep(chose);
		transMeta.addTransHop(new TransHopMeta(query, chose));

		/*
		 * insert
		 */
		final TableOutputMeta toi = new TableOutputMeta();
		toi.setDatabaseMeta(targetDatabase);
		toi.setTableName(target.getTableName());
		toi.setCommitSize(100);
		toi.setTruncateTable(false);
		toi.setSpecifyFields(true);
		toi.setFieldDatabase(targetFields);
		toi.setFieldStream(targetFields);
		final StepMeta insert = new StepMeta("insert", toi);
		insert.setLocation(650, 100);
		insert.setDraw(true);
		insert.setDescription("STEP-INSERT");
		transMeta.addStep(insert);
		transMeta.addTransHop(new TransHopMeta(chose, insert));
		return transMeta;
	}

	public KettleJobEntireDefine createJob() throws KettleException {
		RepositoryDirectoryInterface directory = repositoryClient.getDirectory();
		KettleJobEntireDefine kettleJobEntireDefine = new KettleJobEntireDefine();
		TransMeta transMeta = createTrans();
		transMeta.setRepository(repositoryClient.getRepository());
		transMeta.setRepositoryDirectory(directory);
		kettleJobEntireDefine.getDependentTrans().add(transMeta);

		JobMeta mainJob = new JobMeta();
		mainJob.setRepository(repositoryClient.getRepository());
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
		// 当前目录,即job的同级目录
		trans.setDirectory("${Internal.Entry.Current.Directory}");
		trans.setTransname(transMeta.getName());
		JobEntryCopy excuter = new JobEntryCopy(trans);
		excuter.setLocation(300, 100);
		excuter.setDrawn(true);
		excuter.setDescription("MAINJOB");
		mainJob.addJobEntry(excuter);
		// 连接
		JobHopMeta hop = new JobHopMeta(start, excuter);
		mainJob.addJobHop(hop);
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
		kettleJobEntireDefine.setMainJob(mainJob);
		return kettleJobEntireDefine;
	}
}
