package com.kettle.core.instance.metas.builder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.pentaho.di.core.Condition;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.job.JobHopMeta;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.special.JobEntrySpecial;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.delete.DeleteMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.filterrows.FilterRowsMeta;
import org.pentaho.di.trans.steps.mergerows.MergeRowsMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.trans.steps.update.UpdateMeta;

import com.kettle.core.bean.KettleJobEntireDefine;
import com.kettle.core.instance.KettleMgrInstance;
import com.kettle.core.instance.metas.KettleTableMeta;
import com.kettle.core.repo.KettleRepositoryClient;

public class SyncTablesDatasBuilder {

	/**
	 * 资源链接
	 */
	private final KettleRepositoryClient repositoryClient = KettleMgrInstance.kettleMgrEnvironment
			.getRepositoryClient();
	/**
	 * 源
	 */
	private KettleTableMeta source;
	/**
	 * 目标
	 */
	private KettleTableMeta target;

	public SyncTablesDatasBuilder source(KettleTableMeta source) {
		this.source = source;
		return this;
	}

	public SyncTablesDatasBuilder target(KettleTableMeta target) {
		this.target = target;
		return this;
	}

	private TransMeta createTrans() throws KettleException {
		String uuid = UUID.randomUUID().toString().replace("-", "");
		TransMeta transMeta = null;
		transMeta = new TransMeta();
		transMeta.setName("SYNCT-" + uuid);
		DatabaseMeta sourceDataBase = new DatabaseMeta(
				source.getHost() + "_" + source.getDatabase() + "_" + source.getUser(), source.getType(), "Native",
				source.getHost(), source.getDatabase(), source.getPort(), source.getUser(), source.getPasswd());
		transMeta.addDatabase(sourceDataBase);
		DatabaseMeta targetDatabase = new DatabaseMeta(
				target.getHost() + "_" + target.getDatabase() + "_" + target.getUser(), target.getType(), "Native",
				target.getHost(), target.getDatabase(), target.getPort(), target.getUser(), target.getPasswd());
		transMeta.addDatabase(targetDatabase);
		/*
		 * 获取非PK列
		 */
		String[] allColumns = target.getColumns().toArray(new String[0]);
		/*
		 * 获取非PK列
		 */
		List<String> valueFields = new ArrayList<String>();
		valueFields.addAll(target.getColumns());
		valueFields.removeAll(target.getPkcolumns());
		String[] valueColumns = valueFields.toArray(new String[0]);
		/*
		 * 获取PK列
		 */
		String[] pkColumns = target.getPkcolumns().toArray(new String[0]);
		/*
		 * 条件
		 */
		String[] conditions = new String[target.getPkcolumns().size()];
		for (int i = 0; i < conditions.length; i++) {
			conditions[i] = "=";
		}
		/*
		 * Note
		 */
		String startNote = "Start " + transMeta.getName();
		NotePadMeta ni = new NotePadMeta(startNote, 150, 10, -1, -1);
		transMeta.addNote(ni);
		/*
		 * source
		 */
		TableInputMeta tii = new TableInputMeta();
		tii.setDatabaseMeta(sourceDataBase);
		String selectSQL = source.getSql();
		tii.setSQL(selectSQL);
		StepMeta query = new StepMeta("source", tii);
		query.setLocation(150, 100);
		query.setDraw(true);
		query.setDescription("STEP-SOURCE");
		transMeta.addStep(query);

		/*
		 * 转换名称
		 */
		String[] sourceFields = source.getColumns().toArray(new String[0]);
		String[] targetFields = target.getColumns() == null ? sourceFields : target.getColumns().toArray(new String[0]);
		int[] targetPrecisions = new int[sourceFields.length];
		int[] targetLengths = new int[targetFields.length];
		SelectValuesMeta svi = new SelectValuesMeta();
		svi.setSelectLength(targetLengths);
		svi.setSelectPrecision(targetPrecisions);
		svi.setSelectName(sourceFields);
		svi.setSelectRename(targetFields);
		svi.setDeleteName(new String[0]);
		svi.setMeta(new SelectMetadataChange[0]);
		StepMeta chose = new StepMeta("chose", svi);
		chose.setLocation(350, 100);
		chose.setDraw(true);
		chose.setDescription("STEP-CHOSE");
		transMeta.addStep(chose);
		transMeta.addTransHop(new TransHopMeta(query, chose));

		/*
		 * target
		 */
		TableInputMeta targettii = new TableInputMeta();
		targettii.setDatabaseMeta(targetDatabase);
		targettii.setSQL(target.getSql());
		StepMeta targetQuery = new StepMeta("target", targettii);
		transMeta.addStep(targetQuery);
		targetQuery.setLocation(350, 300);
		targetQuery.setDraw(true);
		targetQuery.setDescription("STEP-TARGET");

		/*
		 * merage
		 */
		MergeRowsMeta mrm = new MergeRowsMeta();
		mrm.setFlagField("flagfield");
		mrm.setValueFields(valueColumns);
		mrm.setKeyFields(pkColumns);
		mrm.getStepIOMeta().setInfoSteps(new StepMeta[] { targetQuery, chose });
		StepMeta merage = new StepMeta("merage", mrm);
		transMeta.addStep(merage);
		merage.setLocation(650, 100);
		merage.setDraw(true);
		merage.setDescription("STEP-MERAGE");
		transMeta.addTransHop(new TransHopMeta(chose, merage));
		transMeta.addTransHop(new TransHopMeta(targetQuery, merage));

		/*
		 * noChange
		 */
		FilterRowsMeta frm_nochange = new FilterRowsMeta();
		frm_nochange.setCondition(
				new Condition("flagfield", Condition.FUNC_EQUAL, null, new ValueMetaAndData("constant", "identical")));
		StepMeta nochang = new StepMeta("nochang", frm_nochange);
		nochang.setLocation(950, 100);
		nochang.setDraw(true);
		nochang.setDescription("STEP-NOCHANGE");
		transMeta.addStep(nochang);
		transMeta.addTransHop(new TransHopMeta(merage, nochang));
		/*
		 * nothing
		 */
		StepMeta nothing = new StepMeta("nothing", new DummyTransMeta());
		nothing.setLocation(950, 300);
		nothing.setDraw(true);
		nothing.setDescription("STEP-NOTHING");
		transMeta.addStep(nothing);
		transMeta.addTransHop(new TransHopMeta(nochang, nothing));
		frm_nochange.getStepIOMeta().getTargetStreams().get(0).setStepMeta(nothing);
		/*
		 * isNew
		 */
		FilterRowsMeta frm_new = new FilterRowsMeta();
		frm_new.setCondition(
				new Condition("flagfield", Condition.FUNC_EQUAL, null, new ValueMetaAndData("constant", "new")));
		StepMeta isNew = new StepMeta("isNew", frm_new);
		isNew.setLocation(1250, 100);
		isNew.setDraw(true);
		isNew.setDescription("STEP-ISNEW");
		transMeta.addStep(isNew);
		transMeta.addTransHop(new TransHopMeta(nochang, isNew));
		frm_nochange.getStepIOMeta().getTargetStreams().get(1).setStepMeta(isNew);
		/*
		 * insert
		 */
		TableOutputMeta toi = new TableOutputMeta();
		toi.setDatabaseMeta(targetDatabase);
		toi.setTableName(target.getTableName());
		toi.setCommitSize(100);
		toi.setTruncateTable(false);
		toi.setSpecifyFields(true);
		toi.setFieldDatabase(targetFields);
		toi.setFieldStream(targetFields);
		StepMeta insert = new StepMeta("insert", toi);
		insert.setLocation(1250, 300);
		insert.setDraw(true);
		insert.setDescription("STEP-INSERT");
		transMeta.addStep(insert);
		transMeta.addTransHop(new TransHopMeta(isNew, insert));
		frm_new.getStepIOMeta().getTargetStreams().get(0).setStepMeta(insert);
		/*
		 * isChange
		 */
		FilterRowsMeta frm_isChange = new FilterRowsMeta();
		frm_isChange.setCondition(
				new Condition("flagfield", Condition.FUNC_EQUAL, null, new ValueMetaAndData("constant", "changed")));
		StepMeta isChange = new StepMeta("isChange", frm_isChange);
		isChange.setLocation(1550, 100);
		isChange.setDraw(true);
		isChange.setDescription("STEP-ISCHANGE");
		transMeta.addStep(isChange);
		transMeta.addTransHop(new TransHopMeta(isNew, isChange));
		frm_new.getStepIOMeta().getTargetStreams().get(1).setStepMeta(isChange);
		/*
		 * update
		 */
		UpdateMeta um = new UpdateMeta();
		um.setDatabaseMeta(targetDatabase);
		um.setUseBatchUpdate(true);
		um.setTableName(target.getTableName());
		um.setCommitSize("100");
		um.setKeyLookup(pkColumns);
		um.setKeyStream(pkColumns);
		um.setKeyCondition(conditions);
		um.setKeyStream2(new String[pkColumns.length]);
		um.setUseBatchUpdate(true);
		um.setUpdateLookup(allColumns);
		um.setUpdateStream(allColumns);

		StepMeta update = new StepMeta("update", um);
		update.setLocation(1850, 300);
		update.setDraw(true);
		update.setDescription("STEP-UPDATE");
		transMeta.addStep(update);
		transMeta.addTransHop(new TransHopMeta(isChange, update));
		frm_isChange.getStepIOMeta().getTargetStreams().get(0).setStepMeta(update);
		/*
		 * delete
		 */
		DeleteMeta dm = new DeleteMeta();
		dm.setDatabaseMeta(targetDatabase);
		dm.setTableName(target.getTableName());
		dm.setCommitSize("100");
		dm.setKeyCondition(conditions);
		dm.setKeyLookup(pkColumns);
		dm.setKeyStream2(new String[target.getPkcolumns().size()]);
		dm.setKeyStream(pkColumns);
		StepMeta delete = new StepMeta("delete", dm);
		delete.setLocation(1550, 300);
		delete.setDraw(true);
		delete.setDescription("STEP-DELETE");
		transMeta.addStep(delete);
		transMeta.addTransHop(new TransHopMeta(isChange, delete));
		frm_isChange.getStepIOMeta().getTargetStreams().get(1).setStepMeta(delete);
		return transMeta;
	}

	public KettleJobEntireDefine createJob() throws Exception {
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
		kettleJobEntireDefine.setMainJob(mainJob);
		return kettleJobEntireDefine;
	}
}
