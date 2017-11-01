package com.kettle.core.instance.metas;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

public class TableDataMigration {
	public static TransMeta createTableDataMigration(KettleTableMeta source, KettleTableMeta target) throws KettleException {
		final String uuid = UUID.randomUUID().toString().replace("-", "");
		TransMeta transMeta = null;
		transMeta = new TransMeta();
		transMeta.setName("TDM-" + uuid);
		final DatabaseMeta sourceDataBase = new DatabaseMeta(
				source.getHost() + "_" + source.getDatabase() + "_" + source.getUser(), source.getType(), "Native",
				source.getHost(), source.getDatabase(), source.getPort(), source.getUser(), source.getPasswd());
		sourceDataBase.setInitialPoolSize(10);
		sourceDataBase.setMaximumPoolSize(20);
		sourceDataBase.setUsingConnectionPool(true);
		transMeta.addDatabase(sourceDataBase);
		final DatabaseMeta targetDatabase = new DatabaseMeta(
				target.getHost() + "_" + target.getDatabase() + "_" + target.getUser(), target.getType(), "Native",
				target.getHost(), target.getDatabase(), target.getPort(), target.getUser(), target.getPasswd());
		targetDatabase.setInitialPoolSize(10);
		targetDatabase.setMaximumPoolSize(20);
		targetDatabase.setUsingConnectionPool(true);
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
}
