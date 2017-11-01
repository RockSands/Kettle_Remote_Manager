package com.kettle.core.instance.metas;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.pentaho.di.core.Condition;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.filterrows.FilterRowsMeta;
import org.pentaho.di.trans.steps.mergerows.MergeRowsMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.sql.ExecSQLMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class CompareTablesDatas {
	public static TransMeta create(KettleTableMeta source, KettleTableMeta target, KettleTableMeta newOption)
			throws Exception {
		String uuid = UUID.randomUUID().toString().replace("-", "");
		Select sourceSelect = (Select) CCJSqlParserUtil.parse(source.getSql());
		Select targetSelect = (Select) CCJSqlParserUtil.parse(target.getSql());
		TransMeta transMeta = null;
		transMeta = new TransMeta();
		transMeta.setName("CTD-" + uuid);
		DatabaseMeta sourceDataBase = new DatabaseMeta(
				source.getHost() + "_" + source.getDatabase() + "_" + source.getUser(), source.getType(), "Native",
				source.getHost(), source.getDatabase(), source.getPort(), source.getUser(), source.getPasswd());
		transMeta.addDatabase(sourceDataBase);
		DatabaseMeta targetDatabase = new DatabaseMeta(
				target.getHost() + "_" + target.getDatabase() + "_" + target.getUser(), target.getType(), "Native",
				target.getHost(), target.getDatabase(), target.getPort(), target.getUser(), target.getPasswd());
		transMeta.addDatabase(targetDatabase);
		DatabaseMeta newDatabase = new DatabaseMeta(
				newOption.getHost() + "_" + newOption.getDatabase() + "_" + newOption.getUser(), newOption.getType(),
				"Native", newOption.getHost(), newOption.getDatabase(), newOption.getPort(), newOption.getUser(),
				newOption.getPasswd());
		transMeta.addDatabase(newDatabase);
		/*
		 * 获取非PK列
		 */
		// List<String> valueFields = new ArrayList<String>();
		// valueFields.addAll(target.getColumns());
		// valueFields.removeAll(target.getPkcolumns());
		// String[] valueColumns = valueFields.toArray(new String[0]);
		/*
		 * 获取PK列
		 */
		// String[] pkColumns = target.getPkcolumns().toArray(new String[0]);
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
		List<OrderByElement> selectOrderBy = ((PlainSelect) sourceSelect.getSelectBody()).getOrderByElements();
		if (selectOrderBy == null) {
			selectOrderBy = new LinkedList<OrderByElement>();
			((PlainSelect) sourceSelect.getSelectBody()).setOrderByElements(selectOrderBy);
		}
		selectOrderBy.clear();
		OrderByElement orderByElement = null;
		for (String pk : source.getPkcolumns()) {
			orderByElement = new OrderByElement();
			orderByElement.setExpression(new Column(pk));
			selectOrderBy.add(orderByElement);
		}
		tii.setSQL(sourceSelect.getSelectBody().toString());
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
		selectOrderBy = ((PlainSelect) targetSelect.getSelectBody()).getOrderByElements();
		if (selectOrderBy == null) {
			selectOrderBy = new LinkedList<OrderByElement>();
			((PlainSelect) targetSelect.getSelectBody()).setOrderByElements(selectOrderBy);
		}
		selectOrderBy.clear();
		for (String pk : target.getPkcolumns()) {
			orderByElement = new OrderByElement();
			orderByElement.setExpression(new Column(pk));
			selectOrderBy.add(orderByElement);
		}
		targettii.setSQL(targetSelect.getSelectBody().toString());
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
		mrm.setValueFields(new String[0]);
		mrm.setKeyFields(target.getPkcolumns().toArray(new String[0]));
		mrm.getStepIOMeta().setInfoSteps(new StepMeta[] { targetQuery, chose });
		StepMeta merage = new StepMeta("merage", mrm);
		transMeta.addStep(merage);
		merage.setLocation(650, 100);
		merage.setDraw(true);
		merage.setDescription("STEP-MERAGE");
		transMeta.addTransHop(new TransHopMeta(chose, merage));
		transMeta.addTransHop(new TransHopMeta(targetQuery, merage));
		/*
		 * isNew
		 */
		FilterRowsMeta frm_new = new FilterRowsMeta();
		frm_new.setCondition(
				new Condition("flagfield", Condition.FUNC_EQUAL, null, new ValueMetaAndData("constant", "new")));
		StepMeta isNew = new StepMeta("isNew", frm_new);
		isNew.setLocation(950, 100);
		isNew.setDraw(true);
		isNew.setDescription("STEP-ISNEW");
		transMeta.addStep(isNew);
		transMeta.addTransHop(new TransHopMeta(merage, isNew));
		/*
		 * insert
		 */
		ExecSQLMeta esm = new ExecSQLMeta();
		esm.setDatabaseMeta(newDatabase);
		esm.setSql(newOption.getSql());
		esm.setExecutedEachInputRow(true);
		esm.setVariableReplacementActive(true);
		esm.setParams(true);
		esm.setArguments(newOption.getColumns().toArray(new String[0]));
		StepMeta sqlExcute = new StepMeta("sqlExcute", esm);
		sqlExcute.setLocation(950, 300);
		sqlExcute.setDraw(true);
		sqlExcute.setDescription("STEP-SQLEXCUTE");
		transMeta.addStep(sqlExcute);
		transMeta.addTransHop(new TransHopMeta(isNew, sqlExcute));
		frm_new.getStepIOMeta().getTargetStreams().get(0).setStepMeta(sqlExcute);
		/*
		 * nothing
		 */
		StepMeta nothing = new StepMeta("nothing", new DummyTransMeta());
		nothing.setLocation(1250, 100);
		nothing.setDraw(true);
		nothing.setDescription("STEP-NOTHING");
		transMeta.addStep(nothing);
		transMeta.addTransHop(new TransHopMeta(isNew, nothing));
		frm_new.getStepIOMeta().getTargetStreams().get(1).setStepMeta(nothing);
		return transMeta;
	}
}
