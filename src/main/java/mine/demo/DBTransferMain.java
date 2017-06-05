package mine.demo;

import org.pentaho.di.core.Condition;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.delete.DeleteMeta;
import org.pentaho.di.trans.steps.dummytrans.DummyTransMeta;
import org.pentaho.di.trans.steps.filterrows.FilterRowsMeta;
import org.pentaho.di.trans.steps.mergerows.MergeRowsMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
import org.pentaho.di.trans.steps.update.UpdateMeta;

public class DBTransferMain {

	public static void main(String[] args) throws KettleException {
		// 初始化环境
		KettleEnvironment.init();
		EnvUtil.environmentInit();
		try {
			// 创建一个转换
			TransMeta transMeta = new TransMeta();
			transMeta.setName("CKW-TransMeta-Test");
			// 源数据源
			DatabaseMeta sourceDataBase = new DatabaseMeta("sourceDataBase", "MySQL", "Native", "192.168.80.138",
					"employees", "3306", "root", "123456");
			transMeta.addDatabase(sourceDataBase);
			// 目标数据源
			DatabaseMeta targetDatabase = new DatabaseMeta("targetDatabase", "MySQL", "Native", "192.168.80.138",
					"person", "3306", "root", "123456");
			transMeta.addDatabase(targetDatabase);

			/*
			 * 日志输出
			 */
			String startNote = "Start CKW-TransMeta-Test";
			NotePadMeta ni = new NotePadMeta(startNote, 150, 10, -1, -1);
			transMeta.addNote(ni);

			/*
			 * 源表输入
			 */
			TableInputMeta tii = new TableInputMeta();
			tii.setDatabaseMeta(sourceDataBase);
			String selectSQL = "SELECT employees.emp_no, employees.first_name, employees.last_name, salaries.salary, salaries.from_date, salaries.to_date "
					+ "FROM employees, salaries "
					+ "WHERE salaries.emp_no = employees.emp_no AND salaries.emp_no = '10001' ORDER BY employees.emp_no, salaries.from_date;";
			tii.setSQL(selectSQL);
			StepMeta query = new StepMeta("query", tii);
			query.setLocation(150, 100);
			query.setDraw(true);
			query.setDescription("STEP-query");
			transMeta.addStep(query);

			/*
			 * 转换名称
			 */
			String[] sourceFields = { "emp_no", "first_name", "last_name", "salary", "from_date", "to_date" };
			String[] targetFields = { "empID", "firstName", "lastName", "salary", "fromDate", "toDate" };
			int[] targetPrecisions = { 0, 0, 0, 0, 0, 0 };
			int[] targetLengths = { 0, 0, 0, 0, 0, 0 };
			SelectValuesMeta svi = new SelectValuesMeta();
			svi.setSelectLength(targetLengths);
			svi.setSelectPrecision(targetPrecisions);
			svi.setSelectName(sourceFields);
			svi.setSelectRename(targetFields);
			StepMeta chose = new StepMeta("chose", svi);
			chose.setLocation(350, 100);
			chose.setDraw(true);
			chose.setDescription("STEP-chose");
			transMeta.addStep(chose);
			transMeta.addTransHop(new TransHopMeta(query, chose));

			/*
			 * 目标表输入
			 */
			TableInputMeta targettii = new TableInputMeta();
			targettii.setDatabaseMeta(targetDatabase);
			targettii.setSQL(
					"SELECT empID, firstName, lastName, salary, fromDate, toDate FROM targetSalary ORDER BY empID, fromDate");
			StepMeta targetQuery = new StepMeta("targetQuery", targettii);
			transMeta.addStep(targetQuery);
			chose.setLocation(350, 300);
			chose.setDraw(true);
			chose.setDescription("STEP-targetQuery");

			/*
			 * 合并数据
			 */
			MergeRowsMeta mrm = new MergeRowsMeta();
			mrm.setFlagField("flagfield");
			mrm.setValueFields(new String[] { "firstName", "lastName", "salary", "toDate" });
			mrm.setKeyFields(new String[] { "empID", "fromDate" });
			mrm.getStepIOMeta().setInfoSteps(new StepMeta[] { targetQuery, chose });
			StepMeta merage = new StepMeta("merage", mrm);
			transMeta.addStep(merage);
			merage.setLocation(650, 100);
			merage.setDraw(true);
			merage.setDescription("STEP-merage");
			transMeta.addTransHop(new TransHopMeta(chose, merage));
			transMeta.addTransHop(new TransHopMeta(targetQuery, merage));

			/*
			 * noChange判断
			 */
			FilterRowsMeta frm_nochange = new FilterRowsMeta();
			frm_nochange.setCondition(new Condition("flagfield", Condition.FUNC_EQUAL, null,
					new ValueMetaAndData("constant", "identical")));
			StepMeta nochang = new StepMeta("nochang", frm_nochange);
			nochang.setLocation(950, 100);
			nochang.setDraw(true);
			nochang.setDescription("STEP-nochang");
			transMeta.addStep(nochang);
			transMeta.addTransHop(new TransHopMeta(merage, nochang));
			/*
			 * nothing
			 */
			StepMeta nothing = new StepMeta("nothing", new DummyTransMeta());
			nothing.setLocation(950, 300);
			nothing.setDraw(true);
			nothing.setDescription("STEP-nothing");
			transMeta.addStep(nothing);
			transMeta.addTransHop(new TransHopMeta(nochang, nothing));
			frm_nochange.getStepIOMeta().getTargetStreams().get(0).setStepMeta(nothing);
			/*
			 * isNew判断
			 */
			FilterRowsMeta frm_new = new FilterRowsMeta();
			frm_new.setCondition(
					new Condition("flagfield", Condition.FUNC_EQUAL, null, new ValueMetaAndData("constant", "new")));
			StepMeta isNew = new StepMeta("isNew", frm_new);
			isNew.setLocation(1250, 100);
			isNew.setDraw(true);
			isNew.setDescription("STEP-isNew");
			transMeta.addStep(isNew);
			transMeta.addTransHop(new TransHopMeta(nochang, isNew));
			frm_nochange.getStepIOMeta().getTargetStreams().get(1).setStepMeta(isNew);
			/*
			 * insert
			 */
			TableOutputMeta toi = new TableOutputMeta();
			toi.setDatabaseMeta(targetDatabase);
			toi.setTableName("targetSalary");
			toi.setCommitSize(100);
			toi.setTruncateTable(false);
			toi.setSpecifyFields(true);
			toi.setFieldDatabase(targetFields);
			toi.setFieldStream(targetFields);
			StepMeta insert = new StepMeta("insert", toi);
			insert.setLocation(1250, 300);
			insert.setDraw(true);
			insert.setDescription("STEP-insert");
			transMeta.addStep(insert);
			transMeta.addTransHop(new TransHopMeta(isNew, insert));
			frm_new.getStepIOMeta().getTargetStreams().get(0).setStepMeta(insert);
			System.out.println(toi.getXML());
			/*
			 * isChange判断
			 */
			FilterRowsMeta frm_isChange = new FilterRowsMeta();
			frm_isChange.setCondition(new Condition("flagfield", Condition.FUNC_EQUAL, null,
					new ValueMetaAndData("constant", "changed")));
			StepMeta isChange = new StepMeta("isChange", frm_isChange);
			isChange.setLocation(1550, 100);
			isChange.setDraw(true);
			isChange.setDescription("STEP-isChange");
			transMeta.addStep(isChange);
			transMeta.addTransHop(new TransHopMeta(isNew, isChange));
			frm_new.getStepIOMeta().getTargetStreams().get(1).setStepMeta(isChange);
			/*
			 * update
			 */
			UpdateMeta um = new UpdateMeta();
			um.setDatabaseMeta(targetDatabase);
			um.setTableName("targetSalary");
			um.setCommitSize(100);
			um.setKeyCondition(new String[] { "=", "=" });
			um.setKeyStream2(new String[] { null, null });
			um.setKeyStream(new String[] { "empID", "fromDate" });
			um.setUseBatchUpdate(true);
			um.setUpdateLookup(targetFields);
			StepMeta update = new StepMeta("update", um);
			update.setLocation(1550, 300);
			update.setDraw(true);
			update.setDescription("STEP-update");
			transMeta.addStep(update);
			transMeta.addTransHop(new TransHopMeta(isChange, update));
			frm_isChange.getStepIOMeta().getTargetStreams().get(0).setStepMeta(update);
			/*
			 * delete
			 */
			DeleteMeta dm = new DeleteMeta();
			dm.setDatabaseMeta(targetDatabase);
			dm.setTableName("targetSalary");
			dm.setCommitSize(100);
			dm.setKeyCondition(new String[] { "=", "=" });
			dm.setKeyLookup(new String[] { "empID", "fromDate" });
			dm.setKeyStream2(new String[] { null, null });
			dm.setKeyStream(new String[] { "empID", "fromDate" });
			StepMeta delete = new StepMeta("delete", dm);
			delete.setLocation(1550, 300);
			delete.setDraw(true);
			delete.setDescription("STEP-delete");
			transMeta.addStep(delete);
			transMeta.addTransHop(new TransHopMeta(isNew, delete));
			frm_isChange.getStepIOMeta().getTargetStreams().get(1).setStepMeta(delete);
			/*
			 * 执行
			 */
			Trans trans = new Trans(transMeta);
			trans.execute(null);
			trans.waitUntilFinished();
			// 转换构建完成
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
