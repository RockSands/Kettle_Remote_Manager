package mine.demo;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

/**
 * 创建一个转换,实现数据库迁移的功能
 * 
 * @author Administrator
 *
 */
public class DBUpdateOrInsertMain {
	public static void main(String[] args) throws Exception {
		// 手动设置KETTLE_HOME
		// System.getProperties().setProperty("KETTLE_HOME",
		// "/Kettle-5-Demo/src/main/resources");
		// 初始化环境
		KettleEnvironment.init();
		EnvUtil.environmentInit();
		try {
			// 创建一个转换
			TransMeta transMeta = new TransMeta();
			transMeta.setName("CKW-DBUpdateOrInsert-Test");

			/*
			 * 获取数据
			 */
			DatabaseMeta sourceDataBase = new DatabaseMeta("sourceDataBase", "MySQL", "Native", "192.168.80.138",
					"employees", "3306", "root", "123456");
			transMeta.addDatabase(sourceDataBase);

			DatabaseMeta targetDatabase = new DatabaseMeta("targetDatabase", "MySQL", "Native", "192.168.80.138",
					"person", "3306", "root", "123456");
			transMeta.addDatabase(targetDatabase);

			String note = "Reads information from table [sourceDataBase] on database [" + sourceDataBase + "]";
			note += "After that, it writes the information to table [person] on database [" + targetDatabase + "]";
			NotePadMeta ni = new NotePadMeta(note, 150, 10, -1, -1);
			transMeta.addNote(ni);

			String fromstepname = "read from [sourceDataBase]";
			TableInputMeta tii = new TableInputMeta();
			tii.setDatabaseMeta(sourceDataBase);
			String selectSQL = "SELECT employees.emp_no, dept_emp.dept_no, employees.first_name, employees.last_name, employees.birth_date "
					+ "FROM employees, dept_emp " + "WHERE employees.emp_no = dept_emp.emp_no AND first_name LIKE 'C%'";
			tii.setSQL(selectSQL);

			StepMeta fromstep = new StepMeta(fromstepname, (StepMetaInterface) tii);
			fromstep.setLocation(150, 100);
			fromstep.setDraw(true);
			fromstep.setDescription("Reads information on database [employees]");
			transMeta.addStep(fromstep);

			/*
			 * 转换名称
			 */
			String[] sourceFields = { "emp_no", "dept_no", "first_name", "last_name", "birth_date" };
			String[] targetFields = { "empID", "deptID", "firstName", "lastName", "born" };
			int[] targetPrecisions = { 0, 0, 0, 0, 0 };// 0 默认
			int[] targetLengths = { 0, 0, 0, 0, 0 };// 0 默认
			SelectValuesMeta svi = new SelectValuesMeta();
			svi.setSelectLength(targetLengths);
			svi.setSelectPrecision(targetPrecisions);
			svi.setSelectName(sourceFields);
			svi.setSelectRename(targetFields);

			String selstepname = "Rename field names";
			StepMeta selstep = new StepMeta(selstepname, svi);
			selstep.setLocation(350, 100);
			selstep.setDraw(true);
			selstep.setDescription("Rename field names");
			transMeta.addStep(selstep);

			TransHopMeta shi = new TransHopMeta(fromstep, selstep);
			transMeta.addTransHop(shi);
			fromstep = selstep;

			/*
			 * 全量
			 */
			// String tostepname = "write to [target_employees]";
			// TableOutputMeta toi = new TableOutputMeta();
			// toi.setDatabaseMeta(targetDatabase);
			// toi.setTableName("target_employees");
			// toi.setCommitSize(200);
			// toi.setTruncateTable(true);
			//
			// StepMeta tostep = new StepMeta(tostepname, (StepMetaInterface)
			// toi);
			// tostep.setLocation(550, 100);
			//
			// tostep.setDraw(true);
			// tostep.setDescription("Write information to table
			// [target_employees] on database [person]");
			// transMeta.addStep(tostep);
			//
			// TransHopMeta hi = new TransHopMeta(fromstep, tostep);
			// transMeta.addTransHop(hi);
			/*
			 * 增量
			 */
			String tostepname = "write to [target_employees]";
			InsertUpdateMeta ium = new InsertUpdateMeta();
			ium.setDatabaseMeta(targetDatabase);
			ium.setTableName("target_employees");
			ium.setCommitSize(100);
			ium.setChanged(true);
			ium.setKeyCondition(new String[] { "=", "=" });
			ium.setKeyLookup(new String[] { "empID", "deptID" });
			ium.setKeyStream(new String[] { "empID", "deptID" });
			ium.setKeyStream2(new String[2]);
			ium.setUpdateLookup(targetFields);
			ium.setUpdateStream(targetFields);
			ium.setUpdate(new Boolean[] { true, true, true, true, true });
			StepMeta tostep = new StepMeta(tostepname, ium);
			tostep.setLocation(550, 100);
			tostep.setDraw(true);
			tostep.setDescription("Write information to table [target_employees] on database [person]");
			TransHopMeta hi = new TransHopMeta(fromstep, tostep);
			transMeta.addStep(tostep);
			transMeta.addTransHop(hi);
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