package com.kettle;

import java.util.Calendar;
import java.util.UUID;

import org.pentaho.di.cluster.ClusterSchema;
import org.pentaho.di.core.Condition;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
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

/**
 * Kettle数据迁移管理者
 * 
 * @author Administrator
 *
 */
public class KettleMgrInstance {

	private static KettleMgrInstance instance = null;

	private static Repository repository = null;

	private static RepositoryDirectoryInterface repositoryDirectory = null;

	private KettleMgrInstance() {

	}

	public KettleMgrInstance instance() throws KettleException {
		synchronized (instance) {
			if (instance == null) {
				instance = new KettleMgrInstance();
				kettleInit();
				return instance;
			}
			return instance;
		}
	}

	private void kettleInit() throws KettleException {
		KettleEnvironment.init();
		// EnvUtil.environmentInit();
		repository = new KettleDatabaseRepository();
		repositoryDirectory = repository.findDirectory("");
		RepositoryMeta dbrepositoryMeta = new KettleDatabaseRepositoryMeta("KettleDBRepo", "KettleDBRepo",
				"Kettle DB Repository", new DatabaseMeta("kettleRepo", "MySQL", "Native", "192.168.80.138", "kettle",
						"3306", "root", "123456"));
		repository.init(dbrepositoryMeta);
	}

	private TransMeta getTransMeta(String name) throws KettleException {
		synchronized (repository) {
			repository.connect("admin", "admin");
			try {
				ObjectId transformationID = repository.getTransformationID(name, repositoryDirectory);
				if (transformationID == null) {
					return null;
				}
				TransMeta transMeta = repository.loadTransformation(transformationID, null);
				return transMeta;
			} finally {
				repository.disconnect();
			}
		}
	}

	private void saveTransMeta(TransMeta transMeta) throws KettleException {
		synchronized (repository) {
			repository.connect("admin", "admin");
			try {
				transMeta.setRepositoryDirectory(repositoryDirectory);
				repository.save(transMeta, "1", Calendar.getInstance(), null, true);
			} finally {
				repository.disconnect();
			}
		}
	}

	private ClusterSchema getClusterSchem() throws KettleException {
		synchronized (repository) {
			repository.connect("admin", "admin");
			try {
				ObjectId clusterID = repository.getClusterID("YHHX-Cluster");
				return repository.loadClusterSchema(clusterID, null, null);
			} finally {
				repository.disconnect();
			}
		}
	}

	public String createDataTransfer() throws KettleException {
		String uuid = UUID.randomUUID().toString().replace("-", "");
		TransMeta transMeta = null;
		ClusterSchema clusterSchema = getClusterSchem();
		try {
			transMeta = new TransMeta();
			transMeta.setName("YHHX-" + uuid);
			DatabaseMeta sourceDataBase = new DatabaseMeta("source-" + uuid, "MySQL", "Native", "192.168.80.138",
					"employees", "3306", "root", "123456");
			transMeta.addDatabase(sourceDataBase);
			DatabaseMeta targetDatabase = new DatabaseMeta("target-" + uuid, "MySQL", "Native", "192.168.80.138",
					"person", "3306", "root", "123456");
			transMeta.addDatabase(targetDatabase);
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
			String selectSQL = "SELECT employees.emp_no, employees.first_name, employees.last_name, salaries.salary, salaries.from_date, salaries.to_date "
					+ "FROM employees, salaries "
					+ "WHERE salaries.emp_no = employees.emp_no AND salaries.emp_no = '10001' ORDER BY employees.emp_no, salaries.from_date;";
			tii.setSQL(selectSQL);
			StepMeta query = new StepMeta("source-" + uuid, tii);
			query.setLocation(150, 100);
			query.setDraw(true);
			query.setDescription("STEP-SOURCE-" + uuid);
			query.setClusterSchema(clusterSchema);
			transMeta.addStep(query);

			/*
			 * 转换名称
			 */
			String[] sourceFields = { "emp_no", "first_name", "last_name", "salary", "from_date", "to_date" };
			String[] targetFields = { "empID", "firstName", "lastName", "salary", "fromDate", "toDate" };
			int[] targetPrecisions = new int[sourceFields.length];
			int[] targetLengths = new int[targetFields.length];
			SelectValuesMeta svi = new SelectValuesMeta();
			svi.setSelectLength(targetLengths);
			svi.setSelectPrecision(targetPrecisions);
			svi.setSelectName(sourceFields);
			svi.setSelectRename(targetFields);
			StepMeta chose = new StepMeta("chose-" + uuid, svi);
			chose.setLocation(350, 100);
			chose.setDraw(true);
			chose.setDescription("STEP-CHOSE-" + uuid);
			chose.setClusterSchema(clusterSchema);
			transMeta.addStep(chose);
			transMeta.addTransHop(new TransHopMeta(query, chose));

			/*
			 * target
			 */
			TableInputMeta targettii = new TableInputMeta();
			targettii.setDatabaseMeta(targetDatabase);
			targettii.setSQL(
					"SELECT empID, firstName, lastName, salary, fromDate, toDate FROM targetSalary ORDER BY empID, fromDate");
			StepMeta targetQuery = new StepMeta("target-" + uuid, targettii);
			transMeta.addStep(targetQuery);
			targetQuery.setLocation(350, 300);
			targetQuery.setDraw(true);
			targetQuery.setDescription("STEP-TARGET-" + uuid);
			targetQuery.setClusterSchema(clusterSchema);

			/*
			 * merage
			 */
			MergeRowsMeta mrm = new MergeRowsMeta();
			mrm.setFlagField("flagfield");
			mrm.setValueFields(new String[] { "firstName", "lastName", "salary", "toDate" });
			mrm.setKeyFields(new String[] { "empID", "fromDate" });
			mrm.getStepIOMeta().setInfoSteps(new StepMeta[] { targetQuery, chose });
			StepMeta merage = new StepMeta("merage-" + uuid, mrm);
			transMeta.addStep(merage);
			merage.setLocation(650, 100);
			merage.setDraw(true);
			merage.setDescription("STEP-MERAGE-" + uuid);
			merage.setClusterSchema(clusterSchema);
			transMeta.addTransHop(new TransHopMeta(chose, merage));
			transMeta.addTransHop(new TransHopMeta(targetQuery, merage));

			/*
			 * noChange
			 */
			FilterRowsMeta frm_nochange = new FilterRowsMeta();
			frm_nochange.setCondition(new Condition("flagfield", Condition.FUNC_EQUAL, null,
					new ValueMetaAndData("constant", "identical")));
			StepMeta nochang = new StepMeta("nochang-" + uuid, frm_nochange);
			nochang.setLocation(950, 100);
			nochang.setDraw(true);
			nochang.setDescription("STEP-NOCHANGE-" + uuid);
			nochang.setClusterSchema(clusterSchema);
			transMeta.addStep(nochang);
			transMeta.addTransHop(new TransHopMeta(merage, nochang));
			/*
			 * nothing
			 */
			StepMeta nothing = new StepMeta("nothing-" + uuid, new DummyTransMeta());
			nothing.setLocation(950, 300);
			nothing.setDraw(true);
			nothing.setDescription("STEP-NOTHING-" + uuid);
			nothing.setClusterSchema(clusterSchema);
			transMeta.addStep(nothing);
			transMeta.addTransHop(new TransHopMeta(nochang, nothing));
			frm_nochange.getStepIOMeta().getTargetStreams().get(0).setStepMeta(nothing);
			/*
			 * isNew
			 */
			FilterRowsMeta frm_new = new FilterRowsMeta();
			frm_new.setCondition(
					new Condition("flagfield", Condition.FUNC_EQUAL, null, new ValueMetaAndData("constant", "new")));
			StepMeta isNew = new StepMeta("isNew-" + uuid, frm_new);
			isNew.setLocation(1250, 100);
			isNew.setDraw(true);
			isNew.setDescription("STEP-ISNEW" + uuid);
			isNew.setClusterSchema(clusterSchema);
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
			StepMeta insert = new StepMeta("insert-" + uuid, toi);
			insert.setLocation(1250, 300);
			insert.setDraw(true);
			insert.setDescription("STEP-INSERT" + uuid);
			insert.setClusterSchema(clusterSchema);
			transMeta.addStep(insert);
			transMeta.addTransHop(new TransHopMeta(isNew, insert));
			frm_new.getStepIOMeta().getTargetStreams().get(0).setStepMeta(insert);
			/*
			 * isChange
			 */
			FilterRowsMeta frm_isChange = new FilterRowsMeta();
			frm_isChange.setCondition(new Condition("flagfield", Condition.FUNC_EQUAL, null,
					new ValueMetaAndData("constant", "changed")));
			StepMeta isChange = new StepMeta("isChange-" + uuid, frm_isChange);
			isChange.setLocation(1550, 100);
			isChange.setDraw(true);
			isChange.setDescription("STEP-ISCHANGE" + uuid);
			isChange.setClusterSchema(clusterSchema);
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
			StepMeta update = new StepMeta("update-" + uuid, um);
			update.setLocation(1550, 300);
			update.setDraw(true);
			update.setDescription("STEP-UPDATE-" + uuid);
			update.setClusterSchema(clusterSchema);
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
			StepMeta delete = new StepMeta("delete-" + uuid, dm);
			delete.setLocation(1550, 300);
			delete.setDraw(true);
			delete.setDescription("STEP-DELETE-" + uuid);
			delete.setClusterSchema(clusterSchema);
			transMeta.addStep(delete);
			transMeta.addTransHop(new TransHopMeta(isChange, delete));
			frm_isChange.getStepIOMeta().getTargetStreams().get(1).setStepMeta(delete);
			/*
			 * 执行
			 */
			saveTransMeta(transMeta);
			Trans trans = new Trans(transMeta);
			trans.execute(null);
			return transMeta.getName();
		} catch (Exception e) {
			throw new KettleException();
		}
	}
}
