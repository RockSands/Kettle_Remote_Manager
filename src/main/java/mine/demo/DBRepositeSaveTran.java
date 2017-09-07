package mine.demo;

import java.util.UUID;

import org.pentaho.di.cluster.SlaveServer;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.NotePadMeta;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransExecutionConfiguration;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.www.SlaveServerTransStatus;
import org.pentaho.di.www.WebResult;

/**
 * Kettle DBReposite保存转换
 * 
 * @author Administrator
 *
 */
public class DBRepositeSaveTran {

	public static void main(String[] args) throws Exception {
		/*
		 * 初始化
		 */
		KettleEnvironment.init();
		/*
		 * 资源库
		 */
		Repository repository = new KettleDatabaseRepository();
		RepositoryMeta dbrepositoryMeta = new KettleDatabaseRepositoryMeta("KettleDatabaseRepository", "DBRepository",
				"Kettle DB Repository",
				new DatabaseMeta("DBRepo", "MySQL", "Native", "192.168.80.138", "kettle", "3306", "root", "123456"));
		repository.init(dbrepositoryMeta);
		/*
		 * 资源路径
		 */
		repository.connect("admin", "admin");
		RepositoryDirectoryInterface repositoryDirectory = repository.findDirectory("");
		repository.disconnect();
		/*
		 * 转换保存
		 */
		repository.connect("admin", "admin");
		TransMeta transMeta = getTransMeta();
		transMeta.setRepositoryDirectory(repositoryDirectory);
		repository.save(transMeta, "ckw-20170810", null, null, true);
		repository.disconnect();
		/*
		 * 获取远程服务
		 */
		repository.connect("admin", "admin");
		SlaveServer remoteServer = null;
		for (SlaveServer server : repository.getSlaveServers()) {
			if (server.isMaster()) {
				remoteServer = server;
				break;
			}
		}
		repository.disconnect();
		/*
		 * 远程执行
		 */
		repository.connect("admin", "admin");
		remoteServer.getLogChannel().setLogLevel(LogLevel.ERROR);
		TransExecutionConfiguration transExecutionConfiguration = new TransExecutionConfiguration();
		transExecutionConfiguration.setRemoteServer(remoteServer);
		transExecutionConfiguration.setLogLevel(LogLevel.ERROR);
		Trans.sendToSlaveServer(transMeta, transExecutionConfiguration, repository, repository.getMetaStore());
		WebResult webresult = remoteServer.startTransformation(transMeta.getName(), null);
		System.out.println("=start=>" + webresult.getId());
		System.out.println("=start=>" + webresult.getMessage());
		System.out.println("=start=>" + webresult.getResult());// OK
		SlaveServerTransStatus slaveServerStatus = null;
		do {
			slaveServerStatus = remoteServer.getTransStatus(transMeta.getName(), null, 0);
			System.out.println("------------------------------");
			System.out.println("=message=>" + slaveServerStatus.getStatusDescription());
			Thread.sleep(5000);
		} while ("Running".equals(slaveServerStatus.getStatusDescription()));
		repository.disconnect();
	}

	public static TransMeta getTransMeta() {
		String uuid = UUID.randomUUID().toString().replace("-", "");
		// 创建一个转换
		TransMeta transMeta = new TransMeta();
		transMeta.setName("CKW-YHHX-" + uuid);

		/*
		 * 获取数据
		 */
		DatabaseMeta sourceDataBase = new DatabaseMeta("sourceDataBase", "MySQL", "Native", "192.168.80.138",
				"employees", "3306", "root", "123456");
		transMeta.addDatabase(sourceDataBase);

		DatabaseMeta targetDatabase = new DatabaseMeta("targetDatabase", "MySQL", "Native", "192.168.80.138", "person",
				"3306", "root", "123456");
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
		svi.setDeleteName(new String[0]);
		svi.setMeta(new SelectMetadataChange[0]);

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
		 * 增量
		 */
		String tostepname = "write to [target_employees]";
		InsertUpdateMeta ium = new InsertUpdateMeta();
		ium.setDatabaseMeta(targetDatabase);
		ium.setTableName("target_employees");
		ium.setCommitSize("100");
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
		return transMeta;
	}

}
