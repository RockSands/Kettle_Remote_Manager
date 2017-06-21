package mine.demo;

import java.io.IOException;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.repository.LongObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.update.UpdateMeta;

public class KettleTranMode {
	public static void main(String[] args) throws KettleException, IOException {

		KettleEnvironment.init();
		EnvUtil.environmentInit();
		Repository repository = new KettleDatabaseRepository();
		RepositoryMeta dbrepositoryMeta = new KettleDatabaseRepositoryMeta("KettleDBRepo", "KettleDBRepo",
				"Kettle DB Repository", new DatabaseMeta("kettleRepo", "MySQL", "Native", "192.168.80.138", "kettle",
						"3306", "root", "123456"));
		repository.init(dbrepositoryMeta);
		repository.connect("admin", "admin");
		TransMeta transMeta = repository.loadTransformation(new LongObjectId(1), null);
		/*
		 * 遍历Step
		 */
		for (StepMeta step : transMeta.getSteps()) {
			System.out.println(step.getName() + " - " + step.getClass().getName());
			if ("update".equals(step.getName())) {
				UpdateMeta um = (UpdateMeta)step.getStepMetaInterface();
				System.out.println(um);
			}
		}
	}
}
