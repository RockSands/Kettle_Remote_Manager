package mine.demo;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.EnvUtil;

public class KettleDBPassword {

	public static void main(String[] args) throws KettleException {
		KettleEnvironment.init();
		EnvUtil.environmentInit();
		System.out.println("Encrypted " + org.pentaho.di.core.encryption.Encr.encryptPassword("123456"));
	}
}
