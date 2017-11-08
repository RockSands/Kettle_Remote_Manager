package mine.demo;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryMeta;
import org.pentaho.di.repository.filerep.KettleFileRepository;
import org.pentaho.di.repository.filerep.KettleFileRepositoryMeta;

import com.kettle.core.repo.KettleRepositoryClient;

public class FileRepoMain {

	public static void main(String[] args) throws KettleException {
		KettleEnvironment.init();
		KettleFileRepository repository = new KettleFileRepository();
		RepositoryMeta dbrepositoryMeta = new KettleFileRepositoryMeta("KettleFileRepository", "KettleFileRepo",
				"File repository", "z://");
		repository.init(dbrepositoryMeta);
		repository.connect("admin", "admin");
		KettleRepositoryClient repositoryClient = new KettleRepositoryClient(repository);
		repositoryClient.disconnect();
	}

}
