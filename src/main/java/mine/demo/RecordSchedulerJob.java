package mine.demo;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class RecordSchedulerJob implements Job {
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		System.out.println("==" + arg0.getJobDetail().getJobDataMap().get("record") + "==");
	}
}
