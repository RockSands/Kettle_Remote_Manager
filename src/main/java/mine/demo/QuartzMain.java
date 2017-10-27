package mine.demo;

import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzMain {

	public static void main(String[] args) throws SchedulerException {
		try {

			// 通过schedulerFactory获取一个调度器
			SchedulerFactory schedulerfactory = new StdSchedulerFactory();

			// 通过schedulerFactory获取一个调度器
			Scheduler scheduler = schedulerfactory.getScheduler();

			// 创建jobDetail实例，绑定Job实现类
			JobDataMap map = new JobDataMap();
			map.put("record", "5");
			JobDetail jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).setJobData(map).build();

			// 定义调度触发规则，本例中使用SimpleScheduleBuilder创建了一个5s执行一次的触发器
			Trigger trigger = TriggerBuilder.newTrigger().startNow()
					.withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?")).build();
			// 启动调度
			scheduler.start();

			// 把作业和触发器注册到任务调度中
			scheduler.scheduleJob(jobDetail, trigger);
			//
			trigger = TriggerBuilder.newTrigger().startNow()
					.withSchedule(CronScheduleBuilder.cronSchedule("0/2 * * * * ?")).build();

			map.put("record", "2");
			jobDetail = JobBuilder.newJob(RecordSchedulerJob.class).setJobData(map).build();
			scheduler.scheduleJob(jobDetail, trigger);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 定时调度,将record放入优先队列
	 * 
	 * @author Administrator
	 */
	public static class RecordSchedulerJob implements Job {
		@Override
		public void execute(JobExecutionContext arg0) throws JobExecutionException {
			System.out.println("==" + arg0.getJobDetail().getJobDataMap().get("record") + "==");
		}
	}
}
