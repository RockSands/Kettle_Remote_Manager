package com.kettle;

import java.util.Arrays;

import org.pentaho.di.www.SlaveServerTransStatus;

public class ExcuteMain {
	/**
	 * 需求: 将源数据库(jdbc:mysql://192.168.80.138:3306/employees 用户名:root 密码: 123456)
	 * 的SQL SELECT employees.emp_no, dept_emp.dept_no, employees.first_name,
	 * employees.last_name, employees.birth_date FROM employees, dept_emp WHERE
	 * employees.emp_no = dept_emp.emp_no AND first_name LIKE 'C%'
	 * 导入到目标数据库(jdbc:mysql://192.168.80.138:3306/person 用户名:root 密码:
	 * 123456)的表targetSalary("empID", "deptID", "firstName", "lastName", "born"
	 * 主键为"empID"与"deptID")
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * Source的TableName无效
		 */
		KettleDBTranDescribe source = new KettleDBTranDescribe();
		source.setType("MySQL");
		source.setHost("192.168.80.138");
		source.setPort("3306");
		source.setDatabase("employees");
		source.setUser("root");
		source.setPasswd("123456");
		source.setSql(
				"SELECT employees.emp_no, dept_emp.dept_no, employees.first_name, employees.last_name, employees.birth_date FROM employees, dept_emp WHERE employees.emp_no = dept_emp.emp_no AND first_name LIKE 'C%'");
		source.setColumns(Arrays.asList("emp_no", "dept_no", "first_name", "last_name", "birth_date"));
		source.setPkcolumns(Arrays.asList("emp_no", "dept_no"));

		KettleDBTranDescribe target = new KettleDBTranDescribe();
		target.setType("MySQL");
		target.setHost("192.168.80.138");
		target.setPort("3306");
		target.setDatabase("person");
		target.setUser("root");
		target.setPasswd("123456");
		target.setColumns(Arrays.asList("empID", "deptID", "firstName", "lastName", "born"));
		target.setPkcolumns(Arrays.asList("empID", "deptID"));
		target.setSql(
				"SELECT empID, firstName, lastName, salary, fromDate, toDate FROM targetSalary ORDER BY empID, fromDate");
		target.setTableName("targetSalary");
		/*
		 * 
		 */
		String id = KettleMgrInstance.getInstance().createDataTransfer(source, target);
		SlaveServerTransStatus slaveServerTransStatus = null;
		do {// 睡1分钟
			Thread.sleep(6000);
			slaveServerTransStatus = KettleMgrInstance.getInstance().fetchTransStatus(id);
			System.out.println("------------------------------");
			System.out.println("=status:wait=>" + slaveServerTransStatus.isWaiting());// 执行成功后
			System.out.println("=status:wait=>" + slaveServerTransStatus.isPaused());// 手动暂停
			System.out.println("=status:wait=>" + slaveServerTransStatus.isStopped());// 异常标识停止
			System.out.println("=status:ErrMsg=>" + slaveServerTransStatus.getErrorDescription());// 异常消息
		} while (slaveServerTransStatus.isRunning());
	}

}
