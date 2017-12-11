package com.kettle.main;

import com.kettle.core.instance.KettleMgrInstance;

/**
 * 测试
 * 
 * @author Administrator
 *
 */
public class KettleMgrInstanceMain {

	public static void main(String[] args) {
		KettleMgrInstance.getInstance();
//		try {
//			List<KettleResult> results = KettleMgrInstance.getInstance()
//					.queryJobs(Arrays.asList("1111", "88290ae44e124e71ac28869299424142",
//							"2a1588de144447988485c90a562e49c1"));
//			for (KettleResult result : results) {
//				System.out.println("====>");
//				System.out.println("====>" + result.getUuid());
//				System.out.println("====>" + result.getStatus());
//			}
//		} catch (KettleException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}

}
