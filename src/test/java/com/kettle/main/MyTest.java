package com.kettle.main;

import java.util.LinkedList;
import java.util.List;

public class MyTest {

	public static void main(String[] args) {
		// ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		// try {
		// while (true) {
		// service.scheduleAtFixedRate(new Runnable() {
		// @Override
		// public void run() {
		// System.out.println("====!=====");
		// Thread.currentThread().stop();
		// System.out.println("====4444=====");
		// }
		// }, 1, 2, TimeUnit.SECONDS);
		// Thread.sleep(6000);
		// }
		// } catch (Exception ex) {
		// ex.printStackTrace();
		// }
		List<String> arr = new LinkedList<String>();
		for (int i = 0; i < 10; i++) {
			arr.add("" + i);
		}
		for (int i = 0; i < arr.size(); i++) {
			if(arr.get(i).equals("5")){
				arr.remove(i);
				arr.add(i,null);
			}
		}
		System.out.println(arr);
	}

}
