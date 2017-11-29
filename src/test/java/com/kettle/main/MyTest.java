package com.kettle.main;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MyTest {

	public static void main(String[] args) {
		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		try {
			while (true) {
				service.scheduleAtFixedRate(new Runnable() {
					@Override
					public void run() {
						System.out.println("====!=====");
						Thread.currentThread().stop();
						return;
					}
				}, 1, 2, TimeUnit.SECONDS);
				Thread.sleep(6000);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
