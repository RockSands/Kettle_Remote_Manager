package com.kettle.main;

import java.util.Random;

public class MyTest {

	public static void main(String[] args) {
		Random random0 = new Random();
		Random random1 = new Random();
		for(int i=0;i<20;i++){
			System.out.print(random0.nextInt(2));
			System.out.println("," + random1.nextInt(2));
		}
	}

}
