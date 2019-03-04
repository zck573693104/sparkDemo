package com.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Test {
    public static void main(String []args){
        Random rand = new Random();
        System.out.println(rand.nextInt(10));
        List<Integer> testList = new ArrayList<>();
        testList.add(1);
        testList.add(2);
        testList.add(3);
        List<Integer> subList = testList.subList(1,2);
        subList.add(5);
        testList.add(6);
        for (Integer value:subList){
            System.out.println(value);
        }
        System.out.println("------");
        for (Integer value:testList){
            System.out.println(value);
        }
    }
}
