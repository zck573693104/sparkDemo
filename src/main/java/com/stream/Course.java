package com.stream;

import com.google.common.collect.ComparisonChain;
import lombok.Data;
import java.util.*;


@Data
public class Course {

    /**
     * 学号
     */
    private String studentNumber;

    /**
     * 姓名
     */
    private String studentName;

    /**
     * 分数
     */
    private int score;


    public static void main(String[] args) {
        int [] array = {1,2,10,9};
       // sort(array,1,3);
        quickSort_2(array,1,3);
        for (int a:array){
            System.out.println(a);
        }

    List<Course> courseArrayList = new ArrayList<>();
        Course course3 = new Course();
        course3.setScore(95);
        course3.setStudentName("Smith");
        course3.setStudentNumber("A");
       // coursesSet.add(course3);
        courseArrayList.add(course3);

        Course course1 = new Course();
        course1.setScore(90);
        course1.setStudentName("Frank");
        course1.setStudentNumber("B");
       // coursesSet.add(course1);
        courseArrayList.add(course1);

        Course course2 = new Course();
        course2.setScore(90);
        course2.setStudentName("Tom");
        course2.setStudentNumber("A");
        //coursesSet.add(course2);
        courseArrayList.add(course2);
        TreeSet<Course> coursesSet = new TreeSet<>(courseArrayList);
        Collections.sort(courseArrayList, new Comparator<Course>() {
            @Override
            public int compare(Course o1, Course o2) {
                 int result = ComparisonChain.start()
                .compare(o2.getScore(), o1.getScore())
                .compare(o1.getStudentNumber(),o2.getStudentNumber())
                .result();
        System.out.println(result);
        return result;
            }
        });
        // 编译时使用iterator实现
        for (Course course : coursesSet) {
            System.out.println(course);
        }
        for (Course course:courseArrayList){
            System.out.println(course);
        }
    }
        public static void quickSort_2(int[] data, int start, int end) {
        if (data == null || start >= end) return;
        int i = start, j = end;
        int pivotKey = data[start];
        while (i < j) {
            while (i < j && data[j] >= pivotKey) j--;
            if (i < j) data[i++] = data[j];
            while (i < j && data[i] <= pivotKey) i++;
            if (i < j) data[j--] = data[i];
        }
        data[i] = pivotKey;
        quickSort_2(data, start, i - 1);
        quickSort_2(data, i + 1, end);
    }

    public static void sort(int []array,int i,int j){
        int key = array[i];
        int start = i;
        int end = j;
        while (i<j){
            while (i<j&&array[j]>key){j--;}
            array[i++] = array[j];
            while (i<j&&array[i]<key){i++;}
            array[j--] = array[i];
        }
        array[i] = key;
        sort(array,start,i-1);
        sort(array,i+1,end);
    }
}