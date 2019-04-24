package com;

import com.alibaba.fastjson.JSONObject;

public class QuickSort {

    static void partition(int a[], int low, int high) {
        if (low >= high) return;
        int pivot = a[low], i = low, j = high;
        while (i < j) {
            while (i < j && a[j] > pivot) --j;
            if (i < j) a[i++] = a[j];
             System.out.println("a"+JSONObject.toJSON(a));
            while (i < j && a[i] <= pivot) ++i;
            if (i < j) a[j--] = a[i];
             System.out.println("b"+JSONObject.toJSON(a));
        }
        a[i] = pivot;
        partition(a, low, i - 1);
        partition(a, i + 1, high);
    }


    public static void main(String[] args) {
        int[] data = {2, 3, 1, 7, 5};
        partition(data, 0, 4);
        System.out.println(JSONObject.toJSON(data));
    }
}
