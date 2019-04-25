package com.calcuate;

import java.util.Map;

public class Dynamic {

    public int testGetFloorWays(int n){
        if (n == 1){
            return 1;
        }
        if (n == 2){
            return 2;
        }
        int a = 1;
        int b = 2;
        int temp = 0;
        for (int i =3; i < n; i++){
            temp = a+b;
            b = temp;
            a = b;
        }
        return temp;
    }
}
