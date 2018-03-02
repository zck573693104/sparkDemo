package spark;

import org.apache.hadoop.io.Text;

import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class TestMain {
    public static char[] randChar(int n) {
        char[] temp = new char[n];
        for (int i = 0; i < n; i++) {
            Random rd = new Random();
            temp[i] = (char) rd.nextInt(256);
        }

        return temp;
    }


    public static void main(String[] args) {
        String s = "13429100031     22554   8       2013-03-11 08:55:19.151754088   571     571     282     571\n";
        System.out.println(s.split(" +|\\t")[0]);

        System.out.println(s.substring(11,s.length()).trim());
        StringTokenizer itr = new StringTokenizer("A B C D E F".toString());
        Set<String> set = new TreeSet<String>();
        String owner = itr.nextToken();
        while (itr.hasMoreTokens()) {
            set.add(itr.nextToken());
        }
        System.out.println();
    }


}
