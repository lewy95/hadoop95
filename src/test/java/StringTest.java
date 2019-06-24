import org.junit.Test;

import java.util.*;

public class StringTest {

    @Test
    public void subStirng() {
        //String line = "zhaoliu@yahoo.com.cn";
        String line = "wangmazi@163.com";
        String result = line.substring(line.indexOf("@") + 1, line.indexOf("."));
        System.out.println(result);
    }

    @Test
    public void getBand() {
        //截取@和第一个.之间的部分，存入数组
        String[] emails = {"zhangsan@163.com", "zhaoliu@yahoo.com.cn", "wangmazi@163.com"};
        List<String> bands = new ArrayList<String>();
        String band;
        for (String email : emails) {
            band = email.substring(email.indexOf("@") + 1, email.indexOf("."));
            bands.add(band);
        }
        //遍历集合，将所有单词作为key，1作为value输出
        for (int i = 0; i < bands.size(); i++) {
            System.out.println(bands.get(i));
        }
    }

    @Test
    public void split() {
        String str = "F-E-K-O-P-";
        String[] slaves = str.split("-");
        for (String slave : slaves) {
            System.out.println(slave);
        }
    }

    @Test
    public void split2() {
        String str = "A:B,C,D,F,E,O";
        String[] tokens = str.split(":");
        String master = tokens[0];
        String[] slaves = tokens[1].split(",");
        for (String slave : slaves) {
            System.out.println(slave);
        }
    }

    @Test
    public void exchange() {
        int m = 156, n = 23;
        m = m ^ n;
        n = m ^ n;
        m = m ^ n;
        System.out.println("m:" + m + " ,n:" + n);
        //求反码
        System.out.println(~m);
    }

    @Test
    public void testListToString() {
        ArrayList<String> list = new ArrayList<String>();
        list.add("hhh");
        list.add("ddd");
        list.add("eee");
        list.add("rrr");
        System.out.println(list.toString());
    }

    @Test
    public void testA_Z() {
        for (int i = 1; i <= 26; i++) {
            if (i != 26) {
                System.out.print((char) (96 + i) + "-");
            } else {
                System.out.print((char) (96 + i));
            }
        }
    }

    @Test
    public void testSort() {
        int[] arr = new int[]{5, 6, 7, 1, 6, 2, 3};
        Arrays.sort(arr);
        for (int a : arr) {
            System.out.println(a);
        }
    }

    @Test
    public void testCount() {

        Map<String, Integer> map = new HashMap<String, Integer>();

        String str = "mm,oo,uu,oo,pp,mm,oo";
        String[] key = str.split(",");

        for (int i = 0; i < key.length; i++) {
            if (map.containsKey(key[i])) {
                map.put(key[i], map.get(key[i]) + 1);
            } else {
                map.put(key[i], 1);
            }
        }
        //遍历map
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }

    @Test
    public void testLine() {
        System.out.println("请输入值：");
        Scanner scanner = new Scanner(System.in);
        String para = scanner.next();
        int param = Integer.parseInt(para);
        int result = param * (param + 1) / 2;
        System.out.println(result);
    }

    @Test
    public void testQIOU() {
        int[] a = new int[]{1, 5, 4, 2, 6, 7, 6};
        int len = a.length;
        int i = 0, j = len - 1;
        int tmp = a[0];
        while (i < j) {
            while ((i < j) && a[j] % 2 == 0) {
                j--;
            }
            a[i] = a[j];
            while ((i < j) && a[i] % 2 != 0) {
                i++;
            }
            a[j] = a[i];
        }
        a[i] = tmp;
        System.out.println(Arrays.toString(a));
    }

    @Test
    public void testHashMap(){

        Map<String,Integer> map = new HashMap<String, Integer>();
        map.put("lewy",21);
        map.put("null",22);//有hashCode的值
        map.put(null,24);//不报错，但其没有hashCode值

        for (Map.Entry<String,Integer> entry: map.entrySet()) {
//            System.out.println("key:" + entry.getKey() + " value:" + entry.getValue());
            System.out.println(entry.hashCode());
            //24
            //3392913
            //3318286
        }
    }

    @Test
    public void testHashTable(){

        Map<String,Integer> map = new Hashtable<String, Integer>();
        map.put("lewy",21);
        map.put("muller",22);
        //map.put(null,23);//报错

        for (Map.Entry<String,Integer> entry: map.entrySet()) {
            System.out.println("key:" + entry.getKey() + " value:" + entry.getValue()) ;
        }
    }

    @Test
    public void testHashSet(){

        Set<String> set = new HashSet<String>();
        set.add("lewy");
        set.add("null");

        for (String s : set) {
            System.out.println(s) ;
        }
    }
}
