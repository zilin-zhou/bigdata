package org.example;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class Testing_Java {
}

class Test1{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String context = sc.nextLine();
        String[] words = context.split(" ");
        Map<String ,Integer> map = new HashMap<>();
        for(int i=0;i<words.length;i++){
            if(map.containsKey(words[i])){
                map.put(words[i],map.get(words[i])+1);
            }else{
                map.put(words[i],1);
            }
        }
        Iterator<Map.Entry<String,Integer>> it = map.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String,Integer> entry = it.next();
            System.out.println(entry.getKey()+","+entry.getValue());
        }
    }
}
class Test2{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int s1 = sc.nextInt();
        int s2 = sc.nextInt();
        int s3 = sc.nextInt();
        Tools.showAvg(s1,s2,s3);
    }
}
class Tools{
    public static double getAvg(int s1,int s2,int s3){
        return (s1+s2+s3)/(double)3;
    }
    public static void showAvg(int s1,int s2,int s3){
        System.out.println("你的平均分=" + getAvg(s1,s2,s3));
    }
}

class Test3{
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String school = sc.nextLine();
        Pupil p = new Pupil();
        p.setSchool(school);
        System.out.println("我的学校是" + p.getSchool());
        p.breathe();
        p.eat();
        p.sleep();
        p.think();
    }

}
interface Biology{
    void breathe();
}
interface Animal{

    void eat();
    void sleep();
}
class Person implements Biology,Animal{

    @Override
    public void breathe() {
        System.out.println("我喜欢呼吸新鲜空气");
    }

    @Override
    public void eat() {
        System.out.println("我会按时吃饭");
    }

    @Override
    public void sleep() {
        System.out.println("早睡早起身体好");
    }
    public void think(){
        System.out.println("我喜欢思考");
    }
}
class Pupil extends Person{
    private String school;

    public String getSchool() {
        return school;
    }

    public void setSchool(String school) {
        this.school = school;
    }

}
