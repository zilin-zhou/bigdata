package org.example;

public class Main {
    public static void main(String[] args) {
        Sub s = new Sub();
        s.a = 10;
        Parent p = s;
        p.a = 20;
        System.out.println(p.getA());
        System.out.println(s.getA());
        Object o;
    }
}