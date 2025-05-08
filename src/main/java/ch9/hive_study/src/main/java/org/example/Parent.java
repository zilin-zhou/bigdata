package org.example;

public class Parent {
    int a;
    public  int getA(){
        return this.a;
    }
}
class Sub extends Parent{
    int a = 10;
    @Override
    public int getA() {
        return this.a*10;
    }
}
class SubOther extends Parent{
    @Override
    public int getA() {
        return a*10;
    }
}
