package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public abstract interface X {
}
class A{
    public static void main(String[] args) throws Exception {
        String username="test",password="123456";
        Class.forName("                 ");
        Connection conn = DriverManager.getConnection(
                "        ://localhost:3306/student",
                "root","");
        String sql = "select * from students where username=? and password=?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1,username);
        ps.setString(2,password);
        ResultSet rs = ps.executeQuery();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}