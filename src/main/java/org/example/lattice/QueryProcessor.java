package org.example.lattice;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class QueryProcessor {
    public static final String regex = ":_:";

    public static List<String> getLatticeDetails(Connection connection) {
        List<String> res = new ArrayList<>();

        try {
            String sql = "SELECT code, lattice_col, type FROM lattice_lookup";

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);


            while(rs.next()) {
                res.add(rs.getInt("code")
                        + regex + rs.getString("lattice_col")
                        + regex + rs.getString("type"));

            }
            return res;

        }
        catch(Exception e) {
            return res;
        }
    }

    public static List<String> getLatticeDetailsOfDimension(String dimensionName, Connection connection) {
        List<String> res = new ArrayList<>();
        try {
            String sql = "SELECT lattice_col, type, code FROM lattice_lookup WHERE dimension = ?";

            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, dimensionName);
            ResultSet rs = stmt.executeQuery();


            while(rs.next()) {
                res.add(rs.getString("lattice_col")
                        + regex + rs.getString("type")
                        + regex + rs.getString("code"));

            }
            return res;

        }
        catch (Exception e) {
            e.printStackTrace();
            return res;
        }
    }

    public static List<String> getFacts(Connection connection) {
        List<String> res = new ArrayList<>();

        try {
            String sql = "SELECT fact, agg FROM fact_agg";

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);


            while(rs.next()) {
                res.add(rs.getString("agg")
                        + regex + rs.getString("fact"));

            }
            return res;

        }
        catch(Exception e) {
            return res;
        }
    }

    public static List<String> getFactNames(Connection connection) {
        List<String> res = new ArrayList<>();

        try {
            String sql = "SELECT fact FROM fact_agg GROUP BY fact";

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);


            while(rs.next()) {
                res.add(rs.getString("fact"));

            }
            return res;

        }
        catch(Exception e) {
            return res;
        }
    }


}
