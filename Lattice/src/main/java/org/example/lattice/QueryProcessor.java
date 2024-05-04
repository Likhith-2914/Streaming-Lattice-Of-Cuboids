package org.example.lattice;

import org.springframework.beans.factory.annotation.Autowired;

import java.sql.*;
import java.util.*;


public class QueryProcessor {
    public static final String regex = ":_:";
    private static final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PWD = "mysql";


    public List<Map<String, Object>> selectTables(String dbName, Map<String, Boolean> cols) {
        List<Map<String, Object>> table = new ArrayList<>();
        try{

            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);

            List<String> lattice_columns = getLatticeDetails(connection);

            List<String> selectedCols = new ArrayList<>();
            for(String col: lattice_columns) {
                String[] col_details = col.split(regex);
                if(cols.get(col_details[1])) selectedCols.add(col_details[0]);
            }


            StringBuilder tableName = new StringBuilder("lattice_");
            if(selectedCols.isEmpty()) tableName.append("apex");
            else {
                for (String colCode : selectedCols) {
                    tableName.append(colCode).append("_");
                }
                tableName.append("facts");
            }

            String sql = "SELECT * FROM " + tableName;

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            int colCount = resultSetMetaData.getColumnCount();

            while(rs.next()) {
                Map<String, Object> row = new HashMap<>();

                for (int i = 1; i <= colCount; i++) {
                    String columnName = resultSetMetaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    if(!(tableName.toString().equals("lattice_apex")
                            && columnName.equals("id"))) row.put(columnName, columnValue);
                }

                table.add(row);
            }

            return table;

        }
        catch (Exception e) {
            e.printStackTrace();
            return new ArrayList<>();
        }

    }

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

    public static List<String> getLatticeDetailsOfDimension(Connection connection, String dimensionName) {
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

    public static Map<String, Object> getStreamProperty(Connection connection, String property) {
        Map<String, Object> map = new HashMap<>();

        try {
            String sql = "SELECT type, count, units FROM streaming_properties WHERE property = ?";

            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, property);
            ResultSet rs = stmt.executeQuery();

            while(rs.next()) {
                map.put("type", rs.getString("type"));
                map.put("count", rs.getInt("count"));
                map.put("units", rs.getString("units"));
            }
            return map;

        }
        catch(Exception e) {
            return map;
        }
    }

    public static int getDataLoaderRowCount(String dbName) {

        String sql = "SELECT COUNT(*) AS row_count FROM data_loader";

        try {
            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);

             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
               return rs.getInt("row_count");
            }
            return -1;

        } catch (Exception e) {
            e.printStackTrace();
            return -1;
        }
    }

    public static int getDataLoaderTimeDiff(String dbName) {
        int diff = -1;
        String query = "SELECT TIME_TO_SEC(MAX(time)) - TIME_TO_SEC(MIN(time)) FROM data_loader";

        try {
            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);

            Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query);
            if (resultSet.next()) {
                diff = resultSet.getInt(1);
            }
            return diff;
        }
        catch(Exception e) {
            return diff;
        }
    }

}
