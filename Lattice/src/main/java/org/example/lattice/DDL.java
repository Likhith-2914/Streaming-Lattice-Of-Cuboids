package org.example.lattice;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.*;
import java.util.*;

public class DDL {
    private static final String regex = ":_:";
    private static final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PWD = "mysql";


    public static String generateDimensionsAndLattices(File xmlFile) {
        Map<String, String[]> tables = new HashMap<>();
        List<String> lattice_columns = new ArrayList<>();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            //check dbname
            NodeList dbNameList = doc.getElementsByTagName("databaseName");
            Element dbNameElement = (Element) dbNameList.item(0);
            String dbName = dbNameElement.getTextContent();
            String res = createDatabase(dbName);
            if(!res.equalsIgnoreCase("success")) return res;

            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);


            //create all dimension tables
            NodeList dimensionList = doc.getElementsByTagName("dimension");
            for (int i = 0; i < dimensionList.getLength(); i++) {
                Element dimension = (Element) dimensionList.item(i);
                String tableName = dimension.getElementsByTagName("name").item(0).getTextContent();

                NodeList properties = dimension.getElementsByTagName("property");
                NodeList ID = dimension.getElementsByTagName("ID");
                String[] columns = new String[properties.getLength()+ID.getLength()];

                Element first_property = (Element) ID.item(0);
                String firstColumnName = first_property.getElementsByTagName("name").item(0).getTextContent();
                String firstcolumnType = first_property.getElementsByTagName("type").item(0).getTextContent();
                String firstlattice = first_property.getElementsByTagName("lattice").item(0).getTextContent();
                columns[0] = firstColumnName + " " + firstcolumnType;
                columns[0] += " PRIMARY KEY";
                if(firstlattice.equals("true")) {
                    lattice_columns.add(firstColumnName + " " + firstcolumnType);
                }


                for (int j = 0; j < properties.getLength(); j++) {
                    Element property = (Element) properties.item(j);
                    String columnName = property.getElementsByTagName("name").item(0).getTextContent();
                    String columnType = property.getElementsByTagName("type").item(0).getTextContent();
                    String lattice = property.getElementsByTagName("lattice").item(0).getTextContent();
                    columns[j+1] = columnName + " " + columnType;
                    if(lattice.equals("true")){
                        lattice_columns.add(columnName + regex + columnType + regex + tableName);
                    }
                }

                tables.put(tableName, columns);
            }


            //get all facts
            NodeList factList = doc.getElementsByTagName("fact");
            List<String> facts = new ArrayList<>();

            for (int i = 0; i < factList.getLength(); i++) {
                Element factElement = (Element) factList.item(i);

                String name = factElement.getElementsByTagName("name").item(0).getTextContent();
                facts.add(name + " float");

            }
            int n = lattice_columns.size()+facts.size()+3;
            String[] cols = new String[n];
            for(int i = 0;i<lattice_columns.size();i++){
                String[] tmp = lattice_columns.get(i).split(regex);
                cols[i] = tmp[0] + " " + tmp[1] + " NOT NULL";
            }
            for(int i = 0; i < facts.size(); i++) {
                cols[i+lattice_columns.size()] = facts.get(i) + " NOT NULL";
            }
            cols[n-3] = "time DATETIME";
            cols[n-2] = "id INT AUTO_INCREMENT";
            cols[n-1] = "PRIMARY KEY (id)";

            //get data loader table
            tables.put("data_loader", cols);

            //look-up table for lattice_cols
            String[] lookup_cols = new String[4];
            lookup_cols[0] = "lattice_col VARCHAR(255) UNIQUE";
            lookup_cols[1] = "type VARCHAR(255)";
            lookup_cols[2] = "code INT PRIMARY KEY";
            lookup_cols[3] = "dimension VARCHAR(255)";
            tables.put("lattice_lookup", lookup_cols);

            //aggregate functions table
            String[] fact_agg = new String[3];
            fact_agg[0] = "fact VARCHAR(255)";
            fact_agg[1] = "agg VARCHAR(255)";
            fact_agg[2] = "PRIMARY KEY (fact, agg)";
            tables.put("fact_agg", fact_agg);

            res  = createTables(connection, tables);
            if(!res.equalsIgnoreCase("success")) {
                new DML().deleteDatabase(dbName);
                return res;
            }

            res  = updateLatticeLookUp(connection, lattice_columns);
            if(!res.equalsIgnoreCase("success")) {
                new DML().deleteDatabase(dbName);
                return res;
            }

            res  = updateFactAgg(connection, factList);
            if(!res.equalsIgnoreCase("success")) {
                new DML().deleteDatabase(dbName);
                return res;
            }

            res  = buildLattice(connection);
            if(!res.equalsIgnoreCase("success")) {
                new DML().deleteDatabase(dbName);
                return res;
            }



        } catch (Exception e) {
            e.printStackTrace();
            return "Failed!";
        }

        return "success";
    }

    public static String storeStreamInfo(File streamXML) {

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(streamXML);

            Element root = document.getDocumentElement();
            String databaseName = root.getElementsByTagName("databaseName").item(0).getTextContent();
            NodeList tick = root.getElementsByTagName("tick");
            String tickType = ((Element) tick.item(0)).getElementsByTagName("tick_type").item(0).getTextContent();
            String tickCount = ((Element) tick.item(0)).getElementsByTagName("tick_count").item(0).getTextContent();
            String tickUnits = ((Element) tick.item(0)).getElementsByTagName("tick_units").item(0).getTextContent();
            NodeList velocity = root.getElementsByTagName("velocity");
            String velocityType = ((Element) velocity.item(0)).getElementsByTagName("velocity_type").item(0).getTextContent();
            String velocityCount = ((Element) velocity.item(0)).getElementsByTagName("velocity_count").item(0).getTextContent();
            String velocityUnits = ((Element) velocity.item(0)).getElementsByTagName("velocity_units").item(0).getTextContent();

            String res = insertDataIntoDatabase(databaseName, tickType, tickCount, tickUnits, velocityType, velocityCount, velocityUnits);
            if(!res.equalsIgnoreCase("success")) return res;

        } catch (Exception e) {
            e.printStackTrace();
            return "Failure\n"+e.getMessage();
        }
        return "success";
    }

    private static String insertDataIntoDatabase(String databaseName, String tickType, String tickCount, String tickUnits, String velocityType, String velocityCount, String velocityUnits) {
        String JDBC_URL = JDBC_SERVER + databaseName;


        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD)) {
            String res = createTableIfNotExists(connection);
            if(!res.equalsIgnoreCase("success")) return res;

            String sql = "INSERT INTO streaming_properties (property, type, count, units) VALUES (?, ?, ?, ?), (?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE type = VALUES(type), count = VALUES(count), units = VALUES(units)";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, "tick");
                statement.setString(2, tickType);
                statement.setString(3, tickCount);
                statement.setString(4, tickUnits);
                statement.setString(5, "velocity");
                statement.setString(6, velocityType);
                statement.setString(7, velocityCount);
                statement.setString(8, velocityUnits);
                statement.executeUpdate();
            }

            return "success";

        } catch (SQLException e) {
            e.printStackTrace();
            return "Failed to insert\n"+e.getMessage();
        }
    }

    private static String createTableIfNotExists(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS streaming_properties ("
                + "property VARCHAR(30) PRIMARY KEY,"
                + "type VARCHAR(30) NOT NULL,"
                + "count INT NOT NULL,"
                + "units VARCHAR(30) NOT NULL"
                + ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
            return "success";
        }
        catch (Exception e) {
            return "Failed to create stream_properties database\n"+e.getMessage();
        }
    }

    private static String buildLattice(Connection connection) {
        Helper helper = new Helper();
        List<String> lattice_cols = QueryProcessor.getLatticeDetails(connection);

        //agg_fact
        List<String> facts = QueryProcessor.getFacts(connection);

        int n = lattice_cols.size();
        for(int i = 0; i < n; i += 1) {
            System.out.println("Level " + i);
            createFactTables(connection, lattice_cols, facts, i);
        }

        try {
            Statement statement = connection.createStatement();
            StringBuilder createStatement = new StringBuilder("CREATE TABLE IF NOT EXISTS lattice_apex (");
            createStatement.append("id INT, \n");
            for(String fact: facts) {
                String fieldName = fact.split(regex)[0] + "_" + fact.split(regex)[1];
                String fieldType = "float";
                createStatement.append(fieldName).append(" ").append(fieldType).append(",\n");
            }
            createStatement.append("PRIMARY KEY ( id");
            createStatement.append(")\n");
            createStatement.append(");");

            statement.executeUpdate(createStatement.toString());
            System.out.println("All fact table created");

        }
        catch(Exception e) {
            System.err.println(e);
            return "Failed to created lattice";
        }
        return "success";

    }
    private static String createDatabase(String databaseName) {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_SERVER, USER, PWD);
            Statement stmt = connection.createStatement();
            String sql = "CREATE DATABASE " + databaseName;
            stmt.executeUpdate(sql);
            connection.close();
        }
        catch(Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }

        return "success";

    }
    private static String createTables(Connection connection, Map<String, String[]> tables) {

        try {

            for (Map.Entry<String, String[]> entry : tables.entrySet()) {
                String tableName = entry.getKey();
                String[] columns = entry.getValue();

                StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
                for (String column : columns) {
                    sql.append(column).append(", ");
                }
                sql.delete(sql.length() - 2, sql.length()); // Remove the last comma and space
                sql.append(");");
                Statement statement = connection.createStatement();
                System.out.println(sql.toString());
                statement.executeUpdate(sql.toString());
                statement.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
            return "Failed to create dimension tables\n" + e.getMessage();
        }

        return "success";
    }
    private static String updateLatticeLookUp(Connection connection, List<String> lattice_cols) {

        String sql = "INSERT INTO lattice_lookup (lattice_col, type, code, dimension) VALUES (?, ?, ?, ?)";

        try {

            PreparedStatement stmt = connection.prepareStatement(sql);
            int i = 1;
            for(String col: lattice_cols) {
                String[] tmp = col.split(regex);
                String col_name = tmp[0];
                String col_type = tmp[1];
                String dimension = tmp[2];
                stmt.setString(1, col_name);
                stmt.setString(2, col_type);
                stmt.setInt(3, i);
                stmt.setString(4, dimension);
                stmt.executeUpdate();
                i += 1;
            }

            return "success";
        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failed to create lattice_lookup\n" + e.getMessage();
        }
    }
    private static String updateFactAgg(Connection connection, NodeList factList) {

        String sql = "INSERT INTO fact_agg (fact, agg) VALUES (?, ?)";
        List<String> available_aggs = Arrays.asList("sum", "avg", "count", "max", "min");



        try {

            PreparedStatement stmt = connection.prepareStatement(sql);
            for (int i = 0; i < factList.getLength(); i++) {
                Element factElement = (Element) factList.item(i);

                String fact = factElement.getElementsByTagName("name").item(0).getTextContent();
                NodeList aggregateFunctions = factElement.getElementsByTagName("aggregate_function");
                boolean add_count = false;

                for (int j = 0; j < aggregateFunctions.getLength(); j++) {
                    String agg = aggregateFunctions.item(j).getTextContent();
                    if(!available_aggs.contains(agg)) {
                        return "Invalid aggregation function";
                    }
                    if(agg.equalsIgnoreCase("avg")) {
                        add_count = true;
                    }
                    if(agg.equalsIgnoreCase("count")) {
                        add_count = false;
                    }
                    stmt.setString(1, fact);
                    stmt.setString(2, agg);
                    stmt.executeUpdate();
                }

                if(add_count) {
                    stmt.setString(1, fact);
                    stmt.setString(2, "count");
                    stmt.executeUpdate();
                }

            }

            return "success";
        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failed to create fact_agg\n" + e.getMessage();
        }

    }
    private static void createFactTables(Connection connection, List<String> lattice_cols, List<String> facts, int level) {
        int n = lattice_cols.size();

        int start = 0;
        int counter = 0;

        List<List<String>> allCombs = Helper.getCombinations(lattice_cols, n - level);
        int tables = allCombs.size();


        while (counter < tables) {
            List<String> cols = allCombs.get(counter);
            StringBuilder tableName = new StringBuilder("lattice_");
            for (String col : cols) {
                String code = col.split(regex)[0];
                tableName.append(code).append("_");
            }
            tableName.append("facts");
            StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName.toString() + " (\n");

            List<String> IDs = new ArrayList<>();
            for (String col : cols) {
                String fieldName = col.split(regex)[1];
                String fieldType = col.split(regex)[2];
                createTableSQL.append(fieldName).append(" ").append(fieldType).append(",\n");
                IDs.add(fieldName);
            }

            for (String col : facts) {
                String fieldName = col.split(regex)[0] + "_" + col.split(regex)[1];
                String fieldType = "float";
                createTableSQL.append(fieldName).append(" ").append(fieldType).append(",\n");
            }

            createTableSQL.append("PRIMARY KEY (");
            for (int i = 0; i < IDs.size() - 1; i += 1) {
                createTableSQL.append(IDs.get(i)).append(", ");
            }
            createTableSQL.append(IDs.get(IDs.size() - 1));
            createTableSQL.append(")\n");
            createTableSQL.append(");");


            try {
                Statement statement = connection.createStatement();
                statement.executeUpdate(createTableSQL.toString());
                int c = counter + 1;
                System.out.println("Created table " + c + "/" + tables + " of level " + level);
            } catch (Exception e) {
                System.err.println(e);
            }

            start += 1;
            counter += 1;
        }

    }


}
