package org.example.lattice;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DDL {
    private static final String regex = ":_:";
    private static final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PWD = "mysql";


    public String generateDimensionsAndLattices(File xmlFile) {
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
            String[] cols = new String[lattice_columns.size()+facts.size()];
            for(int i = 0;i<lattice_columns.size();i++){
                String[] tmp = lattice_columns.get(i).split(regex);
                cols[i] = tmp[0] + " " + tmp[1];
            }
            for(int i = 0; i < facts.size(); i++) {
                cols[i+lattice_columns.size()] = facts.get(i);
            }

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
            if(!res.equalsIgnoreCase("success")) return res;

            res  = updateLatticeLookUp(connection, lattice_columns);
            if(!res.equalsIgnoreCase("success")) return res;

            res  = updateFactAgg(connection, factList);
            if(!res.equalsIgnoreCase("success")) return res;

            res  = buildLattice(connection);
            if(!res.equalsIgnoreCase("success")) return res;


        } catch (Exception e) {
            e.printStackTrace();
            return "Failed!";
        }

        return "success";
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

        try {

            PreparedStatement stmt = connection.prepareStatement(sql);
            for (int i = 0; i < factList.getLength(); i++) {
                Element factElement = (Element) factList.item(i);

                String fact = factElement.getElementsByTagName("name").item(0).getTextContent();
                NodeList aggregateFunctions = factElement.getElementsByTagName("aggregate_function");

                for (int j = 0; j < aggregateFunctions.getLength(); j++) {
                    String agg = aggregateFunctions.item(j).getTextContent();
                    stmt.setString(1, fact);
                    stmt.setString(2, agg);
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
