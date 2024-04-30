package org.example.lattice;

import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

public class Helper {

    private static final String regex = ":_:";
    private static final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PWD = "mysql";


    public boolean validSchema(MultipartFile xmlFile) {
        try {
            InputStream inputStream = xmlFile.getInputStream();
            return validateXMLAgainstXSD(inputStream, new File("src/main/java/org/example/lattice/schema.xsd"));

        } catch (IOException e) {
            return false;
        }
    }
    public File convertMultipartFileToFile(MultipartFile file) throws IOException {
        File tempFile = File.createTempFile("tempfile", file.getOriginalFilename());
        file.transferTo(tempFile);
        return tempFile;
    }

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

    public String loadDataFromTuple(String dbName, Map<String, String> tuple) {

        try {

            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);

            Map<String, Double> fact_values = new HashMap<>();
            List<String> facts = getFactNames(connection);

            for(String fact: facts) {
                fact_values.put(fact, Double.parseDouble(tuple.get(fact)));
                tuple.remove(fact);
            }

            String res = updateDataLoader(tuple, fact_values, connection);
            if(!res.equalsIgnoreCase("success")) return res;

            return "success";
        }
        catch (Exception e) {
            return "Failure\n"+e.getMessage();
        }
    }


    public String updateLattice(String dbName) {

        try {
            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);

            List<String> lattice_cols = getLatticeDetails(connection);
            List<String> facts = getFacts(connection);
            String res = "";
            int n = lattice_cols.size();
            for(int i = 0; i < n; i += 1) {
                System.out.println("Level " + i);
                res = updateLatticeForALevel(connection, lattice_cols, facts, i);
                if(!res.equalsIgnoreCase("success")) return res+"\nAt Level"+i;
            }

            res = updateLatticeApex(connection, facts);
            if(!res.equalsIgnoreCase("success")) return res;

        } catch (Exception e) {
            e.printStackTrace();
            return "Fail\n"+e.getMessage();
        }

        return "success";

    }
    private static String updateLatticeForALevel(Connection connection, List<String> latticeCols, List<String> facts, int level) {
        int n = latticeCols.size();

        List<List<String>> allCombs = getCombinations(latticeCols, n - level);

        for (List<String> cols : allCombs) {

            StringBuilder selectQuery = new StringBuilder("SELECT ");
            for (String col : cols) {
                String colName = col.split(regex)[1];
                selectQuery.append(colName).append(", ");
            }
            for (String fact : facts) {
                String agg = fact.split(regex)[0].toUpperCase();
                String factName = fact.split(regex)[1];
                selectQuery.append(agg).append("(").append(factName).append(") AS ").append(agg.toLowerCase()).append("_").append(factName).append(", ");
            }
            selectQuery.deleteCharAt(selectQuery.length()-1);
            selectQuery.deleteCharAt(selectQuery.length()-1);
            selectQuery.append(" FROM data_loader ");
            selectQuery.append("GROUP BY ");
            for (String col : cols) {
                String colName = col.split(regex)[1];
                selectQuery.append(colName).append(", ");
            }
            selectQuery.deleteCharAt(selectQuery.length()-1);
            selectQuery.deleteCharAt(selectQuery.length()-1);


            StringBuilder tableName = new StringBuilder("lattice_");
            for (String col : cols) {
                String code = col.split(regex)[0];
                tableName.append(code).append("_");
            }
            tableName.append("facts");

            try {
                PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString());
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    StringBuilder updateQuery = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
                    for (String col : cols) {
                        String colName = col.split(regex)[1];
                        updateQuery.append(colName).append(", ");
                    }
                    for (String fact : facts) {
                        String agg = fact.split(regex)[0].toUpperCase();
                        String factName = fact.split(regex)[1];
                        updateQuery.append(agg.toLowerCase()).append("_").append(factName).append(", ");
                    }
                    updateQuery.deleteCharAt(updateQuery.length() - 1);
                    updateQuery.deleteCharAt(updateQuery.length() - 1);
                    updateQuery.append(") VALUES (");
                    updateQuery.append("?, ".repeat(cols.size()+facts.size()-1));
                    updateQuery.append("?) ON DUPLICATE KEY UPDATE ");
                    for (String fact : facts) {
                        String agg = fact.split(regex)[0].toLowerCase();
                        String factName = fact.split(regex)[1];
                        updateQuery.append(agg).append("_").append(factName).append(" = VALUES(").append(agg).append("_").append(factName).append("), ");
                    }
                    updateQuery.deleteCharAt(updateQuery.length() - 1);
                    updateQuery.deleteCharAt(updateQuery.length() - 1);


                    PreparedStatement updateStatement = connection.prepareStatement(updateQuery.toString());
                    int i = 1;
                    for (String col : cols) {
                        String colName = col.split(regex)[1];
                        updateStatement.setObject(i, resultSet.getObject(colName));
                        i += 1;
                    }
                    for (String fact : facts) {
                        String agg = fact.split(regex)[0].toLowerCase();
                        String factName = fact.split(regex)[1];
                        updateStatement.setObject(i, resultSet.getObject(agg+"_"+factName));
                        i += 1;
                    }
                    updateStatement.executeUpdate();
                }
            }

            catch (Exception e) {
                e.printStackTrace();
                return "failed to update lattice";
            }

            System.out.println("Updated table: " + tableName);

        }

        return "success";


    }
    private static String updateLatticeApex(Connection connection, List<String> facts) {
        try {
            StringBuilder selectQuery = new StringBuilder("SELECT ");
            for (String fact : facts) {
                String agg = fact.split(regex)[0].toUpperCase();
                String factName = fact.split(regex)[1];
                selectQuery.append(agg).append("(").append(factName).append(") AS ").append(agg.toLowerCase()).append("_").append(factName).append(", ");
            }
            selectQuery.deleteCharAt(selectQuery.length()-1);
            selectQuery.deleteCharAt(selectQuery.length()-1);
            selectQuery.append(" FROM data_loader");

            PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString());
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                StringBuilder updateQuery = new StringBuilder("INSERT INTO ").append("lattice_apex").append(" (id, ");
                for (String fact : facts) {
                    String agg = fact.split(regex)[0].toUpperCase();
                    String factName = fact.split(regex)[1];
                    updateQuery.append(agg.toLowerCase()).append("_").append(factName).append(", ");
                }
                updateQuery.deleteCharAt(updateQuery.length() - 1);
                updateQuery.deleteCharAt(updateQuery.length() - 1);
                updateQuery.append(") VALUES (");
                updateQuery.append("?, ".repeat(facts.size()));
                updateQuery.append("?) ON DUPLICATE KEY UPDATE ");
                for (String fact : facts) {
                    String agg = fact.split(regex)[0].toLowerCase();
                    String factName = fact.split(regex)[1];
                    updateQuery.append(agg).append("_").append(factName).append(" = VALUES(").append(agg).append("_").append(factName).append("), ");
                }
                updateQuery.deleteCharAt(updateQuery.length() - 1);
                updateQuery.deleteCharAt(updateQuery.length() - 1);

                PreparedStatement updateStatement = connection.prepareStatement(updateQuery.toString());
                updateStatement.setObject(1, 1);
                int i = 2;
                for (String fact : facts) {
                    String agg = fact.split(regex)[0].toLowerCase();
                    String factName = fact.split(regex)[1];
                    updateStatement.setObject(i, resultSet.getObject(agg+"_"+factName));
                    i += 1;
                }
                updateStatement.executeUpdate();
                System.out.println("Updated lattice_apex table");

            }

            return "Success";
        }
        catch (Exception e) {
            e.printStackTrace();
            return "failed to update apex\n"+e.getMessage();
        }
    }


    private static String updateDataLoader(Map<String, String> dim_keys, Map<String, Double> fact_values, Connection connection) {
        List<String> lattice_columns = new ArrayList<>();
        Map<String, Object> baseRow = new HashMap<>();
        try {

            //for each dimension..
            for (String dimension: dim_keys.keySet()) {

                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(null, null, dimension);
                primaryKeyResultSet.next();
                String idCol = primaryKeyResultSet.getString("COLUMN_NAME");

                //get all the lattice columns
                List<String> dim_lattice_columns = getLatticeDetailsOfDimension(dimension, connection);

                if(dim_lattice_columns.isEmpty()) continue;

                //build select query on lattice columns
                StringBuilder selectQuery = new StringBuilder("SELECT ");
                for(int j = 0; j < dim_lattice_columns.size(); j += 1) {
                    String colName = dim_lattice_columns.get(j).split(regex)[0];
                    selectQuery.append(colName);
                    lattice_columns.add(colName);
                    if(j != dim_lattice_columns.size()-1) selectQuery.append(", ");
                }
                selectQuery.append(" FROM ").append(dimension).append(" WHERE ");
                selectQuery.append(idCol).append(" = ?;");

                //query for lattice rows
                PreparedStatement stmt = connection.prepareStatement(selectQuery.toString());
                stmt.setObject(1, dim_keys.get(dimension));

                ResultSet rs = stmt.executeQuery();

                //update the lattice base row's tuple
                while(rs.next()) {
                    for(String col: dim_lattice_columns) {
                        String colName = col.split(regex)[0];
                        baseRow.put(colName, rs.getObject(colName));
                    }
                }
            }

            //facts
            List<String> facts = getFactNames(connection);


            //build the insert Query
            StringBuilder insertQuery = new StringBuilder("INSERT INTO data_loader (");
            for (String latticeColumn : lattice_columns) {
                insertQuery.append(latticeColumn);
                insertQuery.append(", ");
            }

            for(String fact: facts) {
                insertQuery.append(fact).append(", ");
            }
            insertQuery.deleteCharAt(insertQuery.length()-1);
            insertQuery.deleteCharAt(insertQuery.length()-1);

            insertQuery.append(") VALUES (");

            for(int j = 0; j < lattice_columns.size(); j += 1) {
                insertQuery.append('?');
                insertQuery.append(", ");
            }
            for(int j = 0; j < facts.size(); j += 1) {
                insertQuery.append('?');
                insertQuery.append(", ");
            }
            insertQuery.deleteCharAt(insertQuery.length()-1);
            insertQuery.deleteCharAt(insertQuery.length()-1);

            insertQuery.append(");");

            //prepare the update statement
            PreparedStatement stmt = connection.prepareStatement(insertQuery.toString());

            for(int j = 1; j <= lattice_columns.size(); j +=1) {
                stmt.setObject(j, baseRow.get(lattice_columns.get(j-1)));
            }
            for(int j = 1; j<=facts.size(); j++) {
                stmt.setObject(j+lattice_columns.size(), fact_values.get(facts.get(j-1)));
            }
            //execute update
            stmt.executeUpdate();

            return "success";


        } catch (Exception e) {
            e.printStackTrace();
            return "Failed " + e.getMessage();
        }

    }

    private static String buildLattice(Connection connection) {
        Helper helper = new Helper();
        List<String> lattice_cols = getLatticeDetails(connection);

        //agg_fact
        List<String> facts = getFacts(connection);

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

        List<List<String>> allCombs = getCombinations(lattice_cols, n - level);
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

    private static int getLatticeCode(Connection connection, String lattice_col) {

        try {
            String sql = "SELECT code FROM lattice_lookup WHERE lattice_col = " + lattice_col;

            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);


            return rs.getInt("code");

        }
        catch(Exception e) {
            return 0;
        }
    }

    private static List<String> getLatticeDetails(Connection connection) {
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

    private static List<String> getLatticeDetailsOfDimension(String dimensionName, Connection connection) {
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

    private static List<String> getFacts(Connection connection) {
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

    private static List<String> getFactNames(Connection connection) {
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

    private static List<List<String>> getCombinations(List<String> columnNames, int level) {
        List<List<String>> combinations = new ArrayList<>();
        generateCombinations(columnNames, level, 0, new ArrayList<>(), combinations);
        return combinations;
    }

    private static void generateCombinations(List<String> columnNames, int level, int start, List<String> current, List<List<String>> combinations) {
        if (current.size() == level) {
            combinations.add(new ArrayList<>(current));
            return;
        }
        for (int i = start; i < columnNames.size(); i++) {
            current.add(columnNames.get(i));
            generateCombinations(columnNames, level, i + 1, current, combinations);
            current.remove(current.size() - 1);
        }
    }

    private static boolean validateXMLAgainstXSD(InputStream xmlInputStream, File xsdFile) {
        try {
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = factory.newSchema(xsdFile);
            Validator validator = schema.newValidator();
            validator.validate(new StreamSource(xmlInputStream));
            return true; // XML is valid
        } catch (IOException | SAXException e) {
            e.printStackTrace();
            return false;
        }
    }


}
