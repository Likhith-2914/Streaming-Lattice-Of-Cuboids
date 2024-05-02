package org.example.lattice;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DML {

    private static final String regex = ":_:";
    private static final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PWD = "mysql";

    public String loadDataFromTuple(String dbName, Map<String, String> tuple) {

        try {

            String JDBC_URL = JDBC_SERVER + dbName;
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_URL, USER, PWD);

            Map<String, Double> fact_values = new HashMap<>();
            List<String> facts = QueryProcessor.getFactNames(connection);

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

            List<String> lattice_cols = QueryProcessor.getLatticeDetails(connection);
            List<String> facts = QueryProcessor.getFacts(connection);
            Map<String, Object> velocity = QueryProcessor.getStreamProperty(connection, "velocity");

            String res = "";
            int n = lattice_cols.size();
            for(int i = 0; i < n; i += 1) {
                System.out.println("Level " + i);
                res = updateLatticeForALevel(connection, lattice_cols, facts, i, velocity);
                if(!res.equalsIgnoreCase("success")) return res+"\nAt Level"+i;
            }

            res = updateLatticeApex(connection, facts, velocity);
            if(!res.equalsIgnoreCase("success")) return res;

            res = deleteFromDataLoader(connection, velocity);
            if(!res.equalsIgnoreCase("success")) return res;

        } catch (Exception e) {
            e.printStackTrace();
            return "Fail\n"+e.getMessage();
        }

        return "success";

    }

    public void deleteDatabase(String dbName) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection connection = DriverManager.getConnection(JDBC_SERVER, USER, PWD);
            String sql = "DROP DATABASE " + dbName;
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String updateLatticeForALevel(Connection connection, List<String> latticeCols, List<String> facts, int level, Map<String, Object> velocity) {
        int n = latticeCols.size();

        List<List<String>> allCombs = Helper.getCombinations(latticeCols, n - level);

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
            selectQuery.append(" FROM (SELECT * FROM data_loader ");

            if(velocity.get("type").toString().equalsIgnoreCase("physical")) {
                selectQuery.append("ORDER BY id LIMIT ").append(velocity.get("count").toString());
                selectQuery.append(") AS velocity_table ");
            }

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



            Map<String, Double> existingRec = new HashMap<>();

            StringBuilder existingSelectQuery = new StringBuilder("SELECT ");
            for (String fact : facts) {
                String agg = fact.split(regex)[0].toLowerCase();
                String factName = fact.split(regex)[1];
                existingSelectQuery.append(agg).append("_").append(factName).append(", ");
                existingRec.put(agg+"_"+factName, Helper.defaultOfAgg(agg));
            }
            existingSelectQuery.deleteCharAt(existingSelectQuery.length()-1);
            existingSelectQuery.deleteCharAt(existingSelectQuery.length()-1);
            existingSelectQuery.append(" FROM ").append(tableName).append(" WHERE ");
            for (String col : cols) {
                String colName = col.split(regex)[1];
                existingSelectQuery.append(colName).append(" = ? AND ");
            }
            existingSelectQuery.deleteCharAt(existingSelectQuery.length() - 1);
            existingSelectQuery.deleteCharAt(existingSelectQuery.length() - 1);
            existingSelectQuery.deleteCharAt(existingSelectQuery.length() - 1);
            existingSelectQuery.deleteCharAt(existingSelectQuery.length() - 1);
            existingSelectQuery.deleteCharAt(existingSelectQuery.length() - 1);


            try {

                PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString());
                ResultSet resultSet = preparedStatement.executeQuery();

                while (resultSet.next()) {
                    PreparedStatement updateStatement = connection.prepareStatement(updateQuery.toString());

                    PreparedStatement stmt = connection.prepareStatement(existingSelectQuery.toString());


                    int i = 1;
                    for (String col : cols) {
                        String colName = col.split(regex)[1];
                        updateStatement.setObject(i, resultSet.getObject(colName));
                        stmt.setString(i, resultSet.getString(colName));
                        i += 1;
                    }


                    ResultSet rs = stmt.executeQuery();
                    if(rs.next()) {
                        for (String fact : facts) {
                            String agg = fact.split(regex)[0].toLowerCase();
                            String factName = fact.split(regex)[1];
                            existingRec.put(agg + "_" + factName, rs.getDouble(agg + "_" + factName));
                        }
                    }

                    for (String fact : facts) {
                        String agg = fact.split(regex)[0].toLowerCase();
                        String factName = fact.split(regex)[1];
                        if(!agg.equalsIgnoreCase("avg"))
                            updateStatement.setObject(i, Helper.mergeAgg(existingRec.get(agg+"_"+factName), resultSet.getDouble(agg+"_"+factName), agg));
                        else
                            updateStatement.setObject(i, Helper.movingAvg(existingRec.get("avg"+"_"+factName), (existingRec.get("count"+"_"+factName)),
                                    resultSet.getDouble("avg"+"_"+factName), resultSet.getDouble("count"+"_"+factName)));
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
    private static String updateLatticeApex(Connection connection, List<String> facts, Map<String, Object> velocity) {

        StringBuilder selectQuery = new StringBuilder("SELECT ");
        for (String fact : facts) {
            String agg = fact.split(regex)[0].toUpperCase();
            String factName = fact.split(regex)[1];
            selectQuery.append(agg).append("(").append(factName).append(") AS ").append(agg.toLowerCase()).append("_").append(factName).append(", ");
        }
        selectQuery.deleteCharAt(selectQuery.length()-1);
        selectQuery.deleteCharAt(selectQuery.length()-1);
        selectQuery.append(" FROM (SELECT * FROM data_loader ");

        if(velocity.get("type").toString().equalsIgnoreCase("physical")) {
            selectQuery.append("ORDER BY id LIMIT ").append(velocity.get("count").toString());
            selectQuery.append(") AS velocity_table");
        }


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

        Map<String, Double> existingRec = new HashMap<>();

        StringBuilder existingSelectQuery = new StringBuilder("SELECT ");
        for (String fact : facts) {
            String agg = fact.split(regex)[0].toLowerCase();
            String factName = fact.split(regex)[1];
            existingSelectQuery.append(agg).append("_").append(factName).append(", ");
            existingRec.put(agg+"_"+factName, Helper.defaultOfAgg(agg));
        }
        existingSelectQuery.deleteCharAt(existingSelectQuery.length()-1);
        existingSelectQuery.deleteCharAt(existingSelectQuery.length()-1);
        existingSelectQuery.append(" FROM ").append("lattice_apex");


        try {

            PreparedStatement preparedStatement = connection.prepareStatement(selectQuery.toString());
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                PreparedStatement updateStatement = connection.prepareStatement(updateQuery.toString());
                updateStatement.setObject(1, 1);

                PreparedStatement stmt = connection.prepareStatement(existingSelectQuery.toString());
                ResultSet rs = stmt.executeQuery();
                if(rs.next()) {
                    for (String fact : facts) {
                        String agg = fact.split(regex)[0].toLowerCase();
                        String factName = fact.split(regex)[1];
                        existingRec.put(agg + "_" + factName, rs.getDouble(agg + "_" + factName));
                    }
                }

                int i = 2;
                for (String fact : facts) {
                    String agg = fact.split(regex)[0].toLowerCase();
                    String factName = fact.split(regex)[1];
                    updateStatement.setObject(i, Helper.mergeAgg(existingRec.get(agg+"_"+factName), resultSet.getDouble(agg+"_"+factName), agg));
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
    private static String deleteFromDataLoader(Connection connection, Map<String, Object> velocity) {

        try {
            StringBuilder sql = new StringBuilder("DELETE FROM data_loader ");
            if(velocity.get("type").toString().equalsIgnoreCase("physical")) {
                sql.append("ORDER BY id LIMIT ").append(velocity.get("count"));
            }

            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql.toString());

            return "success";

        }
        catch(Exception e) {
            return "failed to delete data loader";
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
                List<String> dim_lattice_columns = QueryProcessor.getLatticeDetailsOfDimension(connection, dimension);

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
            List<String> facts = QueryProcessor.getFactNames(connection);


            //build the insert Query
            StringBuilder insertQuery = new StringBuilder("INSERT INTO data_loader (");
            for (String latticeColumn : lattice_columns) {
                insertQuery.append(latticeColumn);
                insertQuery.append(", ");
            }

            for(String fact: facts) {
                insertQuery.append(fact).append(", ");
            }

            insertQuery.append("time) VALUES (");

            for(int j = 0; j < lattice_columns.size(); j += 1) {
                insertQuery.append('?');
                insertQuery.append(", ");
            }
            for(int j = 0; j < facts.size(); j += 1) {
                insertQuery.append('?');
                insertQuery.append(", ");
            }
            insertQuery.append("?);");

            //prepare the update statement
            PreparedStatement stmt = connection.prepareStatement(insertQuery.toString());

            for(int j = 1; j <= lattice_columns.size(); j +=1) {
                stmt.setObject(j, baseRow.get(lattice_columns.get(j-1)));
            }
            for(int j = 1; j<=facts.size(); j++) {
                stmt.setObject(j+lattice_columns.size(), fact_values.get(facts.get(j-1)));
            }
            stmt.setObject(facts.size()+lattice_columns.size()+1, LocalDateTime.now());
            //execute update
            stmt.executeUpdate();

            return "success";


        } catch (SQLIntegrityConstraintViolationException e) {
            e.printStackTrace();
            return "Invalid data provided.. Metadata for at least one dimension is not specified\n" + e.getMessage();

        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failed " + e.getMessage();
        }

    }



}
