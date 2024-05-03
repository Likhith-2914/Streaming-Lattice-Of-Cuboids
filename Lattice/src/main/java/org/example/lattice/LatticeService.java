package org.example.lattice;

import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.springframework.web.multipart.MultipartFile;


import java.io.File;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class LatticeService {

    private final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private final String USER = "root";
    private final String PWD = "mysql";

    private final DML dml = new DML();
    private final QueryProcessor queryProcessor = new QueryProcessor();

    public String processStarSchema(MultipartFile file) {

        if(!Helper.validSchema(file, "src/main/java/org/example/lattice/starSchema.xsd"))
            return "Invalid XML file !";

        try {

            File xml = Helper.convertMultipartFileToFile(file);

            String res = DDL.generateDimensionsAndLattices(xml);
            if(!res.equalsIgnoreCase("success")) return res;

        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failure!";
        }


        return "success";
    }

    public String processStreamXML(MultipartFile file) {
        if(!Helper.validSchema(file, "src/main/java/org/example/lattice/streamSchema.xsd"))
            return "Invalid XML file !";

        try {

            File xml = Helper.convertMultipartFileToFile(file);

            String res = DDL.storeStreamInfo(xml);
            if(!res.equalsIgnoreCase("success")) return res;

        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failure!\n"+e.getMessage();
        }

        return "success";
    }

    public int getRows(String dbName) {
        return QueryProcessor.getDataLoaderRowCount(dbName);
    }
    public int getTimeDiff(String dbName) {
        return QueryProcessor.getDataLoaderTimeDiff(dbName);
    }

    public String loadDataFromTuple(String dbname, Map<String, String> data) {
        return dml.loadDataFromTuple(dbname, data);
    }

    public String refreshLattice(String dbName) {
        return dml.updateLattice(dbName);
    }

    public List<Map<String, Object>> selectTables(String dbName, Map<String, Boolean> columns) {
        return queryProcessor.selectTables(dbName, columns);
    }

    public Map<String, Object> callGetStreamProperty(String dbName) {
        try {
            Connection connection = DriverManager.getConnection(JDBC_SERVER + dbName, USER, PWD);
            return QueryProcessor.getStreamProperty(connection, "tick");
        } catch (SQLException e) {
            e.printStackTrace();
            return new HashMap<>();
        }
    }

}
