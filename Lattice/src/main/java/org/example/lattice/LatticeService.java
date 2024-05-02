package org.example.lattice;

import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.springframework.web.multipart.MultipartFile;


import java.io.File;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class LatticeService {

    private final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private final String USER = "root";
    private final String PWD = "mysql";

    private final Helper helper = new Helper();
    private final DDL ddl = new DDL();
    private final DML dml = new DML();
    private final QueryProcessor queryProcessor = new QueryProcessor();

    public String processStarSchema(MultipartFile file) {

        if(!helper.validSchema(file)) return "Invalid XML file !";

        try {

            File xml = helper.convertMultipartFileToFile(file);

            String res = ddl.generateDimensionsAndLattices(xml);
            if(!res.equalsIgnoreCase("success")) return res;

        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failure!";
        }


        return "success";
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



}
