package org.example.lattice;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.springframework.web.multipart.MultipartFile;


import java.io.File;
import org.springframework.util.MultiValueMap;

import java.util.HashMap;
import java.util.Map;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@Service
@RequiredArgsConstructor
public class LatticeService {

    private final String JDBC_SERVER = "jdbc:mysql://localhost:3306/";
    private final String USER = "root";
    private final String PWD = "mysql";

    private final Helper helper = new Helper();

    public String processStarSchema(MultipartFile file) {

        if(!helper.validSchema(file)) return "Invalid XML file !";

        try {

            File xml = helper.convertMultipartFileToFile(file);

            String res = helper.generateDimensionsAndLattices(xml);
            if(!res.equalsIgnoreCase("success")) return res;

        }
        catch (Exception e) {
            e.printStackTrace();
            return "Failure!";
        }


        return "success";
    }

    public String loadDataFromTuple(String dbname, Map<String, String> data) {
        return helper.loadDataFromTuple(dbname, data);
    }

    public String refreshLattice(String dbName) {
        return helper.updateLattice(dbName);
    }




}
