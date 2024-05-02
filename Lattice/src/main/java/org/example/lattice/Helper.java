package org.example.lattice;

import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.swing.plaf.nimbus.State;
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

    public static boolean validSchema(MultipartFile xmlFile, String schemaLocation) {
        try {
            InputStream inputStream = xmlFile.getInputStream();
            return validateXMLAgainstXSD(inputStream, new File(schemaLocation));

        } catch (IOException e) {
            return false;
        }
    }
    public static File convertMultipartFileToFile(MultipartFile file) throws IOException {
        File tempFile = File.createTempFile("tempfile", file.getOriginalFilename());
        file.transferTo(tempFile);
        return tempFile;
    }
    public static Double defaultOfAgg(String agg) {

        if(agg.equalsIgnoreCase("sum")) return 0D;
        if(agg.equalsIgnoreCase("count")) return 0D;
        if(agg.equalsIgnoreCase("avg")) return 0D;
        if(agg.equalsIgnoreCase("max")) return Double.MIN_VALUE;
        if(agg.equalsIgnoreCase("min")) return Double.MAX_VALUE;

        return 0D;
    }
    public static Double mergeAgg(Double a, Double b, String agg) {

        if(agg.equalsIgnoreCase("sum")) return a+b;
        if(agg.equalsIgnoreCase("count")) return a+1D;
        if(agg.equalsIgnoreCase("max")) return Double.max(a, b);
        if(agg.equalsIgnoreCase("min")) return Double.min(a, b);
        return a;
    }

    public static Double movingAvg(Double a_avg, Double a_count, Double b_avg, Double b_count) {
        Double total_sum = a_avg*a_count + b_avg*b_count;
        Double total_count = a_count + b_count;
        return total_sum/total_count;
    }
    public static List<List<String>> getCombinations(List<String> columnNames, int level) {
        List<List<String>> combinations = new ArrayList<>();
        generateCombinations(columnNames, level, 0, new ArrayList<>(), combinations);
        return combinations;
    }
    public static void generateCombinations(List<String> columnNames, int level, int start, List<String> current, List<List<String>> combinations) {
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
