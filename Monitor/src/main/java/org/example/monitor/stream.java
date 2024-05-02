package org.example.lattice;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class stream {

    public static void main(String[] args) {
        String xmlFilePath = "stream.xml"; 

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(xmlFilePath);

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

            insertDataIntoDatabase(databaseName, tickType, tickCount, tickUnits, velocityType, velocityCount, velocityUnits);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertDataIntoDatabase(String databaseName, String tickType, String tickCount, String tickUnits, String velocityType, String velocityCount, String velocityUnits) {
        String url = "jdbc:mysql://172.17.0.2:3306/" + databaseName;
        String username = "root";
        String password = "pass";

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            createTableIfNotExists(connection);

            String sql = "INSERT INTO stream_def (property, type, count, units) VALUES (?, ?, ?, ?), (?, ?, ?, ?)";
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTableIfNotExists(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS stream_def ("
                + "property VARCHAR(255) PRIMARY KEY,"
                + "type VARCHAR(255),"
                + "count VARCHAR(255),"
                + "units VARCHAR(255)"
                + ")";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
        }
    }
}