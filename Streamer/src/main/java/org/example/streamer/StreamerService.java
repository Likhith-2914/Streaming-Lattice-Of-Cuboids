package org.example.streamer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class StreamerService {

    private static final String csv_file_path = "src/main/java/org/example/streamer/data.csv";
    private static final String dbName = "market_db";
    @PostConstruct
    public static void streamData() {

        String[] header;
        try (BufferedReader br = new BufferedReader(new FileReader(csv_file_path))) {
            String line;
            header = br.readLine().split(",");
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < header.length; i++) {
                    row.put(header[i], values[i]);
                }

                System.out.println(row);

                HttpHeaders headers = new HttpHeaders();
                RestTemplate restTemplate = new RestTemplate();
                headers.setContentType(MediaType.APPLICATION_JSON);
                ResponseEntity<String> responseEntity = new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);

                responseEntity = restTemplate.exchange("http://localhost:3307/lattice/"+dbName+"/uploadDataFromTuple",
                        HttpMethod.POST, new HttpEntity<>(row, headers), String.class);

                if(responseEntity.getBody() == null) {
                    System.out.println("Failure");
                    System.exit(2);
                }
                else if(responseEntity.getBody().equalsIgnoreCase("data not found")){
                    System.out.println(responseEntity.getBody());
                }
                else if(!responseEntity.getBody().equalsIgnoreCase("success")){
                    System.out.println(responseEntity.getBody());
                    System.exit(1);
                }

                Thread.sleep(2*1000);
            }
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


}
