package org.example.monitor;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
@RequiredArgsConstructor
@EnableScheduling
public class MonitorService {

    private static final String LATTICE_SERVICE_URL = "http://localhost:3307/lattice/";
    private static final String dbName = "market_db";
    private int tick_count = 0;
    private String tick_type = "";


    @PostConstruct
    public void monitor() {
        Map<String, Object> ticksInfo = getTicks();
        Integer tickCount = (Integer) ticksInfo.get("count");
        String type = (String) ticksInfo.get("type");

        if (type.equalsIgnoreCase("physical")) {
            tick_count = tickCount;
            tick_type = "physical";
        }
        else if(type.equalsIgnoreCase("logical")) {
            tick_count = tickCount;
            tick_type = "logical";
        }
    }

    @Scheduled(fixedDelay = 1000)
    public void refreshLattice() {
        if(tick_type.equalsIgnoreCase("physical")) {
            physicalRefreshLattice();
        }
        else if(tick_type.equalsIgnoreCase("logical")) {
            logicalRefreshLattice();
        }
    }

    public void physicalRefreshLattice() {
        int rowCount = getRows();
        System.out.println("Row count: " + rowCount);
        if (rowCount > tick_count) {
            callRefreshLatticeApi();
        }

    }

    public void logicalRefreshLattice() {
        int timeDiff = getTimeDiff();
        System.out.println("Time diff: " + timeDiff);
        if (timeDiff > tick_count) {
            callRefreshLatticeApi();
        }

    }

    private int getRows() {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<Integer> responseEntity = restTemplate.exchange(
                LATTICE_SERVICE_URL + dbName + "/getRows",
                HttpMethod.GET, new HttpEntity<>(new HttpHeaders()), Integer.class);
        if(responseEntity.getBody() == null) return -1;
        return responseEntity.getBody();
    }

    private int getTimeDiff() {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<Integer> responseEntity = restTemplate.exchange(
                LATTICE_SERVICE_URL + dbName + "/getTimeDiff",
                HttpMethod.GET, new HttpEntity<>(new HttpHeaders()), Integer.class);
        if (responseEntity.getBody() == null) return -1;
        return responseEntity.getBody();
    }

    private Map<String, Object> getTicks() {
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<Map<String, Object>> responseEntity = restTemplate.exchange(
                LATTICE_SERVICE_URL + dbName + "/getTicks",
                HttpMethod.GET, new HttpEntity<>(new HttpHeaders()),
                new ParameterizedTypeReference<Map<String, Object>>() {});
        return responseEntity.getBody();
    }

    private void callRefreshLatticeApi() {
        RestTemplate restTemplate = new RestTemplate();

        restTemplate.exchange(LATTICE_SERVICE_URL + dbName + "/refreshLattice", HttpMethod.POST,
                new HttpEntity<>(new HttpHeaders()), String.class);
    }






}