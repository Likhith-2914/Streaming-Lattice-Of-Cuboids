package org.example.lattice;


import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

@RestController
@RequiredArgsConstructor

@RequestMapping("/lattice")
public class LatticeController {

    private final LatticeService latticeService;

    @PostMapping("/uploadStarSchema")
    public String processXML(@RequestBody MultipartFile file) {
        return latticeService.processStarSchema(file);
    }

    @PostMapping("/{dbName}/uploadDataFromTuple")
    public String uploadDataFromTuple(@RequestBody Map<String, String> data, @PathVariable String dbName) {

        return latticeService.loadDataFromTuple(dbName, data);
    }

    @PostMapping("/{dbName}/refreshLattice")
    public String refreshLattice(@PathVariable String dbName) {
        return latticeService.refreshLattice(dbName);
    }

    @GetMapping("/{dbName}/selectTables")
    public List<Map<String, Object>> selectTables(@RequestBody Map<String, Boolean> tables, @PathVariable String dbName) {
        return latticeService.selectTables(dbName, tables);
    }
}
