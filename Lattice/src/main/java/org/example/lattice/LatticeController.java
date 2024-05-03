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

    @PostMapping("/uploadFiles")
    public String processXML(@RequestBody MultipartFile starSchema, @RequestBody MultipartFile streamSchema) {
        String res = latticeService.processStarSchema(starSchema);
        if(!res.equalsIgnoreCase("success")) return res;
        res = latticeService.processStreamXML(streamSchema);
        if(!res.equalsIgnoreCase("success")) return res;
        return "success";
    }

    @PostMapping("/{dbName}/uploadDataFromTuple")
    public String uploadDataFromTuple(@RequestBody Map<String, String> data, @PathVariable String dbName) {
        return latticeService.loadDataFromTuple(dbName, data);
    }

    @PostMapping("/{dbName}/refreshLattice")
    public String refreshLattice(@PathVariable String dbName) {
        return latticeService.refreshLattice(dbName);
    }

    @GetMapping("/{dbName}/selectLatticeNode")
    public List<Map<String, Object>> selectTables(@RequestBody Map<String, Boolean> tables, @PathVariable String dbName) {
        return latticeService.selectTables(dbName, tables);
    }

    @GetMapping("/{dbName}/getRows")
    public int selectTables(@PathVariable String dbName)
    {
        return latticeService.getRows(dbName);
    }

    @GetMapping("/{dbName}/getTimeDiff")
    public int selectTimeDiff(@PathVariable String dbName)
    {
        return latticeService.getTimeDiff(dbName);
    }

    @GetMapping("/{dbName}/getTicks")
    public Map<String, Object> getTicks(@PathVariable String dbName) {
        return latticeService.callGetStreamProperty(dbName);
    }
}
