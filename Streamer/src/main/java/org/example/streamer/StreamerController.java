package org.example.streamer;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor

@RequestMapping("/streamer")
public class StreamerController {

    private final StreamerService streamerService;

//    @PostMapping("/{dbName}/uploadCSV")

}
