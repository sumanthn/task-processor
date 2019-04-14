package sn.analytics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Trigger tasks externally ,
 * Get Queue size and other api interfaces
 */
@RestController
@RequestMapping("/v1/taskprocessor")
public class TaskProcessorApi {


    @Autowired
    private TaskProcessor taskProcessor;
    @RequestMapping(value = "/slot-stats",method = RequestMethod.GET,produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String,Integer> fetchSlotStats(){

        Map<String,Integer> dmap = new HashMap<>();
        dmap.put("occupied",taskProcessor.getOccupiedSlots());
        dmap.put("max",taskProcessor.getMaxSlots());
        return dmap;
    }



}
