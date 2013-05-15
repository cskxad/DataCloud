package com.xad.hadoop.mappers;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public abstract class AbstractCacheMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    protected Map cache;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        // initialize the Map
        cache = new HashMap();
        // initialize the cache
        Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        System.err.println("-->"+DistributedCache.getLocalCacheFiles(context.getConfiguration()));
        BufferedReader bufferedReader;

        if(paths == null) {
            System.err.println("Null Path returned..");
            return;
        }

        for (int i = 0; i < paths.length; i++) {
            Path path = paths[i];
            bufferedReader = new BufferedReader(new FileReader(path.toString()));
            String line = null;

            while((line = bufferedReader.readLine()) != null) {
                String[] records = parseRecord(line);
                if(records != null) {
                    cache.put(records[0], records[1]);
                }
            }
        }
    }
    
    public abstract String[] parseRecord(String record);
}
