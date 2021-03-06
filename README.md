# loadtest
# add new test to config.xml
<!-- 
    Each load test upon completion will emit detailed throughput and latency report.
        <time_interval> <machine_ip> <worker_thread_id> <counter> 
        Where, counter is a throughput per //load_test/args[@name="interval", @value="$VAL"]
-->         

<!-- //load_test[@name] name of python script with load test function implementation -->
<!-- //load_test[@debug="1|0"] optional debug flag -->
<!-- //load_test[@postproc] optional post process command to do something with generated report (e.g. filter, sort, reduce, copy, save to perforce etc...) -->
<!-- //load_test[@config] mandatory configuration file to specify list of machine in the cluster, additional tasks and monitors.
<load_test name="new_test/sleepy_test.py" debug="1" postproc="test1/do_something.sh" config="conf/task_8110.json">

        <!-- use either ssh or gwsh command for remote access -->
        <shell command="ssh|gwsh"/>     

        <!-- user for remote access (valid ssh keys for that user is prerequisite) -->
        <user name="root"/>     

        <!-- specify couch db name -->
        <arg name="dbname" value="db1802" />

        <!-- specify couch db uploading folder name, [default to loadtest/] -->
        <arg name="folder" value="lazy_worker%2F" />

        <!-- specify loading batch. Number of times load test main function will be called with different URI
            generated URIs on each target machine are uniq and corresponds to dbname, machine ip and working folder 
            $dbname/$ip/$folder/$doc_name, where $doc_name is a range counter (e.g. db1802/198.18.44.95/loadtest/1)
            $doc_name(s) in generated URIs are guaranteed to be consistent, such that
            running subsequent tests with same //arg[@name @folder @shard_range @ndocs] on specific //machine[@ip] 
            should always yield same document names
        ->
        <arg name="ndocs" value="10" />

        <!-- number of revision of the same document. It's just an argument passed to main function. 
             It's up to implementation to decide what to do with (e.g. upload same document several times in a row) 
        -->
        <arg name="nrevs" value="2" />

        <!-- target specific shard(s) for document, e.g. f8000000-ffffffff 
             URI with generated document should hit specific shard.

             Note: it will produce artificial load which could be interesting from theoretical standpoint.
             It will skew the load in the cluster to 5 machine in the couch region, hitting same machines again and again while others
             will be doing nothing at all. 
             It might be usefull though, for example if you want quickly beef up specific shard with some content to test shards based operations
             (like compaction ot purge) and not upload millions of document to entire cluster.
        -->
        <arg name="shard_range" value="c0000000-c7ffffff" />
        <arg name="shard_range" value="f8000000-ffffffff" />

        <!-- specify concurrency level. Concurrent invocation of main load function. It allow increase/decrease the load.
        <arg name="workers" value="2" />

        <!-- order of document upload/insert [default to 'rand'] -->
        <arg name="order" value="rand|asce|desc" />

        <!-- throughput report interval adjustment in second -->
        <arg name="interval" value="1" />
</load_test>

