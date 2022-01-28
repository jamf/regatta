# Operations

## Reset cluster data

1. Downscale the regatta statefulset to 0 replicas manually\
   `$ kubectl --context <cluster> --namespace regatta scale statefulset --replicas 0 regatta`
2. Delete persistent volume claims in the cluster:\
   `$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-0`\
   `$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-1`\
   `$ kubectl --context <cluster> --namespace regatta delete persistentvolumeclaims data-regatta-2`
3. Reset kafka offset for given consumer group ID (The consumer group ID is defined in `charts-<env>.yaml` file
   in `.Values.kafka.brokers`.)
    1. SSH to kafka node (Kafka broker configuration is defined in `charts-<env>.yaml` file in `.Values.kafka.groupID`.)
    2. List the topics to which the group is subscribed (use noSSL port 8091)\
       `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --describe`
    3. Reset all topics the consumer group subsribes to (run without `--execute` flag for dry run)\
       `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --reset-offsets --all-topics --to-earliest --execute`
    4. Verify the offset was reset\
       `$ /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:8091 --group <consumer_group_id> --describe`
4. Upscale the regatta statefulset to 3 replicas manually\
   `$ kubectl --context <cluster> --namespace regatta scale statefulset --replicas 3 regatta`
