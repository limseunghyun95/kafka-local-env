docker exec -it -w /opt/kafka/bin broker sh

./kafka-console-consumer.sh \
    --bootstrap-server broker:29092 \
    --topic sample \
    --from-beginning \
    --property print.key=true \
    --property key.separator=" || "