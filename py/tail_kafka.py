from kafka import KafkaConsumer

k = KafkaConsumer('frontend', bootstrap_servers=['172.17.0.1:9092'])

for msg in k:
    print repr(msg.value)
