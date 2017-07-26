# FiloDB
## FiloDB Kafka

Common setup

    val settings = new KafkaSettings(config)
    

FiloDB allows multiple topics/streams to be ingested simultaneously, one per dataset.  The config to be passed in defines the Kafka broker, topic, and other settings, one config per dataset/stream.  An example is in `kafka/src/main/resources/example-source.conf`.  The configuration is a Typesafe Config object. If you use the `filo-cli`, you pass in the name of this config file, which may be a Typesafe HOCON .conf, a .properties file, or even a .conf which includes a .properties file.

* If using a properties file, populate a comma-separated `bootstrap.servers` list of Kafka brokers.  If using a .conf file, use HOCON-format/JSON list of brokers for the `filo-kafka-servers` key.

Creating a publisher
    val io = Scheduler.io("publisher-scheduler")
    
    val producer = KafkaProducer[String, String](settings.producerConfig, io)
    val pushT = Observable.range(0, 1000)
            .map(msg => new ProducerRecord(topic, "obs", msg.toString))
            .bufferIntrospective(1024)
            .consumeWith(producer)