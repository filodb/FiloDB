# FiloDB
## FiloDB Kafka
Common setup

    val settings = new KafkaSettings()
    
Creating a publisher
    val io = Scheduler.io("publisher-scheduler")
    
    val producer = KafkaProducer[String, String](settings.producerConfig, io)
    val pushT = Observable.range(0, 1000)
            .map(msg => new ProducerRecord(topic, "obs", msg.toString))
            .bufferIntrospective(1024)
            .consumeWith(producer)