use std::env;
use std::time::Instant;
use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;

use aerospike::{as_bin, as_key, Bins, Client, ClientPolicy, ReadPolicy, WritePolicy};
use aerospike::operations;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    info!("Starting consumer loop");

    let cpolicy = ClientPolicy::default();
    let hosts = env::var("AEROSPIKE_HOSTS")
        .unwrap_or(String::from("127.0.0.1:3000"));
    let client = Client::new(&cpolicy, &hosts)
        .expect("Failed to connect to cluster");

    let wpolicy = WritePolicy::default();

    let mut count = 0;
    let mut started = Instant::now();
    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };

                let k = String::from_utf8(m.key().unwrap().to_vec()).unwrap();
                let key = as_key!("test".to_string(), "set".to_string(), k);
                let v = serde_json::from_str::<serde_json::Value>(payload).unwrap();
                let after = v.get("after").unwrap();
                let bins = vec![
                    as_bin!("int", after.get("aid").unwrap().as_i64().unwrap()),
                    as_bin!("int", after.get("bid").unwrap().as_i64().unwrap()),
                    as_bin!("int", after.get("abalance").unwrap().as_i64().unwrap()),
                    as_bin!("str", after.get("filler").unwrap().as_str().unwrap())
                ];
                client.put(&wpolicy, &key, &bins).unwrap();

                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();

                count += 1;

                if count == 1 {
                    started = Instant::now();
                }

                if count % 1000 == 0 {
                    info!("count: {}, elapsed: {}µs", count, started.elapsed().as_micros());
                }
            }
        };
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    // let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    // let brokers = matches.value_of("brokers").unwrap();
    // let group_id = matches.value_of("group-id").unwrap();

    let topics = vec!["DozerTestServer.public.pgbench_accounts"];
    let brokers = "localhost:9092";
    let group_id = "test";
    consume_and_print(brokers, group_id, &topics).await
}