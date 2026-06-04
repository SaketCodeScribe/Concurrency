package problems.ConcurrencyDesignProblems;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PublisherSubscriber {
    static class Message{
        int messageId;
        String content;

        public Message(int messageId, String content) {
            this.messageId = messageId;
            this.content = content;
        }

        public int getMessageId() {
            return messageId;
        }

        public String getContent() {
            return content;
        }
    }
    enum SubscriptionType{
        LATEST,
        EARLIEST;
    }
    static class SubscriberMetaData{
        int lastCommittedOffset = -1;
        AtomicBoolean isSubscribed = new AtomicBoolean(false);

        public int getLastCommittedOffset() {
            return lastCommittedOffset;
        }

        public void setLastCommittedOffset(int proceed) {
            this.lastCommittedOffset += lastCommittedOffset;
        }

        public void setSubscribed(boolean subscribed) {
            isSubscribed.set(subscribed);
        }
    }

    static class Topic{
        String topicName;
        ConcurrentHashMap<String, SubscriberMetaData> subscriberMap;
        List<Message> queue;

        public Topic(String topicName) {
            this.topicName = topicName;
            this.subscriberMap = new ConcurrentHashMap<>();
            this.queue = Collections.synchronizedList(new ArrayList<>());
        }

        public SubscriberMetaData subscribe( String subscriberName, SubscriptionType subscriptionType){
            return subscriberMap.computeIfAbsent(subscriberName, key -> {
                    var meta = new SubscriberMetaData();
                    if (subscriptionType == SubscriptionType.LATEST) {
                        meta.setLastCommittedOffset(queue.size());
                    }
                    meta.setSubscribed(true);
                    return meta;
                }
            );
        }

        public boolean unsubscribe(String subscriberName){
            subscriberMap.computeIfPresent(subscriberName, (key, value) -> {
                value.setSubscribed(false);
                return value;
            });
            return true;
        }

        public void deleteSubscription(String subscriberName){
            SubscriberMetaData metaData = subscriberMap.remove(subscriberName);
            metaData.setSubscribed(false);
        }

        public List<Message> request(String subscriberName, int requests){
            SubscriberMetaData metaData = subscriberMap.get(subscriberName);
            int begin = metaData.lastCommittedOffset + 1;
            int end = begin + Math.min(metaData.getLastCommittedOffset() + requests, queue.size() - 1);
            List<Message> messages = new ArrayList<>();
            for (int i = begin; i <= end; i++) {
                if (!metaData.isSubscribed.get()) return null;
                messages.add(queue.get(i));
            }
            return messages;
        }

        public boolean commit(String subscriberName, int offset){
            SubscriberMetaData metaData = subscriberMap.get(subscriberName);
            if (metaData == null) return false;
            int checkpoint = metaData.lastCommittedOffset;
            subscriberMap.computeIfPresent(subscriberName, (key, value) -> {
                if (value.isSubscribed.compareAndSet(true, true)){
                    value.setLastCommittedOffset(offset);
                }
                return value;
            });
            return checkpoint < metaData.lastCommittedOffset;
        }

        public boolean offer(Message message){
            return queue.add(message);
        }
    }
    interface Publisher{
        void publish(List<Record> m);
    }

    static class P1 implements Publisher{
        TopicController controller;

        public P1(TopicController controller) {
            this.controller = controller;
        }

        @Override
        public void publish(List<Record> m) {
            controller.publishRecord(m);
        }

    }

    interface Subscriber{
        List<Record> poll();
        void commit(Map<Topic, Integer> read);
    }

    static class S1 implements Subscriber{
        String subscriberName;
        TopicController topicController;
        int batchSize;
        ConcurrentHashMap<Topic, Integer> topicMap;
        ExecutorService executorService;
        AtomicBoolean isRunning;

        public S1(String subscriberName, TopicController controller, int batchSize, List<String> topic, SubscriptionType subscriptionType) {
            this.subscriberName = subscriberName;
            this.topicController = controller;
            this.batchSize = batchSize;
            topicMap = new ConcurrentHashMap<>(controller.subscribe(subscriberName, topic, subscriptionType));
            executorService = Executors.newFixedThreadPool(1, r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.start();
                return t;
            });
            isRunning = new AtomicBoolean(true);
        }

        public String getSubscriberName() {
            return subscriberName;
        }

        @Override
        public List<Record> poll() {
            return topicController.request(subscriberName, batchSize);
        }

        @Override
        public void commit(Map<Topic, Integer> read) {
            for(Map.Entry<Topic, Integer> entry:read.entrySet()){
                topicController.commit(subscriberName, entry.getKey(), entry.getValue());
                topicMap.put(entry.getKey(), entry.getValue());
            }
        }

        public void process(){
            while(isRunning.get()) {
                Map<Topic, Integer> read = new HashMap<>(topicMap);
                for (Record r : poll()) {
                    if (!isRunning.get()){
                        commit(read);
                        break;
                    }
                    System.out.println(r);
                    read.compute(r.topic, (k, v) -> {
                        return v + 1;
                    });
                }
                commit(read);
            }
        }
        public void shutdown(){
            isRunning.set(false);
            executorService.shutdownNow();
            try {
                topicController.unsubscribe(subscriberName);
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    static class Record{
        Topic topic;
        Message message;

        public Record(Topic topic, Message message) {
            this.topic = topic;
            this.message = message;
        }
    }

    static class TopicController{
        ConcurrentHashMap<String, List<String>> topicMetaData;
        ConcurrentHashMap<String, Topic> topics;

        public TopicController() {
            this.topicMetaData = new ConcurrentHashMap<>();
            topics = new ConcurrentHashMap<>();
        }

        public Map<Topic, Integer> subscribe(String subscriberName, List<String> topic, SubscriptionType subscriptionType) {
            Map<Topic, Integer> map = new HashMap<>();
            topicMetaData.computeIfPresent(subscriberName, (k,v) -> {
                for(String tp:topic){
                    if (!v.contains(tp)) throw new RuntimeException();
                    Topic t = topics.get(tp);
                    map.put(t, t.subscribe(subscriberName, subscriptionType).lastCommittedOffset);
                }
                return v;
            });
            return map;
        }

        public void unsubscribe(String subscriberName) {
            topicMetaData.computeIfPresent(subscriberName, (k,v) -> {
                for(String tp:v) {
                    topics.computeIfPresent(tp, (key, value) -> {
                        value.subscriberMap.compute(subscriberName, (ke, vl) -> {
                            vl.isSubscribed.set(false);
                            return vl;
                        });
                        return value;
                    });
                }
                return v;
            });
        }

        public void createTopic(String topicName){
            topics.computeIfAbsent(topicName, x -> new Topic(topicName));
        }
        public void deleteTopic(String topicName){
            topics.remove(topicName);
        }

        public List<Record> request(String subscriberName, int batchSize) {
            Random rand = new Random(15);
            List<String> tp = topicMetaData.get(subscriberName);
            List<Record> records = new ArrayList<>();
            if (tp == null) return null;
            int count = 0;
            for(String t:tp){
                int size = Math.min(rand.nextInt(batchSize+1), batchSize-count);
                if (size > 0) {
                    topics.get(t).request(subscriberName, size).forEach(m -> records.add(new Record(topics.get(t), m)));
                }
                count += size;
                if (count == batchSize) break;
            }
            return records;
        }

        public void commit(String subscriberName, Topic topic, Integer offset) {
            topics.computeIfPresent(topic.topicName, (k, v) -> {
                if (!v.commit(subscriberName, offset)) throw new RuntimeException("unable to commit");
                return v;
            });
        }

        public void publishRecord(List<Record> records){
            for(Record r:records){
                topics.compute(r.topic.topicName, (k,v) -> {
                    if (v == null) throw new RuntimeException("topic is absent");
                    v.offer(r.message);
                    return v;
                });
            }
        }
    }
}
