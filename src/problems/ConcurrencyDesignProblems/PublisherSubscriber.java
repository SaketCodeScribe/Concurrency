package problems.ConcurrencyDesignProblems;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PublisherSubscriber {
    static class SubscribeOnPauseException extends Exception{
        Record record;

        public SubscribeOnPauseException(String message, Record record) {
            super(message);
            this.record = record;
        }
    }
    static class Message{
        String mssgId;
        String content;
        long timestamp;

        public Message(String mssgId, String content, long timestamp) {
            this.mssgId = mssgId;
            this.content = content;
            this.timestamp = timestamp;
        }

        public String getMssgId() {
            return mssgId;
        }

        public String getContent() {
            return content;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
    enum MessagePriority{
        LOW,
        MEDIUM,
        PRIORITY;
    }

    static class Record{
        Message mssg;
        String topicName;

        public Record(Message mssg, String topicName) {
            this.mssg = mssg;
            this.topicName = topicName;
        }

        public Message getMssg() {
            return mssg;
        }

        public String getTopicName() {
            return topicName;
        }
    }

    enum SubscriberState{
        PAUSED,
        RUNNING;
    }

    static class Topic{
        ConcurrentHashMap<String, SubscriberQueue> buffer;
        List<String> subscribers;
        String topicName;
        long delay;
        int capacity;
        public Topic(String name, long d, int cap) {
            buffer = new ConcurrentHashMap<>();
            this.topicName = name;
            subscribers = new CopyOnWriteArrayList<>();
            this.delay = d;
            this.capacity = cap;
        }

        public String getTopicName() {
            return topicName;
        }

        public void publish(Record record){
            for(String subscriber:subscribers){
                buffer.compute(subscriber, (k, v) ->
                    {
                        if (v == null || v.isExpired(delay)){
                            subscribers.remove(subscriber);
                            buffer.remove(subscriber);
                            return null;
                        }
                        if (v.state.get() == SubscriberState.PAUSED) {
                            raiseException(subscriber, record);
                            return v;
                        }
                        v.onSubscribe(record.getMssg());
                        return v;
                    }
                );
            }
        }

        public void add(String subscriber){
            buffer.computeIfAbsent(subscriber, x -> {
                subscribers.add(x);
                return new SubscriberQueue(capacity);
            });
        }

        public void remove(String subscriber){
            buffer.keySet().removeIf(x -> subscribers.remove(subscriber));
        }

        private void raiseException(String subscriber, Record record){
            new SubscribeOnPauseException(subscriber+" is paused", record);
        }

        public List<Record> request(int n, String subscriberName) {
            List<Record> records = new ArrayList<>();
            buffer.computeIfPresent(subscriberName, (k,v) -> {
                if (v==null) return v;
                try {
                    records.addAll(v.request(n));
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
                return v;
            });
            return records;
        }

        // Each subscriber has it's own queue
        class SubscriberQueue{
            LinkedBlockingQueue<Message> queue;
            // backpressure handling - publisher will not publish the messages to this subscriber
            // if the state is PAUSED and the mssg will be logged exception(having the mssg) will be raised which on
            // retrying will feed it back on to the system (this retrial mechanism can automated - not in
            // current scope).
            AtomicReference<SubscriberState> state;
            AtomicLong lastPaused;
            public SubscriberQueue(int capacity) {
                this.queue = new LinkedBlockingQueue<>(capacity);
                state = new AtomicReference<>(SubscriberState.RUNNING);
                lastPaused = new AtomicLong(-1);
            }

            private void changeState(SubscriberState state){
                this.state.set(state);
            }

            void onSubscribe(Message mssg){
                if (!queue.offer(mssg)){
                    if (state.get() == SubscriberState.RUNNING) {
                        changeState(SubscriberState.PAUSED);
                        lastPaused.set(System.currentTimeMillis());
                    }
                }
                else if (state.get() == SubscriberState.PAUSED){
                    changeState(SubscriberState.RUNNING);
                    lastPaused.set(-1);
                }
            }

            List<Record> request(int n) throws InterruptedException {
                if (n <= 0) return null;
                List<Record> records = new ArrayList<>();
                while(n-- > 0 && !queue.isEmpty()){
                    records.add(new Record(queue.take(), getTopicName()));
                }
                changeState(SubscriberState.RUNNING);
                return records;
            }

            public boolean isExpired(long delay) {
                long l = lastPaused.get();
                return l != -1 ? false : (System.currentTimeMillis() - l) > delay;
            }
        }
        static class TopicConfig{
            private int capacity;
            private long delay;
            private String name;

            private TopicConfig(int capacity, long delay, String name) {
                this.capacity = capacity;
                this.delay = delay;
                this.name = name;
            }
            public static TopicConfigBuilder builder(){
                return new TopicConfigBuilder();
            }

            public int getCapacity() {
                return capacity;
            }

            public long getDelay() {
                return delay;
            }

            public String getName() {
                return name;
            }

            static class TopicConfigBuilder {
                int capacity = 500;
                long delay = 30;
                String name;

                public void withCapacity(int size){
                    this.capacity = Objects.requireNonNull(size);
                }
                public void withDelay(int delay){
                    this.delay = Objects.requireNonNull(delay);
                }
                public void withName(String name){
                    this.name = name;
                }

                public TopicConfig build() {
                    if (this.name == null) throw new RuntimeException("topic cant be null");
                    return new TopicConfig(capacity, delay, name);
                }
            }

        }
    }
    static abstract class Publisher{
        String publisherId;
        public Publisher(String publisherId) {
            this.publisherId = publisherId;
        }
        abstract void publish(Message mssg, List<Topic> topics);
        abstract void add(Topic topic);
        abstract void remove(Topic topic);
    }
    static class P1 extends Publisher{
        Set<Topic> topics;

        public P1(String publisherId) {
            super(publisherId);
            this.topics = ConcurrentHashMap.newKeySet();
        }

        @Override
        public void add(Topic topic){
            topics.add(topic);
        }
        @Override
        void publish(Message mssg, List<Topic> topics) {
            for(Topic topic:topics){
                topic.publish(new Record(mssg, topic.getTopicName()));
            }
        }

        @Override
        void remove(Topic topic) {
            topics.remove(topic);
        }
    }

    static class TopicController {
        ConcurrentHashMap<String, Topic> topics;
        ConcurrentHashMap<String, List<String>> subscribers;

        public TopicController() {
            topics = new ConcurrentHashMap<>();
            subscribers = new ConcurrentHashMap<>();
        }

        public void create(Topic.TopicConfig config) {
            topics.put(config.getName(), new Topic(config.getName(), config.getDelay(), config.getCapacity()));
        }

        public void remove(String topic) {
            topics.remove(topic);
        }

        public void subscribe(String subscriberName, String topic) {
            topics.computeIfPresent(topic, (k, v) -> {
                subscribers.computeIfAbsent(subscriberName, x -> new CopyOnWriteArrayList<>()).add(topic);
                v.add(subscriberName);
                return v;
            });
        }

        public void unsubscribe(String subscriberName, String topic) {
            topics.computeIfPresent(topic, (k, v) -> {
                subscribers.remove(subscriberName);
                v.remove(subscriberName);
                return v;
            });
        }

        public void addTopicToPub(Publisher publisher, String topic) {
            topics.computeIfPresent(topic, (k, v) -> {
                publisher.add(v);
                return v;
            });
        }

        public void removeTopicFromPublisher(Publisher publisher, String topic) {
            topics.computeIfPresent(topic, (k, v) -> {
                publisher.remove(v);
                return v;
            });
        }

        public List<Record> request(int n, String subscriberName) {
            List<Record> records = new ArrayList<>();
            subscribers.computeIfPresent(subscriberName, (k,v) -> {
                for (String tp : v) {
                    this.topics.computeIfPresent(tp, (key, value) -> {
                        records.addAll(value.request(n, subscriberName));
                        return value;
                    });
                }
                return v;
            });

            return records;
        }
    }

    static abstract class Subscriber{
        private String subscriberId;

        public Subscriber(String subscriberId) {
            this.subscriberId = subscriberId;
        }

        abstract void request(int n);
        abstract void subscribe(String topic);
        abstract void unsubscribe(String topic);
        public String getSubscriberId() {
            return subscriberId;
        }
    }

    static class S1 extends Subscriber{
        TopicController controller;
        List<String> topics;
        public S1(String subscriberId, TopicController controller, List<String> topics) {
            super(subscriberId);
            this.topics = topics;
            this.controller = controller;
        }

        @Override
        void request(int n) {
            controller.request(n, getSubscriberId()).forEach(record -> process(record));
        }

        @Override
        void subscribe(String topic) {
            controller.subscribe(getSubscriberId(), topic);
        }

        @Override
        void unsubscribe(String topic) {
            controller.unsubscribe(getSubscriberId(), topic);
        }


        private void process(Record record) {
            System.out.println(record);
        }
    }
}
