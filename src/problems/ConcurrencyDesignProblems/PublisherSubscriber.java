package problems.ConcurrencyDesignProblems;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
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

    static class Subscriber{
        String subscriberName;

        public Subscriber(String subscriberName) {
            this.subscriberName = subscriberName;
        }

        public String getSubscriberName() {
            return subscriberName;
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

        public boolean subscribe(String subscriberName, SubscriptionType subscriptionType){
            subscriberMap.computeIfAbsent(subscriberName, key -> {
                    var meta = new SubscriberMetaData();
                    if (subscriptionType == SubscriptionType.LATEST) {
                        meta.setLastCommittedOffset(queue.size());
                    }
                    meta.setSubscribed(true);
                    return meta;
                }
            );
            return true;
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
            while(true) {
                boolean old = metaData.isSubscribed.get();
                int begin = metaData.lastCommittedOffset + 1;
                int end = begin + Math.min(metaData.getLastCommittedOffset() + requests, queue.size() - 1);
                List<Message> messages = new ArrayList<>();
                for (int i = begin; i <= end; i++) {
                    messages.add(queue.get(i));
                }
                if (!old && metaData.isSubscribed.compareAndSet(old, true)) {
                    return messages;
                }
                else return null;
            }
        }

        public void commit(String subscriberName, int offset){
            SubscriberMetaData metaData = subscriberMap.get(subscriberName);
            while(true){
                boolean old = metaData.isSubscribed.get();

                if (!old && metaData.isSubscribed.compareAndSet(old, true)){
                    metaData.setLastCommittedOffset(offset);
                    return;
                }
                else return;
            }
        }
    }
}
