package com.example.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class RetailOrderProcessor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inputStream = env.fromElements(
                "{\"sourceChannel\": \"retail\", \"customerId\": \"123\", \"orderId\": \"A1\", \"orders\": [{\"productCode\": \"p1\", \"productDescription\": \"apple\", \"productQuantity\": \"2\"}]}",
                "{\"sourceChannel\": \"retail\", \"customerId\": \"123\", \"orderId\": \"A2\", \"orders\": [{\"productCode\": \"p1\", \"productDescription\": \"apple\", \"productQuantity\": \"3\"}, {\"productCode\": \"p2\", \"productDescription\": \"banana\", \"productQuantity\": \"1\"}]}"
        );

        DataStream<String> filtered = inputStream
                .filter(json -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(json);
                    return "retail".equalsIgnoreCase(node.get("sourceChannel").asText());
                });

        filtered
            .keyBy(json -> {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(json);
                return node.get("customerId").asText();
            })
            .process(new OrderAggregator())
            .print();

        env.execute("Retail Order Aggregator");
    }

    public static class OrderAggregator extends KeyedProcessFunction<String, String, String> {

        private transient MapState<String, JsonNode> productMap;
        private transient ValueState<String> latestOrderId;
        private transient ValueState<Long> timerState;

        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, JsonNode> mapStateDescriptor =
                    new MapStateDescriptor<>("products", String.class, JsonNode.class);
            productMap = getRuntimeContext().getMapState(mapStateDescriptor);

            latestOrderId = getRuntimeContext().getState(new ValueStateDescriptor<>("orderId", String.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timer", Long.class));
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            JsonNode order = mapper.readTree(value);
            String orderId = order.get("orderId").asText();
            JsonNode ordersArray = order.get("orders");

            for (JsonNode item : ordersArray) {
                String code = item.get("productCode").asText();
                String desc = item.get("productDescription").asText();
                int qty = Integer.parseInt(item.get("productQuantity").asText());

                JsonNode existing = productMap.get(code);
                int totalQty = qty;
                if (existing != null) {
                    totalQty += Integer.parseInt(existing.get("productQuantity").asText());
                }

                Map<String, Object> product = new HashMap<>();
                product.put("productCode", code);
                product.put("productDescription", desc);
                product.put("productQuantity", String.valueOf(totalQty));

                productMap.put(code, mapper.valueToTree(product));
            }

            latestOrderId.update(orderId);

            if (timerState.value() == null) {
                long triggerTime = ctx.timerService().currentProcessingTime() + 30_000L;
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
                timerState.update(triggerTime);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<JsonNode> products = new ArrayList<>();
            for (JsonNode node : productMap.values()) {
                products.add(node);
            }

            Map<String, Object> result = new HashMap<>();
            result.put("sourceChannel", "retail");
            result.put("customerId", ctx.getCurrentKey());
            result.put("orderId", latestOrderId.value());
            result.put("orders", products);

            out.collect(mapper.writeValueAsString(result));

            productMap.clear();
            latestOrderId.clear();
            timerState.clear();
        }
    }
}
