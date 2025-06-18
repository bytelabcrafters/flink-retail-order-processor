# Retail Order Processor (Apache Flink)

## Overview

Processes retail orders using Apache Flink. It:
- Filters events by sourceChannel == "retail"
- Buffers and merges orders for the same customer within 30 seconds
- Combines product quantities for the same productCode
- Outputs a single merged JSON event

## How to Run

```bash
mvn clean package
flink run target/retail-order-processor-1.0.jar
