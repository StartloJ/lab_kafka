# Noted
Deploy Kafka
```bash
helm upgrade -n start-dev kafkadev -f kafka.yaml --install --wait --version 14.2.0 bitnami/kafka
```