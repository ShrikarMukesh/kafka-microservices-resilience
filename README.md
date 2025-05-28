
# 🔐 Kafka Security – Authorization

## 📌 What is Authorization in Kafka?
Authorization is the process of verifying whether an authenticated user (or service) has the permission to perform a specific action on a Kafka resource.

Kafka handles authorization through **Access Control Lists (ACLs)** that define who can do what on which resource.

---

## 🔄 How Authorization Works in Kafka

1. **User/Client Authenticates** (via SSL or SASL).
2. Kafka **maps the identity to a principal** (e.g., `User:alice`).
3. Kafka **checks ACLs** to verify if the principal is allowed to perform the requested action.
4. If permission is not explicitly granted or is denied, the request fails.

---

## 🧱 Kafka Resource Types

| Resource Type      | Example                      |
|--------------------|------------------------------|
| `Topic`            | `my-topic`                   |
| `Consumer Group`   | `my-consumer-group`          |
| `Cluster`          | Broker-level operations      |
| `TransactionalId`  | For exactly-once semantics   |

---

## ✅ Common Kafka Operations in ACLs

| Operation         | Meaning                                 |
|------------------|------------------------------------------|
| `Read`           | Consume from a topic                     |
| `Write`          | Produce to a topic                       |
| `Create`         | Create a topic                           |
| `Describe`       | Describe a topic or group                |
| `Delete`         | Delete a topic or group                  |
| `Alter`          | Change config of topic or group          |
| `IdempotentWrite`| Write with exactly-once support          |

---

## 🔐 Types of Kafka Principals

Kafka identifies clients using **principals** derived from the authentication mechanism:

- **SASL/PLAIN** → `User:alice`
- **SSL certificates** → `CN=service-name, OU=dept, O=org...`
- **Anonymous** → `User:ANONYMOUS` (if no auth configured)

---

## 🔧 How to Add ACLs in Kafka (CLI Examples)

```bash
# Allow user 'alice' to READ from 'payments-topic'
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:alice \
  --operation Read --topic payments-topic
```

```bash
# Allow user 'bob' to READ from all topics
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:bob \
  --operation Read --topic '*'
```

---

## 📚 Wildcard & Prefix-Based ACLs

You can apply ACLs with:

- **Wildcard `*`** → Applies to all topics or consumer groups
- **Prefix-based matching** (in newer versions with `--resource-pattern-type prefixed`)

```bash
# Allow 'admin' to write to all topics starting with "logs-"
kafka-acls.sh --bootstrap-server localhost:9092 \
  --add --allow-principal User:admin \
  --operation Write --topic logs- \
  --resource-pattern-type prefixed
```

---

## ❗ Default Authorization Behavior

| Situation                          | Behavior           |
|-----------------------------------|--------------------|
| No ACLs configured                | Access allowed     |
| `--authorizer-properties` missing | ACLs not enforced  |
| Conflicting rules                 | **DENY overrides** |
| No matching ACL                   | Access denied      |

---

## 🚨 Important Considerations

- Authorization only works if **authentication is enabled**.
- Disable anonymous access for production systems.
- Prefer Kafka RBAC (Role-Based Access Control) if using Confluent Platform.
- Store and manage ACLs using automation tools (Ansible, Terraform).

---

## ✅ Best Practices

1. Enable TLS or SASL Authentication before setting up ACLs.
2. Use prefix-based ACLs for scalable permissioning.
3. Maintain a central list of principals and roles.
4. Apply least privilege principle.
5. Regularly audit and clean unused ACLs.

---

## 🛡️ Authorization vs. Authentication

| Concept           | Function                           |
|------------------|------------------------------------|
| **Authentication** | Verifies *who* you are (identity)  |
| **Authorization**   | Verifies *what* you can do        |
