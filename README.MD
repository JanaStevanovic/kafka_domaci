# Kafka sistem za obradu događaja za validaciju startap ideja

Ovaj projekat implementira sistem za obradu događaja u okviru platforme za validaciju startup ideja korišćenjem Apache Kafka event-driven arhitekture.

Sistem simulira proces unosa startup ideje, pokretanja validacije i obrade rezultata, pri čemu se događaji šalju kroz Kafka infrastrukturu i perzistiraju u lokalnu bazu podataka.

---

# Tehnologije

- Apache Kafka
- Docker
- Python
- SQLite
- Streamlit
- kafka-python biblioteka

---

# Arhitektura sistema

Sistem se sastoji iz sledećih komponenti:

**Producer aplikacija**  
Generiše i šalje događaje u Kafka topic.

**Consumer aplikacija**  
Čita događaje iz Kafka topic-a i validira njihovu strukturu.

**Producer-Consumer aplikacija**  
Obrađuje događaje, perzistira ih u bazu i šalje neispravne poruke u DLQ topic.

**Database sloj**  
Čuva obrađene događaje i statistiku o startup idejama.

**Dashboard aplikacija**  
Prikazuje statistiku o obrađenim događajima i validacijama startup ideja.

---

# Kafka topic-i

### Glavni topic

```
startup-events
```

Koristi se za obradu svih događaja vezanih za startup ideje.

### DLQ topic

```
startup-events-dlq
```

Koristi se za smeštanje neispravnih poruka koje nisu prošle validaciju.

---

# Tipovi događaja

Sistem koristi sledeće tipove događaja:

- idea_submitted
- idea_validation_requested
- idea_validation_completed
- idea_validation_failed
- feedback_viewed

---

# JSON struktura događaja

Primer događaja:

```json
{
  "event_id": "uuid",
  "event_type": "idea_submitted",
  "timestamp": "2026-03-09T12:00:00Z",
  "business_id": "idea_1234",
  "payload": {
    "startup_name": "AgroVision",
    "problem": "Poljoprivrednici nemaju precizne podatke o usevima",
    "solution": "AI analiza useva pomoću dronova",
    "target_market": "Poljoprivreda",
    "startup_stage": "pre-seed"
  }
}
```

---

# Pokretanje sistema

## 1 Pokretanje Kafka okruženja

```bash
docker compose up -d
```

## 2 Provera ili kreiranje Kafka topic-a

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:9092
```

Ako topic-i ne postoje, kreirati ih:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic startup-events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic startup-events-dlq --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

## 3 Pokretanje baze

```bash
python init_db.py
```

## 4 Pokretanje DB consumer aplikacije

```bash
python db_consumer.py
```

## 5 Pokretanje producer aplikacije

```bash
python producer.py
```

## 6 Pokretanje dashboard aplikacije

```bash
streamlit run dashboard.py
```

Dashboard će biti dostupan na adresi:

```
http://localhost:8501
```

---

# Struktura projekta

```
kafka-startup-validation
│
├── docker-compose.yml
├── README.md
│
├── app
│   ├── producer.py
│   ├── consumer.py
│   ├── db_consumer.py
│   ├── init_db.py
│   ├── dashboard.py
│   ├── startup_events.db
│   ├── requirements.txt
│   └── .env
```

---

# Baza podataka

Sistem koristi SQLite bazu sa dve tabele:

**events**  
Čuva sve obrađene događaje i njihov originalni JSON payload.

**idea_stats**  
Čuva agregirane statistike o događajima po startup ideji.

---

# Idempotentnost

Idempotentnost upisa obezbeđena je korišćenjem jedinstvenog identifikatora događaja `event_id`, koji je definisan kao UNIQUE u bazi podataka.

Na taj način isti događaj ne može biti upisan više puta.

---

# Dashboard

Dashboard implementiran korišćenjem Streamlit biblioteke omogućava pregled:

- ukupnog broja događaja
- statistike po startup ideji
- broja uspešnih validacija
- svih obrađenih događaja

---

