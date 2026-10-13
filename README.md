# Projet microservices



**Équipe :** Asma Yazidi et Emma Dos Santos

## Rapports

| TP | Sujet | Rapport |
| --- | --- | --- |
| TP1 | Communication synchrone par API REST | [tp1/TP1.md](tp1/TP1.md) |
| TP2 | Communication asynchrone avec Kafka | [tp2/TP2.md](tp2/TP2.md) |
| TP3 | Microservice de prédiction (Machine Learning) | [tp3/TP3.md](tp3/TP3.md) |

Chaque rapport contient le code, les commandes utilisées, les captures et les réponses aux questions.

## Organisation du projet

```
tp-microservices/
├── README.md
├── tp1/
│   ├── TP1.md              rapport
│   ├── captures_tp1/       captures d'écran
│   ├── clients/            app.py, Dockerfile, requirements.txt
│   └── commandes/          app.py, Dockerfile, requirements.txt
├── tp2/
│   ├── TP2.md
│   ├── docker-compose.yml  lance Kafka et les 3 services
│   ├── captures_tp2/
│   ├── clients/            API + consommateur Kafka
│   ├── commandes/          API + producteur Kafka
│   └── statistiques/       consommateur Kafka (stats produits / mois)
└── tp3/
    ├── TP3.md
    ├── docker-compose.yml
    ├── captures_tp3/
    ├── commandes/          envoie les demandes, reçoit les résultats
    ├── prediction/         app.py, entrainement.py, modele_commande.pkl
    └── generateur/         envoie des demandes en continu
```

## Ports

| Service | Port | TP |
| --- | --- | --- |
| clients | 8080 | TP1, TP2 |
| commandes | 8081 | TP1, TP2, TP3 |
| statistiques | 8082 | TP2 |
| Kafka (depuis la machine) | 9094 | TP2, TP3 |

## Lancer un TP

Prérequis : Docker et Docker Compose. **Un seul TP à la fois** : ils utilisent les mêmes noms de conteneurs et les mêmes ports.

### TP1
```bash
cd tp1
docker build -t microservice-clients ./clients
docker build -t microservice-commandes ./commandes
docker network create tp-net
docker run -d --name clients --network tp-net -p 8080:8080 microservice-clients
docker run -d --name commandes --network tp-net -p 8081:8080 -e CLIENTS_URL=http://clients:8080 microservice-commandes
curl http://localhost:8081/commandes/1
```
Arrêt : `docker rm -f clients commandes`

### TP2
```bash
cd tp2
docker compose up -d --build
curl -X POST http://localhost:8081/commandes -H "Content-Type: application/json" -d '{"client_id": 1, "produit": "PC", "montant": 1000}'
curl http://localhost:8082/stats
```
Arrêt : `docker compose down` (dans `tp2`)

### TP3
```bash
cd tp3
docker compose up -d kafka
# attendre une quinzaine de secondes que Kafka démarre
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic demandes-prediction --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic resultats-predictions --partitions 2 --replication-factor 1 --bootstrap-server localhost:9092
K=3 docker compose up -d --build
```
`K` = nombre d'instances du service prediction. Pour lancer sans le générateur : `docker compose up -d --build commandes prediction`.

Test :
```bash
curl -X POST http://localhost:8081/predictions -H "Content-Type: application/json" -d '{"client_id": 25, "age": 32, "anciennete": 3, "nombre_commandes": 8, "montant_total": 1250, "montant_moyen": 156.25, "jours_depuis_derniere_commande": 12}'
curl http://localhost:8081/predictions
```
Arrêt : `docker compose down` (dans `tp3`)
