# Projet microservices

Série de TP sur les microservices en Python (Flask), avec Docker et Kafka.

**Équipe :** Asma Yazidi et Emma Dos Santos

## Rapports

| TP | Sujet | Rapport |
| --- | --- | --- |
| TP1 | Communication entre microservices par API REST | [tp1/TP1.md](tp1/TP1.md) |
| TP2 | Communication asynchrone avec Kafka | [tp2/TP2.md](tp2/TP2.md) |
| TP3 | Microservice de prédiction (Machine Learning) | [tp3/TP3.md](tp3/TP3.md) |

Chaque rapport contient le code, les commandes utilisées, les captures et les réponses aux questions.

## Organisation

```
tp-microservices/
├── tp1/   clients + commandes (REST)
├── tp2/   kafka + commandes + clients + statistiques
└── tp3/   kafka + commandes + prediction + generateur
```

## Lancer un TP

Il faut Docker avec Docker Compose. Un seul TP à la fois, car ils utilisent les mêmes noms de conteneurs et les mêmes ports.

- **TP1** : voir les commandes `docker build` / `docker run` dans [tp1/TP1.md](tp1/TP1.md).
- **TP2** : `cd tp2 && docker compose up -d --build`
- **TP3** : `cd tp3 && K=3 docker compose up -d --build` (K = nombre d'instances de prediction). Les topics sont à créer avant, voir [tp3/TP3.md](tp3/TP3.md).

Pour tout arrêter : `docker compose down` dans le dossier racine.
