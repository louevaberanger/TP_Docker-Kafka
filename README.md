# TP Docker, Kafka et Machine Learning

## Présentation du projet

Ce projet consiste à concevoir une architecture de microservices évolutive en trois étapes : communication synchrone via une API REST, communication asynchrone avec Apache Kafka, puis intégration d'un modèle de Machine Learning..

## Architecture et fonctionnement

Le projet est organisé en trois TP :

* **TP1 — Microservices et API REST :** développement des microservices `Clients` et `Commandes` en Python, conteneurisation avec Docker et communication synchrone via des API REST sur un réseau Docker.
* **TP2 — Communication avec Kafka :** introduction d'Apache Kafka pour découpler les microservices. Lorsqu'une commande est créée, un événement est publié dans le topic `commandes-creees`. Plusieurs consommateurs peuvent exploiter cet événement indépendamment. Puis enrichissement de l'architecture avec un autre microservice consommateur :
* * **Statistiques** : consomme les événements de commandes pour calculer le nombre de commandes par produit et le chiffre d'affaires total par mois.
* **TP3 — Machine Learning :** enrichissement de l'architecture avec un autre microservice consommateur :
  * **Prediction** : utilise un modèle de Machine Learning pour estimer la probabilité qu'un client passe une commande.

### Flux de données

1. Le microservice `Clients` gère les informations des clients.
2. Le microservice `Commandes` crée les commandes et publie des événements dans Kafka.
3. Le topic `commandes-creees` distribue les événements aux consommateurs concernés.
4. Le microservice `Statistiques` exploite ces événements pour calculer des indicateurs commerciaux.
5. Le microservice `Prediction` intègre un modèle de Machine Learning pour estimer la probabilité de commande.

```text
Clients <---- API REST ----> Commandes
                              |
                              | Événement de création
                              v
                         Apache Kafka
                       commandes-creees
                         /           \
                        v             v
                 Statistiques     Prediction
                 Indicateurs      Probabilité
                 commerciaux      de commande
```

Les microservices communiquent via des API REST ou des événements Kafka selon leur rôle. Docker permet de conteneuriser et d'exécuter les composants de l'application.

## Technologies utilisées

* **Python** : développement des microservices et intégration du Machine Learning.
* **API REST** : communication synchrone entre services.
* **Apache Kafka** : publication et consommation d'événements.
* **Docker** : conteneurisation et réseau entre services.
* **Machine Learning** : estimation de la probabilité de commande.

## Commandes Docker utiles

Les commandes suivantes permettent de construire, démarrer et gérer les conteneurs du projet.

```bash
# Construire les images et démarrer les services
docker compose up --build

# Démarrer les services en arrière-plan
docker compose up -d

# Arrêter les services
docker compose down

# Afficher les conteneurs en cours d'exécution
docker ps

# Consulter les logs de tous les services
docker compose logs -f

# Consulter les logs d'un service spécifique
docker compose logs -f prediction

# Reconstruire les images et redémarrer les services
docker compose up --build -d

# Supprimer les conteneurs et les réseaux créés par Compose
docker compose down
```

**Remarque :** ces commandes sont à exécuter depuis le dossier contenant le fichier `docker-compose.yml`. Le nom `prediction` est à remplacer par le nom réel du service dans la configuration Docker Compose.

