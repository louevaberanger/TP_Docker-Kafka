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

## Tester le projet : commandes Docker et API REST

### 1. Construire les images Docker

Les images Docker permettent de créer des conteneurs indépendants pour chaque microservice.

```bash
# Construire l'image du microservice Clients
docker build -t microservice-clients ./clients

# Construire l'image du microservice Commandes
docker build -t microservice-commandes ./commandes
```

### 2. Créer le réseau Docker

Les microservices doivent pouvoir communiquer entre eux. Le réseau Docker `tp-reseau` permet aux conteneurs qui y sont connectés de communiquer en utilisant leurs noms.

```bash
docker network create tp-reseau
```

Si le réseau existe déjà, Docker renverra une erreur indiquant que son nom est utilisé.

### 3. Lancer les microservices

```bash
# Lancer le microservice Clients
docker run -d --name clients --network tp-reseau -p 8080:8080 microservice-clients

# Lancer le microservice Commandes
docker run -d --name commandes --network tp-reseau -p 8081:8080 microservice-commandes
```

L'option `-d` lance les conteneurs en arrière-plan, tandis que `-p` permet d'accéder aux services depuis la machine hôte.

* Microservice Clients : `http://localhost:8080`
* Microservice Commandes : `http://localhost:8081`

### 4. Démarrer et vérifier les conteneurs

Pour démarrer des conteneurs déjà créés mais arrêtés :

```bash
docker start kafka clients commandes prediction
```

Pour vérifier les conteneurs en cours d'exécution :

```bash
docker ps
```

### 5. Tester l'API REST du microservice Commandes

**Consulter toutes les commandes :**

```bash
curl http://localhost:8081/commandes
```

**Consulter une commande par son identifiant :**

```bash
curl http://localhost:8081/commandes/1
```

**Créer une nouvelle commande :**

```bash
curl -X POST http://localhost:8081/commandes \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": 2,
    "produit": "Clavier",
    "montant": 75,
    "date": "2026-10-12"
  }'
```

Cette requête envoie une commande au format JSON au microservice Commandes. Si la requête est valide, le service crée la commande et, selon l'implémentation du projet, publie un événement dans le topic Kafka `commandes-creees`.

### 6. Observer les logs

Pour consulter les logs du microservice Commandes :

```bash
docker logs commandes
```

Pour suivre les logs en temps réel :

```bash
docker logs -f commandes
```

Les logs permettent de vérifier le traitement des requêtes et d'identifier d'éventuelles erreurs.

### 7. Vérifier les événements Kafka

Après avoir créé une commande, il est possible de vérifier que l'événement a été publié dans le topic `commandes-creees`, puis traité par les consommateurs `Statistiques` et `Prediction`.

Pour consulter les logs des consommateurs :

```bash
docker logs -f statistiques
docker logs -f prediction
```

L'apparition du message permet de vérifier la publication de l'événement. Les logs des consommateurs permettent ensuite d'observer son traitement.

### 8. Arrêter les conteneurs

Pour arrêter un conteneur spécifique :

```bash
docker stop commandes
```

Remplacer `commandes` par le nom du conteneur à arrêter.


