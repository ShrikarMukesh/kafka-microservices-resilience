# Docker Container Management Guide

## How to Delete Existing Docker Containers

When working with the Kafka microservices resilience project, you may need to delete existing Docker containers. Here are the methods to do so:

### Using Docker Compose (Recommended)

Since this project uses Docker Compose, the easiest way to stop and remove all containers defined in the `docker-compose.yml` file is:

```bash
# Stop and remove containers, networks, images, and volumes
docker-compose down

# If you want to remove volumes as well
docker-compose down -v
```

### Using Docker CLI Commands

If you need more granular control or want to delete specific containers:

1. **List all containers** (running and stopped):
   ```bash
   docker ps -a
   ```

2. **Stop running containers**:
   ```bash
   # Stop a specific container
   docker stop <container_id_or_name>
   
   # Stop all running containers
   docker stop $(docker ps -q)
   ```

3. **Remove stopped containers**:
   ```bash
   # Remove a specific container
   docker rm <container_id_or_name>
   
   # Remove all stopped containers
   docker rm $(docker ps -a -q)
   ```

4. **Force remove running containers** (stop and remove in one command):
   ```bash
   # Force remove a specific container
   docker rm -f <container_id_or_name>
   
   # Force remove all containers (running and stopped)
   docker rm -f $(docker ps -a -q)
   ```

5. **Remove containers for this project specifically**:
   ```bash
   # Remove the containers defined in docker-compose.yml
   docker rm -f zookeeper broker
   ```

### Cleaning Up Other Docker Resources

You might also want to clean up other Docker resources:

1. **Remove unused volumes**:
   ```bash
   docker volume prune
   ```

2. **Remove unused networks**:
   ```bash
   docker network prune
   ```

3. **Remove unused images**:
   ```bash
   docker image prune
   ```

4. **Remove everything unused** (containers, networks, images, and volumes):
   ```bash
   docker system prune -a
   ```

## Starting Fresh

After cleaning up, you can start the services again with:

```bash
docker-compose up -d
```

This will recreate the containers defined in the `docker-compose.yml` file.