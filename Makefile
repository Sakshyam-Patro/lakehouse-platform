.PHONY: up down logs clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

clean: down
	docker compose down -v --remove-orphans
	rm -rf data/
