# Solo para Linux, Docker y EC2. En Windows usa los .bat de la raiz.
.PHONY: proto run down clean

proto:
	python3 scripts/gen_proto.py

run:
	docker compose up --build

down:
	docker compose down -v

clean:
	rm -rf proto/gen/*.py proto/gen/*.pyi __pycache__ */__pycache__ data/
