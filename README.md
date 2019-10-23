# rocksdb playground

## running the fat jar
```bash
java -jar build/libs/rocksdb-playground-all.jar
```

## running inside docker
Starting a container for running the test.
```bash
docker run -it --name rocksdb --rm --memory=4g --cpus=4 -v $(pwd):/skyscanner -v /tmp/rocks-db-1:/tmp/rocks-db-1 openjdk:11 bash
```