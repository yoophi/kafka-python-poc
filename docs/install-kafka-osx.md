osx 에 설치 및 실행 
===

## install 

```bash
brew install zookeeper
brew install kafka
```

## run kafka

```bash
brew services start zookeeper
brew services start kafka
```

## trouble shooting

kafka.common.InconsistentClusterIdException: The Cluster ID doesn't match stored clusterId Some in meta.properties. The broker is trying to join the wrong cluster. Configured zookeeper.connect may be wrong

카프카 설정파일(`/usr/local/etc/kafka/server.properties`)의 `log.dirs` 항목에서 설정되어 있는 디렉토리의
`meta.properties` 파일을 지워주고 카프카를 재시작해주면 해결된다.

