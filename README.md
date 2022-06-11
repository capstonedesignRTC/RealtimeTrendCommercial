# RealtimeTrendCommercial
Real-time Trend Commercial Analysis - 2022년 1학기 캡스톤디자인1 프로젝트

### Introduction
As a service for **prospective entrepreneurs**, it is a service that informs which business is suitable for a certain location before starting a business. It collects and refines a large amount of information required from various open API sites, and the information comes out when the user selects a location.

### 구성원

이름 | 학과 |  학번  | 이메일
------------ | -------------  | ------------- | -------------  
이창렬 | 컴퓨터공학과 | 2019110634 | lclgood@khu.ac.kr
허인경 | 컴퓨터공학과 | 2019102241 |  red131729@khu.ac.kr


### 프로젝트 소개

카프카를 이용해 분산되어 있는 데이터를 한 곳으로 모은 후, 스파크를 이용해 빅데이터를 연산하고 이를 시각화할 수 있는 툴로 접근할 수 있는 데이터 레이크를 구축한다.

1. 스케쥴러가 Kafkka를 실행시켜 주기적으로 데이터를 수집한다. 이 데이터들은  데이터가 사용 준비 상태가 될 때까지 원시 상태로 보관된다.
2. 데이터에 접근하여 필요한 데이터를 정제한 후, 분석 레포트를 생성한다.
3. 분석 레포트는 매일 갱신된다.
4. 사용자는 시각화 툴을 통해 빅데이터를 통해 얻고 싶은 정보를 획득한다.

### Development Environment
- Apache Kafka
- Apache Spark
- Django
- AWS S3