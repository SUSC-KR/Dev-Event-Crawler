# Dev-Event-Crawler

본 프로젝트는 Apache Airflow를 활용하여 다가오는 2개월 동안의 개발자 이벤트 정보를 매일 자동으로 크롤링하는 프로젝트입니다. 개발 행사를 한곳에서 쉽게 확인할 수 있도록 하기 위해 시작되었습니다.

## 프로젝트의 목적
현재 개발자 행사를 모아두는 다양한 플랫폼이 존재하지만, 대부분의 정보는 수동 등록이 필요하며 지속적인 관리가 필요합니다. 이 프로젝트는 행사 주최자와 프로젝트 관리자의 이러한 반복적인 작업을 자동화하여 최신 개발자 행사 정보를 손쉽게 제공하는 것을 목표로 합니다.

현재까지 지원하고 있는 행사 사이트는 다음과 같습니다.
- [x] [이벤터스](https://event-us.kr/)
- [ ] [Meetups](https://www.meetup.com/home/)

추가적인 행사 사이트 크롤링을 원할 경우 `include/custom_platform_functions/` 에 원하는 사이트의 크롤링 로직을 구현하여 확장할 수 있습니다.

## 개발 환경
- Python:3.10
- astro-runtime:12.7.1
- airflow: 2.10.5

## 설치 및 실행
1. 프로젝트 클론
2. [Astro CLI 설치](https://www.astronomer.io/docs/astro/cli/install-cli)
3. `astro dev start`
4. `http://localhost:8080` 접속


## 기여 방법
이 프로젝트의 발전에 기여를 해보고 싶으신가요? 기여 가이드를 확인하여 설정, PR 가이드 라인을 포함한 기여 방법에 대한 자세한 내용을 확인하세요.

**문의 및 제안:** 새로운 행사 사이트 추가 요청 또는 버그 제보는 GitHub Issue를 통해 남겨주세요.