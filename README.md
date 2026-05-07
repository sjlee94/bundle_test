# Bundle Test — Databricks Asset Bundles

Databricks 워크스페이스 UI 기반으로 DAB(Databricks Asset Bundles)를 사용해 dev/prod 환경을 분리하고, GitHub Actions로 prod 자동 배포 파이프라인 구축
## 📦 프로젝트 구성

```
bundle_test/                       ← repo 루트
├── .github/
│   └── workflows/
│       └── prod-deploy.yml        ← GitHub Actions 자동 배포
└── bundle/                        ← Databricks Asset Bundle
    ├── databricks.yml             ← 번들 메인 설정 (dev/prod 분리)
    └── resources/
        └── my_job.yml             ← Job 정의
```

## 🔄 배포 흐름

```
Dev Workspace (UI 작업)
   ↓ Commit & Push (stage 브랜치)
GitHub
   ↓ stage → main PR 머지
GitHub Actions
   ↓ 자동 배포 + 수동 승인
Prod Workspace
```

| 누가 | 어디서 | 무엇을 |
|------|--------|--------|
| 사람 | Dev Databricks UI | YAML 편집, dev 배포·테스트, Git 커밋 |
| GitHub Actions | GitHub | main 머지 감지 → prod 자동 배포 |

## 🚀 빠른 시작

### Dev 작업
1. Dev Databricks UI에서 stage 브랜치로 이동
2. `databricks.yml` 또는 `resources/*.yml` 편집
3. 🚀 로켓 아이콘 → **Deploy** (dev 환경)
4. ▶️ 버튼으로 Job 실행 테스트
5. Git 패널 → **Commit & Push**

### Prod 배포
1. GitHub에서 stage → main PR 생성
2. 코드 리뷰 후 머지
3. GitHub Actions 자동 트리거
4. 승인 게이트에서 **Approve and deploy** 클릭
5. Prod 워크스페이스 Workflows 탭에서 결과 확인

## 🌍 환경별 설정

| 항목 | dev | prod |
|------|-----|------|
| Mode | `development` | `production` |
| Run as | 사용자 본인 | 서비스 프린시펄 (SP) |
| 스케줄 | 자동 OFF | UNPAUSED |
| 배포 방식 | UI Deploy 버튼 | GitHub Actions 자동 |

## 📚 상세 가이드

처음 셋업하거나 자세한 설명이 필요하면 [DAB UI 가이드](docs/databricks_dab_ui_guide.md)를 참고하세요. 다음 내용을 포함합니다:

- GitHub repo 생성부터 Asset Bundle 생성까지 단계별 설명
- `databricks.yml` 전체 작성 가이드 (주석 포함)
- Job YAML 작성 방법 3가지
- 컴퓨트 옵션 비교 (Serverless / Job 클러스터 / All-purpose)
- Prod 자동 배포 (SP 생성, 권한, Secrets, 워크플로)
- FAQ & 트러블슈팅

## 🔗 참고 링크

- [Databricks Asset Bundles 공식 문서](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- [DAB 워크스페이스 UI 튜토리얼](https://docs.databricks.com/aws/en/dev-tools/bundles/workspace-tutorial)
- [GitHub Actions 통합 가이드](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github)
