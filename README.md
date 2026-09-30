# 临床实验室质量控制与结果拦截

只使用Python标准库和SQLite的模块化服务，默认端口`8339`。覆盖检测项目、质控品批次、质控规则、允许范围、仪器校准、连续偏差、趋势、失控、结果拦截、复测、调查、批次切换和历史更正，以及外部回报对账（偏差超限冻结、双签确认放行、并发冲突裁决）。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：QC规则计算、状态机、校准与放行约束、对账偏差计算。
- `src/repository.py`：SQLite持久化、乐观锁、幂等、审计查询和单事务编排。
- `src/reconciliation.py`：外部回报、对账结论、待核异常的用例编排。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8339
```

## 核心对象

`assay`为检测项目，`qc_lot`为质控品批次，`instrument`为仪器，`qc_run`为质控结果，`result_batch`为患者结果批次。

对账新增对象：

- `external_report`：外部回报，按`assay_id|instrument_id|batch_no`自然键入库。
- `reconciliation`：对账结论（`matched`/`pending_review`/`unmatched`/`superseded`），每条回报至多一条有效结论。
- `review_exception`：偏差超限生成的待核异常（`open`→`confirmed`→`resolved`→`released`）。

患者结果批次新增`frozen`状态；`result_batch`可携带`batch_no`用于与回报配对。

## 对账流程

1. 外部回报通过`POST /api/external_reports/import`入库，按检测项目、仪器、批次号配对本地质控结果与患者批次。
2. 入库与对账（偏差计算、异常生成、批次冻结、审计）在同一事务内完成。偏差超限：创建`review_exception`并把相关`waiting`批次置为`frozen`，冻结批次不能走普通`release`。
3. 冻结批次需两名不同人员确认（`confirm`再`resolve`），再由主管（`supervisor`/`admin`）`release`解冻放行。
4. 并发的确认/放行请求按乐观锁裁决：后到者得到`409`，响应体的`latest`为最新结论、`conflicts`为冲突项（`already_confirmed`、`batch_state_changed`等）。
5. 偏差限值取检测项目的`reconcile_abs_limit`；未配置时默认`2 × 质控品SD`。
6. 同一回报重复导入只处理一次（返回`duplicate: true`与已存结论），处理中途失败整体回滚，可安全重试，不重复计偏差或审计。
7. 回报早于本地批次时为`unmatched`；本地批次补齐后用`reconcile`补对账，旧`unmatched`结论标记`superseded`保留可查。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤（含`external_reports`、`reconciliations`、`review_exceptions`）
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `POST /api/external_reports/import`（可带`Idempotency-Key`）
- `POST /api/external_reports/<id>/reconcile`
- `POST /api/review_exceptions/<id>/confirm|resolve|release`（body含`note`，可带`expected_version`）
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。回报入库允许`reporter`/`operator`/`coordinator`/`supervisor`/`admin`；异常确认允许`operator`/`analyst`/`coordinator`/`supervisor`/`admin`；放行仅`supervisor`/`admin`。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

质控规则与对账阈值为可运行的简化模型，包含1-3s、连续偏移和趋势检查，对账默认使用`2 × SD`绝对偏差，但不替代CLIA、ISO 15189、Westgard完整规则集或实验室信息系统接口。
